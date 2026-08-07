use redis::{ErrorKind, RetryMethod, ServerError, ServerErrorKind};
use ventstream_core::SinkError;

use super::config::{RedisConfig, RedisTopology};
use super::topology::topology_label;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum RedisFailure {
    Transient { reason: String, reconnect: bool },
    Blocked(String),
}

pub(super) fn classify_error(error: &redis::RedisError, topology: &RedisTopology) -> RedisFailure {
    let reason = error.to_string();
    let cluster = matches!(topology, RedisTopology::Cluster { .. });
    let sentinel = matches!(topology, RedisTopology::Sentinel(_));
    if reason.contains("VENTSTREAM_FENCED") {
        return RedisFailure::Transient {
            reason: "Redis writer was fenced by a newer VentStream process".to_owned(),
            reconnect: false,
        };
    }
    if reason.contains("VENTSTREAM_WRITER_OWNED") {
        return RedisFailure::Transient {
            reason: "Redis target is owned by another active VentStream writer".to_owned(),
            reconnect: false,
        };
    }
    if let Some(errors) = error.clone().into_server_errors() {
        let mut saw_transient = false;
        let mut reconnect = false;
        let mut saw_blocked = false;
        for (_, server_error) in errors.iter() {
            match classify_server_error(server_error, topology) {
                RedisFailure::Transient {
                    reconnect: required,
                    ..
                } => {
                    saw_transient = true;
                    reconnect |= required;
                }
                RedisFailure::Blocked(_) => saw_blocked = true,
            }
        }
        if saw_transient {
            return RedisFailure::Transient { reason, reconnect };
        }
        if saw_blocked {
            return RedisFailure::Blocked(reason);
        }
    }

    match error.kind() {
        ErrorKind::AuthenticationFailed
        | ErrorKind::InvalidClientConfig
        | ErrorKind::Client
        | ErrorKind::UnexpectedReturnType
        | ErrorKind::EmptySentinelList
        | ErrorKind::RESP3NotSupported => RedisFailure::Blocked(reason),
        ErrorKind::ClusterConnectionNotFound if cluster => RedisFailure::Transient {
            reason,
            reconnect: true,
        },
        ErrorKind::MasterNameNotFoundBySentinel | ErrorKind::NoValidReplicasFoundBySentinel
            if sentinel =>
        {
            RedisFailure::Transient {
                reason,
                reconnect: true,
            }
        }
        ErrorKind::Server(ServerErrorKind::MasterDown) if sentinel => RedisFailure::Transient {
            reason,
            reconnect: true,
        },
        ErrorKind::ClusterConnectionNotFound => RedisFailure::Blocked(format!(
            "{reason}; configure Redis Cluster topology to use cluster discovery"
        )),
        ErrorKind::MasterNameNotFoundBySentinel | ErrorKind::NoValidReplicasFoundBySentinel => {
            RedisFailure::Blocked(format!(
                "{reason}; configure Redis Sentinel topology to use Sentinel discovery"
            ))
        }
        ErrorKind::Io | ErrorKind::Parse => match error.retry_method() {
            RetryMethod::Reconnect => RedisFailure::Transient {
                reason,
                reconnect: true,
            },
            RetryMethod::RetryImmediately | RetryMethod::WaitAndRetry => RedisFailure::Transient {
                reason,
                reconnect: false,
            },
            _ => RedisFailure::Blocked(reason),
        },
        _ => match error.retry_method() {
            RetryMethod::RetryImmediately | RetryMethod::WaitAndRetry => RedisFailure::Transient {
                reason,
                reconnect: false,
            },
            RetryMethod::Reconnect => RedisFailure::Transient {
                reason,
                reconnect: true,
            },
            RetryMethod::RefreshSlotsAndRetry => RedisFailure::Transient {
                reason,
                reconnect: true,
            },
            RetryMethod::AskRedirect | RetryMethod::MovedRedirect if cluster => {
                RedisFailure::Transient {
                    reason,
                    reconnect: false,
                }
            }
            RetryMethod::ReconnectFromInitialConnections if cluster => RedisFailure::Transient {
                reason,
                reconnect: true,
            },
            RetryMethod::AskRedirect
            | RetryMethod::MovedRedirect
            | RetryMethod::ReconnectFromInitialConnections => RedisFailure::Blocked(format!(
                "{reason}; configure Redis Cluster topology to follow Cluster redirects"
            )),
            RetryMethod::NoRetry => RedisFailure::Blocked(reason),
            _ => RedisFailure::Blocked(reason),
        },
    }
}

pub(super) fn is_capacity_pressure(error: &redis::RedisError) -> bool {
    if error
        .to_string()
        .contains("Redis replica acknowledgement target was not met")
        || error
            .to_string()
            .contains("Redis AOF acknowledgement target was not met")
    {
        return true;
    }
    error.clone().into_server_errors().is_some_and(|errors| {
        errors
            .iter()
            .any(|(_, server_error)| is_capacity_server_error(server_error))
    })
}

pub(super) fn is_authentication_failure(error: &redis::RedisError) -> bool {
    error.kind() == ErrorKind::AuthenticationFailed
}

pub(super) fn record_topology_event(error: &redis::RedisError, topology: &RedisTopology) {
    let cause = if let Some(errors) = error.clone().into_server_errors() {
        errors.iter().find_map(|(_, error)| match error.kind() {
            Some(ServerErrorKind::Moved) => Some("moved"),
            Some(ServerErrorKind::Ask) => Some("ask"),
            Some(ServerErrorKind::ReadOnly) => Some("readonly"),
            Some(ServerErrorKind::MasterDown) => Some("master_down"),
            Some(ServerErrorKind::ClusterDown) => Some("cluster_down"),
            _ => None,
        })
    } else {
        None
    }
    .or_else(|| match error.kind() {
        ErrorKind::MasterNameNotFoundBySentinel => Some("master_discovery"),
        ErrorKind::NoValidReplicasFoundBySentinel => Some("replica_discovery"),
        ErrorKind::ClusterConnectionNotFound => Some("slot_discovery"),
        _ => match error.retry_method() {
            RetryMethod::RefreshSlotsAndRetry => Some("slot_refresh"),
            RetryMethod::AskRedirect => Some("ask"),
            RetryMethod::MovedRedirect => Some("moved"),
            RetryMethod::ReconnectFromInitialConnections => Some("seed_reconnect"),
            _ => None,
        },
    });
    if let Some(cause) = cause {
        ventstream_telemetry::bump_redis_topology_event(topology_label(topology), cause);
    }
}

fn is_capacity_server_error(error: &ServerError) -> bool {
    matches!(
        error.kind(),
        Some(
            ServerErrorKind::BusyLoading | ServerErrorKind::TryAgain | ServerErrorKind::ClusterDown
        )
    ) || matches!(
        error.code().to_ascii_uppercase().as_str(),
        "OOM" | "BUSY" | "LOADING" | "TRYAGAIN" | "CLUSTERDOWN" | "NOREPLICAS"
    )
}

fn classify_server_error(error: &ServerError, topology: &RedisTopology) -> RedisFailure {
    let reason = error.to_string();
    let cluster = matches!(topology, RedisTopology::Cluster { .. });
    let sentinel = matches!(topology, RedisTopology::Sentinel(_));
    if error.code().eq_ignore_ascii_case("VENTSTREAM_FENCED")
        || error
            .details()
            .is_some_and(|details| details.contains("VENTSTREAM_FENCED"))
    {
        return RedisFailure::Transient {
            reason: "Redis writer was fenced by a newer VentStream process".to_owned(),
            reconnect: false,
        };
    }
    if error.code().eq_ignore_ascii_case("VENTSTREAM_WRITER_OWNED")
        || error
            .details()
            .is_some_and(|details| details.contains("VENTSTREAM_WRITER_OWNED"))
    {
        return RedisFailure::Transient {
            reason: "Redis target is owned by another active VentStream writer".to_owned(),
            reconnect: false,
        };
    }
    match error.kind() {
        Some(ServerErrorKind::MasterDown) => RedisFailure::Transient {
            reason,
            reconnect: sentinel,
        },
        Some(
            ServerErrorKind::BusyLoading | ServerErrorKind::TryAgain | ServerErrorKind::ClusterDown,
        ) => RedisFailure::Transient {
            reason,
            reconnect: false,
        },
        Some(ServerErrorKind::ReadOnly) => RedisFailure::Transient {
            reason,
            reconnect: true,
        },
        Some(ServerErrorKind::Moved | ServerErrorKind::Ask) if cluster => RedisFailure::Transient {
            reason,
            reconnect: false,
        },
        Some(ServerErrorKind::Moved | ServerErrorKind::Ask) => RedisFailure::Blocked(format!(
            "{reason}; configure Redis Cluster topology to follow Cluster redirects"
        )),
        Some(
            ServerErrorKind::ResponseError
            | ServerErrorKind::ExecAbort
            | ServerErrorKind::NoScript
            | ServerErrorKind::CrossSlot
            | ServerErrorKind::NotBusy
            | ServerErrorKind::NoSub
            | ServerErrorKind::NoPerm,
        ) => RedisFailure::Blocked(reason),
        None if matches!(
            error.code().to_ascii_uppercase().as_str(),
            "OOM" | "MISCONF" | "BUSY" | "NOREPLICAS"
        ) =>
        {
            RedisFailure::Transient {
                reason,
                reconnect: false,
            }
        }
        None => RedisFailure::Blocked(reason),
        Some(_) => RedisFailure::Blocked(reason),
    }
}

pub(super) fn map_connect_error(error: &redis::RedisError, config: &RedisConfig) -> SinkError {
    if config.has_rotating_credentials() && is_authentication_failure(error) {
        return SinkError::Connection(
            "Redis authentication failed while using reloadable credentials".to_owned(),
        );
    }
    match classify_error(error, &config.topology) {
        RedisFailure::Transient { reason, .. } => SinkError::Connection(reason),
        RedisFailure::Blocked(reason) => SinkError::Blocked(reason),
    }
}
