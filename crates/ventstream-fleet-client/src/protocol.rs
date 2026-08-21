use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use uuid::Uuid;
use ventstream_fleet_protocol::v1::agent_to_server::Payload as AgentPayload;
use ventstream_fleet_protocol::v1::operation::Parameters as OperationParameters;
use ventstream_fleet_protocol::v1::server_to_agent::Payload;
use ventstream_fleet_protocol::v1::{
    AgentHello, AgentStatus, AgentToServer, DesiredRunState as ProtoDesiredRunState, DesiredState,
    ObservedRunState as ProtoObservedRunState, Operation, OperationKind as ProtoOperationKind,
    OperationState as ProtoOperationState, OperationUpdate, ServerHello, ServerToAgent,
};

use crate::{
    AgentError, AgentOperationKind, AgentScope, ConfigurationBundle, ConvergenceOperation,
    DesiredRunState, DesiredStateSnapshot, ManagementState, ObservedRunState, OperationReceipt,
    ReceiptState,
};

const PROTOCOL_MAJOR: u32 = 1;
const PROTOCOL_MINOR: u32 = 0;
const MAX_CLOCK_SKEW: ChronoDuration = ChronoDuration::minutes(5);
const MAX_CONFIGURATION_DOCUMENT_BYTES: usize = 64 * 1024;

/// Stable process identity and capabilities sent on every new control stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentDescriptor {
    instance_id: Uuid,
    boot_id: Uuid,
    engine_version: String,
    build_digest: String,
    roles: Vec<String>,
    capabilities: Vec<String>,
}

/// Ephemeral engine telemetry sampled locally by the supervisor.
///
/// This data is never persisted in management state. A fresh sample is attached
/// to each authenticated heartbeat so a process restart cannot replay stale
/// delivery counters or readiness.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AgentRuntimeStatus {
    /// Configured CDC source kind, when known.
    pub source_kind: String,
    /// Configured sink kind, when known.
    pub sink_kind: String,
    /// Age of the latest source cursor.
    pub cursor_age_milliseconds: Option<u64>,
    /// Events emitted by the engine per second over the latest sample window.
    pub events_per_second: Option<f64>,
    /// Events accepted for sink or realtime delivery since process start.
    pub events_received_total: Option<u64>,
    /// Events acknowledged by the sink or delivered to realtime clients.
    pub events_delivered_total: Option<u64>,
    /// Successfully delivered events per second over the latest sample window.
    pub events_delivered_per_second: Option<f64>,
    /// Events that could not be delivered to the configured sink.
    pub events_failed_total: Option<u64>,
    /// Event-level sink retry attempts.
    pub sink_retries_total: Option<u64>,
    /// Successfully persisted dead-letter records.
    pub dlq_writes_total: Option<u64>,
    /// Dead-letter persistence failures.
    pub dlq_write_failures_total: Option<u64>,
    /// Engine backpressure events.
    pub backpressure_total: Option<u64>,
    /// Latest sink bulk-write p95 latency.
    pub sink_latency_p95_milliseconds: Option<u64>,
    /// Current bounded engine queue depth and configured capacity.
    pub queue_depth: Option<u64>,
    /// Configured engine queue capacity.
    pub queue_capacity: Option<u64>,
    /// Engine process CPU as percent of one core over the latest sample
    /// window (may exceed 100 on multicore). Computed engine-side.
    pub cpu_percent: Option<f64>,
    /// Engine process resident set size in bytes.
    pub rss_bytes: Option<u64>,
    /// Container memory limit in bytes, when one is imposed.
    pub memory_limit_bytes: Option<u64>,
    /// Container CPU quota in millicores, when one is imposed.
    pub cpu_limit_millicores: Option<u64>,
    /// Realtime connection and subscription gauges.
    pub active_connections: Option<u64>,
    /// Current realtime subscriptions.
    pub active_subscriptions: Option<u64>,
    /// Broker messages evaluated across realtime subscription sessions.
    pub realtime_broker_observations_total: Option<u64>,
    /// Broker subscription evaluations per second over the latest sample window.
    pub realtime_broker_observations_per_second: Option<f64>,
    /// Realtime loss and terminal failure counters.
    pub realtime_dropped_total: Option<u64>,
    /// Realtime terminal failures since process start.
    pub realtime_terminal_failures_total: Option<u64>,
    /// Latest observed input and successful output times as Unix milliseconds.
    pub last_input_at_unix_milliseconds: Option<u64>,
    /// Latest successful output time as Unix milliseconds.
    pub last_output_at_unix_milliseconds: Option<u64>,
    /// Source schema drift events observed since process start.
    pub schema_drift_total: Option<u64>,
    /// Schema-classified DLQ routings since process start.
    pub schema_casualties_total: Option<u64>,
    /// TRUNCATE statements observed in the source change stream.
    pub truncate_events_total: Option<u64>,
    /// Bounded operator-safe summary of observed drift per table
    /// (e.g. `orders added=1 type_change=1`). Empty when no drift.
    pub schema_drift_detail: String,
    /// Latest engine readiness result. `None` means no sample is available.
    pub engine_ready: Option<bool>,
    /// Stable machine-readable runtime failure code.
    pub error_code: String,
    /// Bounded operator-safe runtime failure message.
    pub safe_error_message: String,
}

impl AgentDescriptor {
    /// Validates bounded identity metadata for one running engine process.
    pub fn new(
        instance_id: Uuid,
        boot_id: Uuid,
        engine_version: impl Into<String>,
        build_digest: impl Into<String>,
        roles: Vec<String>,
        capabilities: Vec<String>,
    ) -> Result<Self, AgentError> {
        let engine_version = engine_version.into();
        let build_digest = build_digest.into();
        if instance_id.is_nil()
            || boot_id.is_nil()
            || !valid_metadata(&engine_version, 120)
            || !valid_metadata(&build_digest, 200)
            || !valid_identifiers(&roles, 32)
            || !valid_identifiers(&capabilities, 64)
        {
            return Err(protocol_error("agent descriptor is invalid"));
        }
        Ok(Self {
            instance_id,
            boot_id,
            engine_version,
            build_digest,
            roles,
            capabilities,
        })
    }
}

/// Validated settings received during the server handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerHelloData {
    /// Random connection fence assigned by the gateway.
    pub connection_id: Uuid,
    /// Status heartbeat cadence required by the gateway.
    pub heartbeat_interval: Duration,
    /// Current deployment generation used to fence replaced identities.
    pub deployment_generation: u64,
    /// Complete desired state included in the handshake, when one exists.
    pub desired_state: Option<DesiredStateSnapshot>,
    /// Capabilities the server requires from this agent.
    pub required_capabilities: Vec<String>,
    /// Current certificate rotation deadline.
    pub certificate_rotation_deadline: DateTime<Utc>,
}

/// One validated server instruction ready for local processing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerInstruction {
    /// Initial handshake response.
    Hello(ServerHelloData),
    /// Complete desired-state snapshot.
    DesiredState(DesiredStateSnapshot),
    /// Run-state convergence operation.
    Converge(ConvergenceOperation),
}

/// Exact-sequence, connection-fenced validator for one V1 control stream.
#[derive(Debug)]
pub struct ControlSession {
    next_server_sequence: u64,
    next_agent_sequence: u64,
    connection_id: Option<Uuid>,
    descriptor: AgentDescriptor,
}

impl ControlSession {
    /// Creates a session expecting a server hello at sequence one.
    pub fn new(descriptor: AgentDescriptor) -> Self {
        Self {
            next_server_sequence: 1,
            next_agent_sequence: 1,
            connection_id: None,
            descriptor,
        }
    }

    /// Returns the established connection fence after a valid handshake.
    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    /// Validates and converts the next server envelope.
    pub fn accept(
        &mut self,
        message: &ServerToAgent,
        scope: &AgentScope,
        current_desired: Option<&DesiredStateSnapshot>,
        now: DateTime<Utc>,
    ) -> Result<ServerInstruction, AgentError> {
        let envelope_connection =
            validate_envelope(message, self.next_server_sequence, self.connection_id, now)?;

        let instruction = match message.payload.as_ref() {
            Some(Payload::Hello(hello)) if self.connection_id.is_none() => {
                ServerInstruction::Hello(parse_server_hello(
                    hello,
                    envelope_connection,
                    scope,
                    &self.descriptor.capabilities,
                    now,
                )?)
            }
            Some(Payload::DesiredState(desired)) if self.connection_id.is_some() => {
                ServerInstruction::DesiredState(parse_desired_state(desired, scope, now)?)
            }
            Some(Payload::Operation(operation)) if self.connection_id.is_some() => {
                let desired = current_desired
                    .ok_or_else(|| protocol_error("operation arrived before desired state"))?;
                ServerInstruction::Converge(parse_agent_operation(
                    operation,
                    &message.correlation_id,
                    scope,
                    desired,
                    now,
                )?)
            }
            Some(Payload::CancelOperation(_)) => {
                return Err(protocol_error("operation cancellation is not enabled"));
            }
            Some(Payload::Hello(_)) => {
                return Err(protocol_error("server hello may only appear once"));
            }
            Some(Payload::DesiredState(_)) | Some(Payload::Operation(_)) => {
                return Err(protocol_error("server instruction arrived before hello"));
            }
            None => return Err(protocol_error("server payload is required")),
        };

        if self.connection_id.is_none() {
            self.connection_id = Some(envelope_connection);
        }
        self.next_server_sequence = self
            .next_server_sequence
            .checked_add(1)
            .ok_or_else(|| protocol_error("server sequence exhausted"))?;
        Ok(instruction)
    }

    /// Builds the first outbound hello from durable local cursors.
    pub fn agent_hello(
        &mut self,
        state: &ManagementState,
        now: DateTime<Utc>,
    ) -> Result<AgentToServer, AgentError> {
        if self.connection_id.is_some() || self.next_agent_sequence != 1 {
            return Err(protocol_error("agent hello may only be sent once"));
        }
        let sequence = self.take_agent_sequence()?;
        Ok(AgentToServer {
            sequence,
            sent_at: Some(timestamp(now)),
            correlation_id: Uuid::now_v7().to_string(),
            connection_id: String::new(),
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            payload: Some(AgentPayload::Hello(AgentHello {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
                minimum_protocol_minor: PROTOCOL_MINOR,
                engine_version: self.descriptor.engine_version.clone(),
                build_digest: self.descriptor.build_digest.clone(),
                instance_id: self.descriptor.instance_id.to_string(),
                boot_id: self.descriptor.boot_id.to_string(),
                roles: self.descriptor.roles.clone(),
                capabilities: self.descriptor.capabilities.clone(),
                last_desired_state_revision: state.desired().map_or(0, |desired| desired.revision),
                last_configuration_revision: state.applied_configuration_revision().unwrap_or(0),
                last_operation_sequence: state.last_completed_operation_sequence(),
                observed_run_state: proto_observed_state(state.observed()).into(),
                local_state_healthy: true,
            })),
        })
    }

    /// Builds a bounded current-state heartbeat after the handshake.
    pub fn agent_status(
        &mut self,
        state: &ManagementState,
        active_operation_id: Option<Uuid>,
        now: DateTime<Utc>,
    ) -> Result<AgentToServer, AgentError> {
        self.agent_status_with_runtime(
            state,
            active_operation_id,
            &AgentRuntimeStatus::default(),
            now,
        )
    }

    /// Builds a heartbeat enriched with a fresh local engine sample.
    pub fn agent_status_with_runtime(
        &mut self,
        state: &ManagementState,
        active_operation_id: Option<Uuid>,
        runtime: &AgentRuntimeStatus,
        now: DateTime<Utc>,
    ) -> Result<AgentToServer, AgentError> {
        let connection_id = self.established_connection_id()?;
        let sequence = self.take_agent_sequence()?;
        let report_runtime = state.observed() == ObservedRunState::Running;
        let empty_runtime = AgentRuntimeStatus::default();
        let runtime = if report_runtime {
            runtime
        } else {
            &empty_runtime
        };
        if !valid_optional_metadata(&runtime.source_kind, 64)
            || !valid_optional_metadata(&runtime.sink_kind, 64)
            || runtime.events_per_second.is_some_and(|value| {
                !value.is_finite() || !(0.0..=1_000_000_000_000.0).contains(&value)
            })
            || runtime.events_delivered_per_second.is_some_and(|value| {
                !value.is_finite() || !(0.0..=1_000_000_000_000.0).contains(&value)
            })
            || runtime
                .realtime_broker_observations_per_second
                .is_some_and(|value| {
                    !value.is_finite() || !(0.0..=1_000_000_000_000.0).contains(&value)
                })
            || !valid_optional_identifier(&runtime.error_code)
            || !valid_optional_metadata(&runtime.safe_error_message, 1000)
        {
            return Err(protocol_error("agent runtime status is invalid"));
        }
        let observed = if report_runtime && runtime.engine_ready == Some(false) {
            ObservedRunState::Error
        } else {
            state.observed()
        };
        Ok(AgentToServer {
            sequence,
            sent_at: Some(timestamp(now)),
            correlation_id: Uuid::now_v7().to_string(),
            connection_id: connection_id.to_string(),
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            payload: Some(AgentPayload::Status(AgentStatus {
                instance_id: self.descriptor.instance_id.to_string(),
                boot_id: self.descriptor.boot_id.to_string(),
                run_state: proto_observed_state(observed).into(),
                desired_state_revision: state.desired().map_or(0, |desired| desired.revision),
                configuration_revision: state.applied_configuration_revision().unwrap_or(0),
                source_kind: runtime.source_kind.clone(),
                sink_kind: runtime.sink_kind.clone(),
                cursor_age_milliseconds: runtime.cursor_age_milliseconds,
                events_per_second: runtime.events_per_second,
                dlq_writes_total: runtime.dlq_writes_total,
                schema_drift_total: runtime.schema_drift_total,
                schema_casualties_total: runtime.schema_casualties_total,
                truncate_events_total: runtime.truncate_events_total,
                schema_drift_detail: runtime.schema_drift_detail.clone(),
                dlq_write_failures_total: runtime.dlq_write_failures_total,
                backpressure_total: runtime.backpressure_total,
                sink_latency_p95_milliseconds: runtime.sink_latency_p95_milliseconds,
                active_operation_id: active_operation_id
                    .map_or_else(String::new, |id| id.to_string()),
                error_code: runtime.error_code.clone(),
                safe_error_message: runtime.safe_error_message.clone(),
                events_received_total: runtime.events_received_total,
                events_delivered_total: runtime.events_delivered_total,
                events_delivered_per_second: runtime.events_delivered_per_second,
                events_failed_total: runtime.events_failed_total,
                sink_retries_total: runtime.sink_retries_total,
                queue_depth: runtime.queue_depth,
                queue_capacity: runtime.queue_capacity,
                cpu_percent: runtime.cpu_percent,
                rss_bytes: runtime.rss_bytes,
                memory_limit_bytes: runtime.memory_limit_bytes,
                cpu_limit_millicores: runtime.cpu_limit_millicores,
                active_connections: runtime.active_connections,
                active_subscriptions: runtime.active_subscriptions,
                realtime_broker_observations_total: runtime.realtime_broker_observations_total,
                realtime_broker_observations_per_second: runtime
                    .realtime_broker_observations_per_second,
                realtime_dropped_total: runtime.realtime_dropped_total,
                realtime_terminal_failures_total: runtime.realtime_terminal_failures_total,
                last_input_at_unix_milliseconds: runtime.last_input_at_unix_milliseconds,
                last_output_at_unix_milliseconds: runtime.last_output_at_unix_milliseconds,
            })),
        })
    }

    /// Builds an operation update from a receipt already persisted locally.
    pub fn operation_update(
        &mut self,
        receipt: &OperationReceipt,
        now: DateTime<Utc>,
    ) -> Result<AgentToServer, AgentError> {
        let connection_id = self.established_connection_id()?;
        let sequence = self.take_agent_sequence()?;
        let (state, progress_percent, result_code) = match receipt.state {
            ReceiptState::Acknowledged => (ProtoOperationState::Acknowledged, 0, String::new()),
            ReceiptState::Running => (ProtoOperationState::Running, 50, String::new()),
            ReceiptState::Succeeded => (ProtoOperationState::Succeeded, 100, String::new()),
            ReceiptState::Failed => (
                ProtoOperationState::Failed,
                100,
                "engine_transition_failed".to_owned(),
            ),
        };
        Ok(AgentToServer {
            sequence,
            sent_at: Some(timestamp(now)),
            correlation_id: Uuid::now_v7().to_string(),
            connection_id: connection_id.to_string(),
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            payload: Some(AgentPayload::OperationUpdate(OperationUpdate {
                operation_id: receipt.operation_id.to_string(),
                state: state.into(),
                progress_percent,
                result_code,
                safe_message: receipt.message.clone().unwrap_or_default(),
                updated_at: Some(timestamp(now)),
            })),
        })
    }

    fn established_connection_id(&self) -> Result<Uuid, AgentError> {
        self.connection_id
            .ok_or_else(|| protocol_error("server hello has not been accepted"))
    }

    fn take_agent_sequence(&mut self) -> Result<u64, AgentError> {
        let sequence = self.next_agent_sequence;
        self.next_agent_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| protocol_error("agent sequence exhausted"))?;
        Ok(sequence)
    }
}

fn validate_envelope(
    message: &ServerToAgent,
    expected_sequence: u64,
    expected_connection_id: Option<Uuid>,
    now: DateTime<Utc>,
) -> Result<Uuid, AgentError> {
    let connection_id = canonical_uuid(&message.connection_id, "connection ID")?;
    if message.sequence != expected_sequence
        || message.protocol_major != PROTOCOL_MAJOR
        || message.protocol_minor != PROTOCOL_MINOR
        || expected_connection_id.is_some_and(|expected| expected != connection_id)
        || canonical_uuid(&message.correlation_id, "correlation ID").is_err()
        || parse_near_timestamp(message.sent_at.as_ref(), now).is_none()
    {
        return Err(protocol_error("server envelope is invalid"));
    }
    Ok(connection_id)
}

fn parse_server_hello(
    hello: &ServerHello,
    connection_id: Uuid,
    scope: &AgentScope,
    supported_capabilities: &[String],
    now: DateTime<Utc>,
) -> Result<ServerHelloData, AgentError> {
    let heartbeat_seconds = u64::from(hello.heartbeat_interval_seconds);
    let certificate_rotation_deadline =
        parse_timestamp(hello.certificate_rotation_deadline.as_ref())
            .filter(|deadline| *deadline > now)
            .ok_or_else(|| protocol_error("certificate rotation deadline is invalid"))?;
    if hello.protocol_major != PROTOCOL_MAJOR
        || hello.protocol_minor != PROTOCOL_MINOR
        || !(5..=40).contains(&heartbeat_seconds)
        || hello.deployment_generation == 0
        || parse_near_timestamp(hello.server_time.as_ref(), now).is_none()
        || !valid_identifiers(&hello.required_capabilities, 64)
        || !hello
            .required_capabilities
            .iter()
            .all(|required| supported_capabilities.contains(required))
    {
        return Err(protocol_error("server hello is invalid"));
    }

    let desired_state = hello
        .desired_state
        .as_ref()
        .map(|desired| parse_desired_state(desired, scope, now))
        .transpose()?;
    Ok(ServerHelloData {
        connection_id,
        heartbeat_interval: Duration::from_secs(heartbeat_seconds),
        deployment_generation: hello.deployment_generation,
        desired_state,
        required_capabilities: hello.required_capabilities.clone(),
        certificate_rotation_deadline,
    })
}

fn parse_desired_state(
    desired: &DesiredState,
    scope: &AgentScope,
    now: DateTime<Utc>,
) -> Result<DesiredStateSnapshot, AgentError> {
    let pipeline_id = canonical_uuid(&desired.pipeline_id, "pipeline ID")?;
    let deployment_id = canonical_uuid(&desired.deployment_id, "deployment ID")?;
    let run_state = match ProtoDesiredRunState::try_from(desired.run_state).ok() {
        Some(ProtoDesiredRunState::Running) => DesiredRunState::Running,
        Some(ProtoDesiredRunState::Paused) => DesiredRunState::Paused,
        Some(ProtoDesiredRunState::Drained) => DesiredRunState::Drained,
        Some(ProtoDesiredRunState::Unspecified) | None => {
            return Err(protocol_error("desired run state is invalid"));
        }
    };
    if pipeline_id != scope.pipeline_id
        || deployment_id != scope.deployment_id
        || desired.revision == 0
        || parse_timestamp(desired.changed_at.as_ref())
            .is_none_or(|changed_at| changed_at > now + MAX_CLOCK_SKEW)
        || !valid_metadata(&desired.changed_by, 500)
        || !valid_optional_metadata(&desired.reason, 1000)
    {
        return Err(protocol_error("desired-state snapshot is invalid"));
    }
    Ok(DesiredStateSnapshot {
        pipeline_id,
        deployment_id,
        revision: desired.revision,
        run_state,
        configuration_revision: (desired.configuration_revision != 0)
            .then_some(desired.configuration_revision),
    })
}

fn parse_agent_operation(
    operation: &Operation,
    correlation_id: &str,
    scope: &AgentScope,
    desired: &DesiredStateSnapshot,
    now: DateTime<Utc>,
) -> Result<ConvergenceOperation, AgentError> {
    let operation_id = canonical_uuid(&operation.operation_id, "operation ID")?;
    let correlation_id = canonical_uuid(correlation_id, "correlation ID")?;
    let kind = ProtoOperationKind::try_from(operation.kind).ok();
    let created_at = parse_timestamp(operation.created_at.as_ref())
        .ok_or_else(|| protocol_error("operation creation time is invalid"))?;
    let expires_at = parse_timestamp(operation.expires_at.as_ref())
        .ok_or_else(|| protocol_error("operation expiry time is invalid"))?;
    let selected_configuration_revision = desired.configuration_revision.unwrap_or(0);
    if operation_id != correlation_id
        || operation.operation_sequence == 0
        || operation.expected_desired_state_revision == 0
        || operation.expected_configuration_revision != selected_configuration_revision
        || !valid_optional_metadata(&operation.reason, 1000)
        || created_at > expires_at
        || created_at > now + MAX_CLOCK_SKEW
        || expires_at <= now
        || desired.pipeline_id != scope.pipeline_id
        || desired.deployment_id != scope.deployment_id
        || desired.revision != operation.expected_desired_state_revision
    {
        return Err(protocol_error("operation is invalid"));
    }
    let (kind, delete_orphans, destructive_action_confirmed, configuration) = match kind {
        Some(ProtoOperationKind::ConvergeState) if operation.parameters.is_none() => {
            (AgentOperationKind::ConvergeState, false, false, None)
        }
        Some(ProtoOperationKind::ApplyConfiguration) => match operation.parameters.as_ref() {
            Some(OperationParameters::ApplyConfiguration(parameters)) => (
                AgentOperationKind::ApplyConfiguration,
                false,
                false,
                Some(parse_configuration_bundle(parameters)?),
            ),
            _ => {
                return Err(protocol_error(
                    "apply-configuration operation parameters are invalid",
                ));
            }
        },
        Some(ProtoOperationKind::Reconcile) => match operation.parameters.as_ref() {
            Some(OperationParameters::Reconcile(parameters)) => (
                AgentOperationKind::Reconcile,
                parameters.delete_orphans,
                false,
                None,
            ),
            _ => return Err(protocol_error("reconcile operation parameters are invalid")),
        },
        Some(ProtoOperationKind::Rebootstrap) => match operation.parameters.as_ref() {
            Some(OperationParameters::Rebootstrap(parameters))
                if parameters.destructive_action_confirmed =>
            {
                (AgentOperationKind::Rebootstrap, false, true, None)
            }
            _ => {
                return Err(protocol_error(
                    "rebootstrap operation parameters are invalid",
                ));
            }
        },
        _ => return Err(protocol_error("operation kind is unsupported")),
    };

    Ok(ConvergenceOperation {
        operation_id,
        pipeline_id: scope.pipeline_id,
        deployment_id: scope.deployment_id,
        sequence: operation.operation_sequence,
        expected_desired_revision: operation.expected_desired_state_revision,
        kind,
        run_state: desired.run_state,
        delete_orphans,
        destructive_action_confirmed,
        configuration,
    })
}

fn parse_configuration_bundle(
    parameters: &ventstream_fleet_protocol::v1::ApplyConfigurationParameters,
) -> Result<ConfigurationBundle, AgentError> {
    if parameters.configuration_revision == 0
        || parameters.schema_version == 0
        || !valid_content_digest(&parameters.content_digest)
        || parameters.document_json.is_empty()
        || parameters.document_json.len() > MAX_CONFIGURATION_DOCUMENT_BYTES
        || serde_json::from_str::<serde_json::Value>(&parameters.document_json)
            .ok()
            .and_then(|value| value.as_object().map(|_| ()))
            .is_none()
    {
        return Err(protocol_error(
            "apply-configuration operation parameters are invalid",
        ));
    }
    Ok(ConfigurationBundle {
        revision: parameters.configuration_revision,
        schema_version: parameters.schema_version,
        content_digest: parameters.content_digest.clone(),
        document_json: parameters.document_json.clone(),
    })
}

fn valid_content_digest(value: &str) -> bool {
    value.len() == "sha256:".len() + 64
        && value.starts_with("sha256:")
        && value
            .bytes()
            .skip("sha256:".len())
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_near_timestamp(
    value: Option<&prost_types::Timestamp>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    parse_timestamp(value).filter(|timestamp| {
        *timestamp >= now - MAX_CLOCK_SKEW && *timestamp <= now + MAX_CLOCK_SKEW
    })
}

fn proto_observed_state(state: ObservedRunState) -> ProtoObservedRunState {
    match state {
        ObservedRunState::Starting => ProtoObservedRunState::Starting,
        ObservedRunState::Running => ProtoObservedRunState::Running,
        ObservedRunState::Paused => ProtoObservedRunState::Paused,
        ObservedRunState::Drained => ProtoObservedRunState::Drained,
        ObservedRunState::Error => ProtoObservedRunState::Error,
        ObservedRunState::Stopped => ProtoObservedRunState::Stopped,
    }
}

fn timestamp(value: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: i32::try_from(value.timestamp_subsec_nanos()).unwrap_or_default(),
    }
}

fn parse_timestamp(value: Option<&prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    let value = value?;
    let nanos = u32::try_from(value.nanos).ok()?;
    DateTime::from_timestamp(value.seconds, nanos)
}

fn canonical_uuid(value: &str, name: &str) -> Result<Uuid, AgentError> {
    let parsed =
        Uuid::parse_str(value).map_err(|_| protocol_error(format!("{name} is invalid")))?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(protocol_error(format!("{name} is invalid")));
    }
    Ok(parsed)
}

fn valid_identifiers(values: &[String], maximum_items: usize) -> bool {
    if values.len() > maximum_items {
        return false;
    }
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    values.iter().all(|value| {
        value.len() <= 120
            && !value.is_empty()
            && value.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || (index > 0 && matches!(byte, b'.' | b'_' | b'-' | b':'))
            })
            && seen.insert(value.as_str())
    })
}

fn valid_metadata(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_optional_metadata(value: &str, maximum: usize) -> bool {
    value.is_empty() || valid_metadata(value, maximum)
}

fn valid_optional_identifier(value: &str) -> bool {
    value.is_empty()
        || (value.len() <= 120
            && value.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || (index > 0 && matches!(byte, b'.' | b'_' | b'-' | b':'))
            }))
}

fn protocol_error(message: impl Into<String>) -> AgentError {
    AgentError::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};
    use uuid::Uuid;
    use ventstream_fleet_protocol::v1::agent_to_server::Payload as AgentPayload;
    use ventstream_fleet_protocol::v1::operation::Parameters as OperationParameters;
    use ventstream_fleet_protocol::v1::server_to_agent::Payload;
    use ventstream_fleet_protocol::v1::{
        ApplyConfigurationParameters, DesiredRunState as ProtoDesiredRunState, DesiredState,
        ObservedRunState as ProtoObservedRunState, Operation, OperationKind as ProtoOperationKind,
        RebootstrapParameters, ReconcileParameters, ServerHello, ServerToAgent,
    };

    use crate::{
        AgentDescriptor, AgentError, AgentRuntimeStatus, AgentScope, DesiredRunState,
        ManagementState, OperationReceipt, ReceiptState, ServerInstruction,
    };

    use super::ControlSession;

    fn scope() -> AgentScope {
        AgentScope {
            pipeline_id: Uuid::now_v7(),
            deployment_id: Uuid::now_v7(),
        }
    }

    fn descriptor() -> Result<AgentDescriptor, AgentError> {
        AgentDescriptor::new(
            Uuid::now_v7(),
            Uuid::now_v7(),
            "0.1.0",
            "sha256:test",
            vec!["cdc".to_owned()],
            vec!["status.v1".to_owned()],
        )
    }

    fn timestamp(value: chrono::DateTime<Utc>) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: value.timestamp(),
            nanos: i32::try_from(value.timestamp_subsec_nanos()).unwrap_or_default(),
        }
    }

    fn desired(scope: &AgentScope, revision: u64, state: ProtoDesiredRunState) -> DesiredState {
        desired_with_configuration(scope, revision, state, 0)
    }

    fn desired_with_configuration(
        scope: &AgentScope,
        revision: u64,
        state: ProtoDesiredRunState,
        configuration_revision: u64,
    ) -> DesiredState {
        DesiredState {
            pipeline_id: scope.pipeline_id.to_string(),
            deployment_id: scope.deployment_id.to_string(),
            revision,
            run_state: state.into(),
            configuration_revision,
            changed_at: Some(timestamp(Utc::now())),
            changed_by: "operator:test".to_owned(),
            reason: "test transition".to_owned(),
        }
    }

    fn envelope(
        sequence: u64,
        connection_id: Uuid,
        correlation_id: Uuid,
        payload: Payload,
    ) -> ServerToAgent {
        ServerToAgent {
            sequence,
            sent_at: Some(timestamp(Utc::now())),
            correlation_id: correlation_id.to_string(),
            connection_id: connection_id.to_string(),
            protocol_major: 1,
            protocol_minor: 0,
            payload: Some(payload),
        }
    }

    fn hello(scope: &AgentScope, connection_id: Uuid) -> ServerToAgent {
        envelope(
            1,
            connection_id,
            Uuid::now_v7(),
            Payload::Hello(ServerHello {
                protocol_major: 1,
                protocol_minor: 0,
                server_time: Some(timestamp(Utc::now())),
                heartbeat_interval_seconds: 15,
                deployment_generation: 1,
                desired_state: Some(desired(scope, 1, ProtoDesiredRunState::Paused)),
                required_capabilities: vec!["status.v1".to_owned()],
                certificate_rotation_deadline: Some(timestamp(
                    Utc::now() + ChronoDuration::hours(1),
                )),
            }),
        )
    }

    #[test]
    fn validates_hello_desired_state_and_convergence_in_exact_sequence() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut session = ControlSession::new(descriptor()?);
        let hello = session.accept(&hello(&scope, connection_id), &scope, None, Utc::now())?;
        let ServerInstruction::Hello(hello) = hello else {
            return Err(AgentError::Protocol("expected server hello".to_owned()));
        };
        let initial_desired = hello
            .desired_state
            .ok_or_else(|| AgentError::Protocol("desired state is missing".to_owned()))?;
        assert_eq!(initial_desired.run_state, DesiredRunState::Paused);
        assert_eq!(session.connection_id(), Some(connection_id));

        let operation_id = Uuid::now_v7();
        let operation_message = envelope(
            2,
            connection_id,
            operation_id,
            Payload::Operation(Operation {
                operation_id: operation_id.to_string(),
                kind: ProtoOperationKind::ConvergeState.into(),
                operation_sequence: 11,
                expected_desired_state_revision: 1,
                expected_configuration_revision: 0,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::seconds(1))),
                expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(5))),
                reason: "apply pause".to_owned(),
                parameters: None,
            }),
        );
        let instruction = session.accept(
            &operation_message,
            &scope,
            Some(&initial_desired),
            Utc::now(),
        )?;
        let ServerInstruction::Converge(operation) = instruction else {
            return Err(AgentError::Protocol(
                "expected convergence operation".to_owned(),
            ));
        };
        assert_eq!(operation.operation_id, operation_id);
        assert_eq!(operation.sequence, 11);
        assert_eq!(operation.run_state, DesiredRunState::Paused);
        Ok(())
    }

    #[test]
    fn rejected_envelope_does_not_advance_sequence_or_connection_fence() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut session = ControlSession::new(descriptor()?);
        let accepted = session.accept(&hello(&scope, connection_id), &scope, None, Utc::now())?;
        let ServerInstruction::Hello(hello) = accepted else {
            return Err(AgentError::Protocol("expected server hello".to_owned()));
        };
        let initial_desired = hello
            .desired_state
            .ok_or_else(|| AgentError::Protocol("desired state is missing".to_owned()))?;

        let skipped = envelope(
            3,
            connection_id,
            Uuid::now_v7(),
            Payload::DesiredState(desired(&scope, 2, ProtoDesiredRunState::Running)),
        );
        assert!(
            session
                .accept(&skipped, &scope, Some(&initial_desired), Utc::now())
                .is_err()
        );

        let correct = envelope(
            2,
            connection_id,
            Uuid::now_v7(),
            Payload::DesiredState(desired(&scope, 2, ProtoDesiredRunState::Running)),
        );
        let accepted = session.accept(&correct, &scope, Some(&initial_desired), Utc::now())?;
        assert!(matches!(accepted, ServerInstruction::DesiredState(_)));

        let wrong_fence = envelope(
            3,
            Uuid::now_v7(),
            Uuid::now_v7(),
            Payload::DesiredState(desired(&scope, 3, ProtoDesiredRunState::Drained)),
        );
        assert!(
            session
                .accept(&wrong_fence, &scope, None, Utc::now())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_expired_and_unsupported_operations() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut session = ControlSession::new(descriptor()?);
        let accepted = session.accept(&hello(&scope, connection_id), &scope, None, Utc::now())?;
        let ServerInstruction::Hello(hello) = accepted else {
            return Err(AgentError::Protocol("expected server hello".to_owned()));
        };
        let initial_desired = hello
            .desired_state
            .ok_or_else(|| AgentError::Protocol("desired state is missing".to_owned()))?;
        let operation_id = Uuid::now_v7();
        let expired = envelope(
            2,
            connection_id,
            operation_id,
            Payload::Operation(Operation {
                operation_id: operation_id.to_string(),
                kind: ProtoOperationKind::ConvergeState.into(),
                operation_sequence: 1,
                expected_desired_state_revision: 1,
                expected_configuration_revision: 0,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::minutes(2))),
                expires_at: Some(timestamp(Utc::now() - ChronoDuration::minutes(1))),
                reason: String::new(),
                parameters: None,
            }),
        );
        assert!(
            session
                .accept(&expired, &scope, Some(&initial_desired), Utc::now())
                .is_err()
        );

        let operation_id = Uuid::now_v7();
        let unsupported = envelope(
            2,
            connection_id,
            operation_id,
            Payload::Operation(Operation {
                operation_id: operation_id.to_string(),
                kind: ProtoOperationKind::RotateIdentity.into(),
                operation_sequence: 1,
                expected_desired_state_revision: 1,
                expected_configuration_revision: 0,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::seconds(1))),
                expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(1))),
                reason: String::new(),
                parameters: None,
            }),
        );
        assert!(
            session
                .accept(&unsupported, &scope, Some(&initial_desired), Utc::now())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn accepts_reconcile_and_confirmed_rebootstrap_operations() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut session = ControlSession::new(descriptor()?);
        let accepted = session.accept(&hello(&scope, connection_id), &scope, None, Utc::now())?;
        let ServerInstruction::Hello(hello) = accepted else {
            return Err(AgentError::Protocol("expected server hello".to_owned()));
        };
        let initial_desired = hello
            .desired_state
            .ok_or_else(|| AgentError::Protocol("desired state is missing".to_owned()))?;

        let operation_id = Uuid::now_v7();
        let reconcile = envelope(
            2,
            connection_id,
            operation_id,
            Payload::Operation(Operation {
                operation_id: operation_id.to_string(),
                kind: ProtoOperationKind::Reconcile.into(),
                operation_sequence: 1,
                expected_desired_state_revision: 1,
                expected_configuration_revision: 0,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::seconds(1))),
                expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(1))),
                reason: String::new(),
                parameters: Some(OperationParameters::Reconcile(ReconcileParameters {
                    delete_orphans: true,
                })),
            }),
        );
        let ServerInstruction::Converge(operation) =
            session.accept(&reconcile, &scope, Some(&initial_desired), Utc::now())?
        else {
            return Err(AgentError::Protocol(
                "expected reconcile operation".to_owned(),
            ));
        };
        assert_eq!(operation.kind, crate::AgentOperationKind::Reconcile);
        assert!(operation.delete_orphans);

        let operation_id = Uuid::now_v7();
        let rebootstrap = envelope(
            3,
            connection_id,
            operation_id,
            Payload::Operation(Operation {
                operation_id: operation_id.to_string(),
                kind: ProtoOperationKind::Rebootstrap.into(),
                operation_sequence: 2,
                expected_desired_state_revision: 1,
                expected_configuration_revision: 0,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::seconds(1))),
                expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(1))),
                reason: "rebuild sink".to_owned(),
                parameters: Some(OperationParameters::Rebootstrap(RebootstrapParameters {
                    destructive_action_confirmed: true,
                })),
            }),
        );
        let ServerInstruction::Converge(operation) =
            session.accept(&rebootstrap, &scope, Some(&initial_desired), Utc::now())?
        else {
            return Err(AgentError::Protocol(
                "expected rebootstrap operation".to_owned(),
            ));
        };
        assert_eq!(operation.kind, crate::AgentOperationKind::Rebootstrap);
        assert!(operation.destructive_action_confirmed);
        Ok(())
    }

    #[test]
    fn accepts_apply_configuration_only_for_selected_revision() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut session = ControlSession::new(descriptor()?);
        let hello_message = envelope(
            1,
            connection_id,
            Uuid::now_v7(),
            Payload::Hello(ServerHello {
                protocol_major: 1,
                protocol_minor: 0,
                server_time: Some(timestamp(Utc::now())),
                heartbeat_interval_seconds: 15,
                deployment_generation: 1,
                desired_state: Some(desired_with_configuration(
                    &scope,
                    7,
                    ProtoDesiredRunState::Running,
                    12,
                )),
                required_capabilities: vec!["status.v1".to_owned()],
                certificate_rotation_deadline: Some(timestamp(
                    Utc::now() + ChronoDuration::hours(1),
                )),
            }),
        );
        let accepted = session.accept(&hello_message, &scope, None, Utc::now())?;
        let ServerInstruction::Hello(hello) = accepted else {
            return Err(AgentError::Protocol("expected server hello".to_owned()));
        };
        let selected_desired = hello
            .desired_state
            .ok_or_else(|| AgentError::Protocol("desired state is missing".to_owned()))?;

        let operation_id = Uuid::now_v7();
        let apply = envelope(
            2,
            connection_id,
            operation_id,
            Payload::Operation(Operation {
                operation_id: operation_id.to_string(),
                kind: ProtoOperationKind::ApplyConfiguration.into(),
                operation_sequence: 3,
                expected_desired_state_revision: 7,
                expected_configuration_revision: 12,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::seconds(1))),
                expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(1))),
                reason: "apply selected config".to_owned(),
                parameters: Some(OperationParameters::ApplyConfiguration(
                    ApplyConfigurationParameters {
                        configuration_revision: 12,
                        schema_version: 1,
                        content_digest:
                            "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                                .to_owned(),
                        document_json: r#"{"version":1}"#.to_owned(),
                    },
                )),
            }),
        );
        let ServerInstruction::Converge(operation) =
            session.accept(&apply, &scope, Some(&selected_desired), Utc::now())?
        else {
            return Err(AgentError::Protocol(
                "expected apply configuration operation".to_owned(),
            ));
        };
        assert_eq!(
            operation.kind,
            crate::AgentOperationKind::ApplyConfiguration
        );
        assert_eq!(
            operation
                .configuration
                .as_ref()
                .map(|bundle| bundle.revision),
            Some(12)
        );

        let wrong_revision = envelope(
            3,
            connection_id,
            Uuid::now_v7(),
            Payload::Operation(Operation {
                operation_id: Uuid::now_v7().to_string(),
                kind: ProtoOperationKind::ApplyConfiguration.into(),
                operation_sequence: 4,
                expected_desired_state_revision: 7,
                expected_configuration_revision: 11,
                created_at: Some(timestamp(Utc::now() - ChronoDuration::seconds(1))),
                expires_at: Some(timestamp(Utc::now() + ChronoDuration::minutes(1))),
                reason: "apply stale config".to_owned(),
                parameters: Some(OperationParameters::ApplyConfiguration(
                    ApplyConfigurationParameters {
                        configuration_revision: 11,
                        schema_version: 1,
                        content_digest:
                            "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                                .to_owned(),
                        document_json: r#"{"version":1}"#.to_owned(),
                    },
                )),
            }),
        );
        assert!(
            session
                .accept(&wrong_revision, &scope, Some(&selected_desired), Utc::now())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn outbound_messages_use_durable_cursors_and_exact_agent_sequence() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut state = ManagementState::new(scope.clone());
        state.desired = Some(crate::DesiredStateSnapshot {
            pipeline_id: scope.pipeline_id,
            deployment_id: scope.deployment_id,
            revision: 3,
            run_state: DesiredRunState::Paused,
            configuration_revision: None,
        });
        state.observed = crate::ObservedRunState::Paused;
        let receipt = OperationReceipt {
            operation_id: Uuid::now_v7(),
            sequence: 8,
            expected_desired_revision: 3,
            kind: crate::AgentOperationKind::ConvergeState,
            run_state: DesiredRunState::Paused,
            delete_orphans: false,
            destructive_action_confirmed: false,
            configuration_revision: None,
            state: ReceiptState::Succeeded,
            message: None,
            updated_at: Utc::now(),
        };
        state.last_operation_sequence = 9;
        state.operation_receipts.push(receipt.clone());
        state.operation_receipts.push(OperationReceipt {
            operation_id: Uuid::now_v7(),
            sequence: 9,
            expected_desired_revision: 3,
            kind: crate::AgentOperationKind::ConvergeState,
            run_state: DesiredRunState::Paused,
            delete_orphans: false,
            destructive_action_confirmed: false,
            configuration_revision: None,
            state: ReceiptState::Running,
            message: None,
            updated_at: Utc::now(),
        });

        let mut session = ControlSession::new(descriptor()?);
        let outbound_hello = session.agent_hello(&state, Utc::now())?;
        let Some(AgentPayload::Hello(agent_hello)) = outbound_hello.payload else {
            return Err(AgentError::Protocol("expected agent hello".to_owned()));
        };
        assert_eq!(outbound_hello.sequence, 1);
        assert_eq!(agent_hello.last_desired_state_revision, 3);
        assert_eq!(agent_hello.last_operation_sequence, 8);
        assert!(session.agent_status(&state, None, Utc::now()).is_err());

        session.accept(&hello(&scope, connection_id), &scope, None, Utc::now())?;
        let status = session.agent_status(&state, Some(receipt.operation_id), Utc::now())?;
        assert_eq!(status.sequence, 2);
        assert_eq!(status.connection_id, connection_id.to_string());
        let update = session.operation_update(&receipt, Utc::now())?;
        assert_eq!(update.sequence, 3);
        let Some(AgentPayload::OperationUpdate(update)) = update.payload else {
            return Err(AgentError::Protocol("expected operation update".to_owned()));
        };
        assert_eq!(update.progress_percent, 100);
        assert_eq!(update.result_code, "");
        Ok(())
    }

    #[test]
    fn runtime_sample_populates_status_and_failed_readiness_reports_error() -> Result<(), AgentError>
    {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut state = ManagementState::new(scope.clone());
        state.observed = crate::ObservedRunState::Running;
        let mut session = ControlSession::new(descriptor()?);
        session.accept(&hello(&scope, connection_id), &scope, None, Utc::now())?;
        let runtime = AgentRuntimeStatus {
            source_kind: "postgres".to_owned(),
            sink_kind: "opensearch".to_owned(),
            events_per_second: Some(12.5),
            realtime_broker_observations_total: Some(250),
            realtime_broker_observations_per_second: Some(31.25),
            dlq_writes_total: Some(2),
            sink_latency_p95_milliseconds: Some(9),
            engine_ready: Some(false),
            error_code: "engine_not_ready".to_owned(),
            safe_error_message: "Supervised engine readiness probe failed".to_owned(),
            ..AgentRuntimeStatus::default()
        };
        let message = session.agent_status_with_runtime(&state, None, &runtime, Utc::now())?;
        let Some(AgentPayload::Status(status)) = message.payload else {
            return Err(AgentError::Protocol("expected agent status".to_owned()));
        };
        assert_eq!(status.source_kind, "postgres");
        assert_eq!(status.sink_kind, "opensearch");
        assert_eq!(status.events_per_second, Some(12.5));
        assert_eq!(status.realtime_broker_observations_total, Some(250));
        assert_eq!(status.realtime_broker_observations_per_second, Some(31.25));
        assert_eq!(status.dlq_writes_total, Some(2));
        assert_eq!(status.sink_latency_p95_milliseconds, Some(9));
        assert_eq!(status.error_code, "engine_not_ready");
        assert_eq!(status.run_state, i32::from(ProtoObservedRunState::Error));
        Ok(())
    }

    #[test]
    fn server_required_capabilities_must_be_supported() -> Result<(), AgentError> {
        let scope = scope();
        let connection_id = Uuid::now_v7();
        let mut message = hello(&scope, connection_id);
        if let Some(Payload::Hello(hello)) = message.payload.as_mut() {
            hello.required_capabilities = vec!["configuration.v1".to_owned()];
        }
        let mut session = ControlSession::new(descriptor()?);

        assert!(session.accept(&message, &scope, None, Utc::now()).is_err());
        assert_eq!(session.connection_id(), None);
        Ok(())
    }
}
