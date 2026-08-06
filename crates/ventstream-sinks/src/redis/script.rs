use redis::{ErrorKind, FromRedisValue, ServerErrorKind, ToRedisArgs};

use super::topology::RedisConnection;

pub(super) struct RedisScript {
    source: &'static str,
    hash: String,
}

impl RedisScript {
    pub(super) fn new(source: &'static str) -> Self {
        Self {
            source,
            hash: redis::Script::new(source).get_hash().to_owned(),
        }
    }

    pub(super) fn prepare_invoke(&self) -> RedisScriptInvocation<'_> {
        RedisScriptInvocation {
            script: self,
            keys: Vec::new(),
            args: Vec::new(),
        }
    }
}

pub(super) struct RedisScriptInvocation<'a> {
    script: &'a RedisScript,
    keys: Vec<Vec<u8>>,
    args: Vec<Vec<u8>>,
}

impl RedisScriptInvocation<'_> {
    pub(super) fn key<T: ToRedisArgs>(&mut self, key: T) -> &mut Self {
        key.write_redis_args(&mut self.keys);
        self
    }

    pub(super) fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
        arg.write_redis_args(&mut self.args);
        self
    }

    pub(super) async fn invoke<T: FromRedisValue>(
        &self,
        connection: &mut RedisConnection,
        routing_key: &str,
    ) -> redis::RedisResult<T> {
        match connection
            .query_on_primary::<T>(self.evalsha_command(), routing_key)
            .await
        {
            Ok(value) => Ok(value),
            Err(error) if is_no_script(&error) => {
                ventstream_telemetry::bump_redis_script_cache_misses();
                let mut load = redis::cmd("SCRIPT");
                load.arg("LOAD").arg(self.script.source.as_bytes());
                let loaded = connection
                    .query_on_primary::<String>(load, routing_key)
                    .await?;
                if loaded != self.script.hash {
                    return Err(redis::RedisError::from((
                        ErrorKind::UnexpectedReturnType,
                        "Redis SCRIPT LOAD returned an unexpected hash",
                    )));
                }
                ventstream_telemetry::bump_redis_script_loads();
                connection
                    .query_on_primary::<T>(self.evalsha_command(), routing_key)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    fn evalsha_command(&self) -> redis::Cmd {
        let mut command = redis::cmd("EVALSHA");
        command.arg(&self.script.hash).arg(self.keys.len());
        for key in &self.keys {
            command.arg(key);
        }
        for arg in &self.args {
            command.arg(arg);
        }
        command
    }
}

fn is_no_script(error: &redis::RedisError) -> bool {
    if error.kind() == ErrorKind::Server(ServerErrorKind::NoScript)
        || error
            .code()
            .is_some_and(|code| code.eq_ignore_ascii_case("NOSCRIPT"))
        || error.to_string().contains("NOSCRIPT:")
    {
        return true;
    }
    error.clone().into_server_errors().is_some_and(|errors| {
        errors.iter().any(|(_, server_error)| {
            server_error.kind() == Some(ServerErrorKind::NoScript)
                || server_error.code().eq_ignore_ascii_case("NOSCRIPT")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_separates_keys_from_arguments() {
        let script = RedisScript::new("return {KEYS[1], ARGV[1]}");
        let mut invocation = script.prepare_invoke();
        invocation.key("key").arg("value");
        let packed = invocation.evalsha_command().get_packed_command();
        let text = String::from_utf8_lossy(&packed);
        assert!(text.contains("EVALSHA"));
        assert!(text.contains("key"));
        assert!(text.contains("value"));
    }
}
