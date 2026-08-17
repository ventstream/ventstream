//! Windows stub for the Kafka source.
//!
//! The real source binds librdkafka (C), which has no supported Windows
//! build in this project. The stub keeps the configuration surface
//! compiling on Windows and fails with a clear operator-facing error the
//! moment a Kafka pipeline actually starts.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use ventstream_core::{Source, SourceContext, SourceError};

use super::config::KafkaCdcConfig;

/// Unsupported-platform placeholder with the same construction API as the
/// real source.
pub struct KafkaCdcSource {
    config: KafkaCdcConfig,
}

impl KafkaCdcSource {
    /// Accepts the parsed configuration so validation and logging behave
    /// identically up to the run boundary.
    pub fn new(config: KafkaCdcConfig) -> Self {
        Self { config }
    }

    /// Present for signature parity; the progress handle is unused because
    /// the source never runs.
    pub fn with_sink_progress(self, _progress: Arc<AtomicU64>) -> Self {
        self
    }
}

#[async_trait]
impl Source for KafkaCdcSource {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "kafka_cdc"
    }

    async fn run(&self, _ctx: SourceContext) -> Result<(), SourceError> {
        Err(SourceError::Internal(
            "the Kafka source is not supported on Windows; run this pipeline \
             on Linux/macOS or in the container image"
                .to_owned(),
        ))
    }
}
