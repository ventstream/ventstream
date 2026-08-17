use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use uuid::Uuid;
use ventstream_fleet_client::{
    ConfigurationBundle, DesiredRunState, EngineAdapter, ObservedRunState,
};

const MAX_STAGED_CONFIGURATION_BYTES: usize = 96 * 1024;
const FLEET_CHILD_ENVIRONMENT: [&str; 22] = [
    "VS_AGENT_KEY",
    "VS_FLEET_GATEWAY_URL",
    "VS_FLEET_PIPELINE_ID",
    "VS_FLEET_DEPLOYMENT_ID",
    "VS_FLEET_INSTANCE_ID",
    "VS_FLEET_CERT_CHAIN_PATH",
    "VS_FLEET_PRIVATE_KEY_PATH",
    "VS_FLEET_TRUST_BUNDLE_PATH",
    "VS_FLEET_CREDENTIAL_STATE_PATH",
    "VS_FLEET_CERTIFICATE_SERIAL",
    "VS_FLEET_STATE_PATH",
    "VS_FLEET_ENGINE_BIN",
    "VS_FLEET_ENGINE_CONFIG_PATH",
    "VS_FLEET_ENGINE_HEALTH_URL",
    "VS_FLEET_ENGINE_START_TIMEOUT_SECS",
    "VS_FLEET_ENGINE_STOP_TIMEOUT_SECS",
    "VS_FLEET_ENGINE_DRAIN_TIMEOUT_SECS",
    "VS_FLEET_FORCE_BOOTSTRAP",
    "VS_FLEET_APPLIED_CONFIG_PATH",
    "VS_FLEET_APPLIED_CONFIG_REVISION",
    "VS_FLEET_APPLIED_CONFIG_DIGEST",
    "VS_ENGINE_CONFIG",
];

#[derive(Serialize)]
struct StagedConfiguration<'a> {
    revision: u64,
    schema_version: u64,
    content_digest: &'a str,
    document: serde_json::Value,
}

/// Process operations required by the Fleet run-state adapter.
pub trait EngineProcessControl: Send {
    /// Ensures the data process is running and locally ready.
    fn ensure_running(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    /// Gracefully stops the data process, with a bounded forced fallback.
    fn ensure_stopped(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    /// Idempotently releases local resume state while stopped.
    fn drain_local_state(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    /// Idempotently reconciles source and sink state.
    fn reconcile(
        &mut self,
        delete_orphans: bool,
    ) -> impl Future<Output = Result<(), String>> + Send;
    /// Idempotently rebuilds sink state from the source.
    fn rebootstrap(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    /// Idempotently stages a non-secret configuration bundle for the local engine.
    fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Fleet adapter that maps desired run state to one supervised engine process.
pub struct ProcessEngineAdapter<C> {
    process: C,
    desired: Option<DesiredRunState>,
}

impl<C> ProcessEngineAdapter<C> {
    /// Wraps a concrete process controller.
    pub fn new(process: C) -> Self {
        Self {
            process,
            desired: None,
        }
    }

    /// Returns the underlying process controller.
    pub fn process(&self) -> &C {
        &self.process
    }
}

impl<C: EngineProcessControl> ProcessEngineAdapter<C> {
    /// Gracefully stops the child when the supervisor itself is terminating.
    pub async fn shutdown(&mut self) -> Result<(), String> {
        self.process.ensure_stopped().await
    }

    async fn apply(&mut self, desired: DesiredRunState) -> Result<ObservedRunState, String> {
        match desired {
            DesiredRunState::Running => {
                self.process.ensure_running().await?;
                Ok(ObservedRunState::Running)
            }
            DesiredRunState::Paused => {
                self.process.ensure_stopped().await?;
                Ok(ObservedRunState::Paused)
            }
            DesiredRunState::Drained => {
                self.process.ensure_stopped().await?;
                self.process.drain_local_state().await?;
                Ok(ObservedRunState::Drained)
            }
        }
    }

    async fn restore_running_if_desired(&mut self) -> Result<(), String> {
        if self.desired == Some(DesiredRunState::Running) {
            self.apply(DesiredRunState::Running).await?;
        }
        Ok(())
    }
}

impl<C: EngineProcessControl> EngineAdapter for ProcessEngineAdapter<C> {
    async fn apply_startup_gate(
        &mut self,
        desired: Option<DesiredRunState>,
    ) -> Result<ObservedRunState, String> {
        self.desired = desired;
        match desired {
            Some(desired) => self.apply(desired).await,
            None => {
                self.process.ensure_stopped().await?;
                Ok(ObservedRunState::Stopped)
            }
        }
    }

    async fn apply_run_state(
        &mut self,
        desired: DesiredRunState,
    ) -> Result<ObservedRunState, String> {
        self.desired = Some(desired);
        self.apply(desired).await
    }

    async fn reconcile(&mut self, delete_orphans: bool) -> Result<(), String> {
        self.process.reconcile(delete_orphans).await?;
        self.restore_running_if_desired().await
    }

    async fn rebootstrap(&mut self) -> Result<(), String> {
        self.process.rebootstrap().await?;
        self.restore_running_if_desired().await
    }

    async fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> Result<(), String> {
        self.process.apply_configuration(configuration).await?;
        self.restore_running_if_desired().await
    }
}

/// Tokio-backed controller for the open-core `ventstream` child binary.
pub struct TokioEngineProcess {
    binary: PathBuf,
    configuration_path: PathBuf,
    health_url: reqwest::Url,
    start_timeout: Duration,
    stop_timeout: Duration,
    drain_timeout: Duration,
    health_client: reqwest::Client,
    child: Option<Child>,
    force_next_bootstrap: bool,
    applied_configuration: Option<ConfigurationBundle>,
}

/// Engine process implementation used by the Kubernetes smoke test.
pub struct SmokeEngineProcess {
    listen: SocketAddr,
    configuration_path: Option<PathBuf>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl SmokeEngineProcess {
    /// Creates a smoke process that serves local engine readiness.
    pub fn new(listen: SocketAddr) -> Self {
        Self {
            listen,
            configuration_path: None,
            task: None,
        }
    }

    /// Creates a smoke process that also stages managed configuration locally.
    pub fn with_configuration_path(listen: SocketAddr, configuration_path: PathBuf) -> Self {
        Self {
            listen,
            configuration_path: Some(configuration_path),
            task: None,
        }
    }

    async fn start(&mut self) -> Result<(), String> {
        if self.task.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }
        let listener = TcpListener::bind(self.listen)
            .await
            .map_err(|_| "starting smoke engine health listener failed".to_owned())?;
        self.task = Some(tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    let Ok(bytes_read) = stream.read(&mut buffer).await else {
                        return;
                    };
                    let Some(request_bytes) = buffer.get(..bytes_read) else {
                        return;
                    };
                    let request = String::from_utf8_lossy(request_bytes);
                    let status = if request.starts_with("GET /readyz ")
                        || request.starts_with("GET /healthz ")
                    {
                        "200 OK"
                    } else {
                        "404 Not Found"
                    };
                    let body = if status == "200 OK" {
                        "ok\n"
                    } else {
                        "not found\n"
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        }));
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for SmokeEngineProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Concrete process implementation selected by runtime configuration.
pub enum SupervisorEngineProcess {
    /// Supervise the real open-core VentStream engine binary.
    Real(Box<TokioEngineProcess>),
    /// Serve a local health endpoint without starting an engine process.
    Smoke(SmokeEngineProcess),
}

impl TokioEngineProcess {
    /// Creates a process controller from already validated runtime configuration.
    pub fn new(
        binary: PathBuf,
        configuration_path: PathBuf,
        health_url: reqwest::Url,
        start_timeout: Duration,
        stop_timeout: Duration,
        drain_timeout: Duration,
    ) -> Result<Self, String> {
        let health_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "building local health client failed".to_owned())?;
        Ok(Self {
            binary,
            configuration_path,
            health_url,
            start_timeout,
            stop_timeout,
            drain_timeout,
            health_client,
            child: None,
            force_next_bootstrap: false,
            applied_configuration: None,
        })
    }

    /// Gracefully stops the child process.
    pub async fn shutdown(&mut self) -> Result<(), String> {
        self.stop_child().await
    }

    async fn start_child(&mut self) -> Result<(), String> {
        if let Some(child) = self.child.as_mut() {
            match child.try_wait() {
                Ok(None) => return self.wait_until_ready().await,
                Ok(Some(_)) => self.child = None,
                Err(_) => return Err("inspecting engine process failed".to_owned()),
            }
        }

        let mut command = self.engine_command();
        if self.force_next_bootstrap {
            command.env("VS_FLEET_FORCE_BOOTSTRAP", "1");
        }
        let child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .env("VS_FLEET_SUPERVISED", "1")
            .spawn()
            .map_err(|_| "starting engine process failed".to_owned())?;
        self.child = Some(child);

        if let Err(error) = self.wait_until_ready().await {
            drop(self.stop_child().await);
            return Err(error);
        }
        self.force_next_bootstrap = false;
        Ok(())
    }

    async fn wait_until_ready(&mut self) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + self.start_timeout;
        loop {
            let child = self
                .child
                .as_mut()
                .ok_or_else(|| "engine process disappeared".to_owned())?;
            match child.try_wait() {
                Ok(Some(status)) => {
                    self.child = None;
                    return Err(format!("engine process exited before readiness ({status})"));
                }
                Ok(None) => {}
                Err(_) => return Err("inspecting engine process failed".to_owned()),
            }

            if self
                .health_client
                .get(self.health_url.clone())
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("engine readiness timed out".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn stop_child(&mut self) -> Result<(), String> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child
            .try_wait()
            .map_err(|_| "inspecting engine process failed".to_owned())?
            .is_some()
        {
            return Ok(());
        }

        if let Some(mut stdin) = child.stdin.take()
            && stdin.write_all(b"shutdown\n").await.is_ok()
        {
            drop(stdin.shutdown().await);
        }
        match tokio::time::timeout(self.stop_timeout, child.wait()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => Err("waiting for engine shutdown failed".to_owned()),
            Err(_) => {
                child
                    .kill()
                    .await
                    .map_err(|_| "forcing engine shutdown failed".to_owned())?;
                child
                    .wait()
                    .await
                    .map_err(|_| "reaping engine process failed".to_owned())?;
                Ok(())
            }
        }
    }

    async fn run_maintenance_command(
        &mut self,
        args: &[&str],
        timeout: Duration,
        label: &str,
    ) -> Result<(), String> {
        self.stop_child().await?;
        let mut command = self.engine_command();
        let mut child = command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| format!("starting {label} command failed"))?;
        let status = match tokio::time::timeout(timeout, child.wait()).await {
            Ok(result) => result.map_err(|_| format!("waiting for {label} failed"))?,
            Err(_) => {
                child
                    .kill()
                    .await
                    .map_err(|_| format!("stopping timed-out {label} failed"))?;
                drop(child.wait().await);
                return Err(format!("{label} timed out"));
            }
        };
        if !status.success() {
            return Err(format!("{label} command failed ({status})"));
        }
        Ok(())
    }

    async fn run_drain(&mut self) -> Result<(), String> {
        let timeout = self.drain_timeout;
        self.run_maintenance_command(&["--fleet-drain-local-state"], timeout, "engine drain")
            .await
    }

    fn engine_command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        if let Some(configuration_directory) = self
            .configuration_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            command.current_dir(configuration_directory);
        }
        scrub_child_environment(&mut command);
        self.apply_configuration_environment(&mut command);
        command
    }

    fn apply_configuration_environment(&self, command: &mut Command) {
        if self.applied_configuration.is_some() || self.configuration_path.exists() {
            command.env("VS_FLEET_APPLIED_CONFIG_PATH", &self.configuration_path);
        }
        let engine_config_path = self.engine_config_path();
        if engine_config_path.exists() {
            command.env("VS_ENGINE_CONFIG", engine_config_path);
        }
        if let Some(configuration) = &self.applied_configuration {
            command
                .env(
                    "VS_FLEET_APPLIED_CONFIG_REVISION",
                    configuration.revision.to_string(),
                )
                .env(
                    "VS_FLEET_APPLIED_CONFIG_DIGEST",
                    &configuration.content_digest,
                );
        }
    }

    fn engine_config_path(&self) -> PathBuf {
        self.configuration_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .join("ventstream.yaml")
    }
}

fn scrub_child_environment(command: &mut Command) {
    for name in FLEET_CHILD_ENVIRONMENT {
        command.env_remove(name);
    }
    command
        .env_remove("VS_CONTROL_PLANE_URL")
        .env_remove("VS_CONTROL_PLANE_KEY");
}

fn stage_configuration_file(
    destination: &Path,
    configuration: &ConfigurationBundle,
) -> Result<(), String> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| "configuration path must have a parent directory".to_owned())?;
    create_private_directory(parent)?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|_| "configuration directory cannot be inspected".to_owned())?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err("configuration parent must be a real directory".to_owned());
    }
    validate_private_directory(parent, &parent_metadata)?;
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("staged configuration must be a regular file".to_owned());
        }
        validate_private_file(destination, &metadata)?;
    }

    let document = serde_json::from_str::<serde_json::Value>(&configuration.document_json)
        .map_err(|_| "configuration document is not valid JSON".to_owned())?;
    let document_object = document
        .as_object()
        .ok_or_else(|| "configuration document must be a JSON object".to_owned())?;
    if !document_object.contains_key("schema_version") {
        return Err("configuration document is missing schema_version".to_owned());
    }
    stage_engine_config_files(parent, document_object)?;
    let staged = StagedConfiguration {
        revision: configuration.revision,
        schema_version: configuration.schema_version,
        content_digest: &configuration.content_digest,
        document,
    };
    let encoded = serde_json::to_vec(&staged)
        .map_err(|_| "configuration envelope could not be encoded".to_owned())?;
    if encoded.len() > MAX_STAGED_CONFIGURATION_BYTES {
        return Err("staged configuration exceeds the local size limit".to_owned());
    }

    let temporary = parent.join(format!(".applied-configuration-{}.tmp", Uuid::now_v7()));
    let result = write_private_file_and_replace(&temporary, destination, parent, &encoded);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn stage_engine_config_files(
    root: &Path,
    document: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let Some(engine_config_yaml) = document.get("engine_config_yaml") else {
        return Ok(());
    };
    let engine_config_yaml = engine_config_yaml
        .as_str()
        .ok_or_else(|| "engine_config_yaml must be a string".to_owned())?;
    stage_managed_config_text(root, "ventstream.yaml", engine_config_yaml.as_bytes())?;
    if let Some(files) = document.get("files") {
        let files = files
            .as_object()
            .ok_or_else(|| "configuration files must be an object".to_owned())?;
        for (name, contents) in files {
            let contents = contents
                .as_str()
                .ok_or_else(|| "configuration file contents must be strings".to_owned())?;
            stage_managed_config_text(root, name, contents.as_bytes())?;
        }
    }
    Ok(())
}

fn stage_managed_config_text(root: &Path, name: &str, contents: &[u8]) -> Result<(), String> {
    if contents.len() > 128 * 1024 {
        return Err("managed configuration file exceeds local size limit".to_owned());
    }
    let destination = managed_config_destination(root, name)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| "managed configuration file must have a parent".to_owned())?;
    create_private_directory(parent)?;
    let metadata = fs::symlink_metadata(parent)
        .map_err(|_| "managed configuration directory cannot be inspected".to_owned())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("managed configuration parent must be a real directory".to_owned());
    }
    validate_private_directory(parent, &metadata)?;
    if let Ok(metadata) = fs::symlink_metadata(&destination) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("managed configuration target must be a regular file".to_owned());
        }
        validate_private_file(&destination, &metadata)?;
    }
    let temporary = parent.join(format!(".managed-config-{}.tmp", Uuid::now_v7()));
    let result = write_private_file_and_replace(&temporary, &destination, parent, contents);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn managed_config_destination(root: &Path, name: &str) -> Result<PathBuf, String> {
    if !valid_managed_config_path(name) {
        return Err("managed configuration file path is unsafe".to_owned());
    }
    Ok(root.join(name.trim_start_matches("./")))
}

fn valid_managed_config_path(value: &str) -> bool {
    if value.trim() != value
        || value.is_empty()
        || value.chars().count() > 255
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return false;
    }
    let value = value.strip_prefix("./").unwrap_or(value);
    value
        .split('/')
        .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn write_private_file_and_replace(
    temporary: &Path,
    destination: &Path,
    parent: &Path,
    encoded: &[u8],
) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_private_file_mode(&mut options);
    let mut file = options
        .open(temporary)
        .map_err(|_| "creating staged configuration failed".to_owned())?;
    file.write_all(encoded)
        .map_err(|_| "writing staged configuration failed".to_owned())?;
    file.write_all(b"\n")
        .map_err(|_| "writing staged configuration failed".to_owned())?;
    file.sync_all()
        .map_err(|_| "syncing staged configuration failed".to_owned())?;
    drop(file);
    fs::rename(temporary, destination)
        .map_err(|_| "activating staged configuration failed".to_owned())?;
    sync_directory(parent).map_err(|_| "syncing configuration directory failed".to_owned())
}

/// Directory durability barrier; no-op on Windows, which cannot open
/// directories via `File::open` and journals renames in NTFS.
#[cfg(unix)]
fn sync_directory(parent: &Path) -> std::io::Result<()> {
    fs::File::open(parent).and_then(|directory| directory.sync_all())
}

#[cfg(not(unix))]
fn sync_directory(_parent: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
        .create(path)
        .map_err(|_| "creating configuration directory failed".to_owned())
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|_| "creating configuration directory failed".to_owned())
}

#[cfg(unix)]
fn set_private_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_file_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn validate_private_directory(path: &Path, metadata: &fs::Metadata) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "configuration directory {} must not be accessible by group or other users",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_directory(_path: &Path, _metadata: &fs::Metadata) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn validate_private_file(path: &Path, metadata: &fs::Metadata) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "staged configuration {} must not be accessible by group or other users",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_file(_path: &Path, _metadata: &fs::Metadata) -> Result<(), String> {
    Ok(())
}

impl EngineProcessControl for TokioEngineProcess {
    async fn ensure_running(&mut self) -> Result<(), String> {
        self.start_child().await
    }

    async fn ensure_stopped(&mut self) -> Result<(), String> {
        self.stop_child().await
    }

    async fn drain_local_state(&mut self) -> Result<(), String> {
        self.run_drain().await
    }

    async fn reconcile(&mut self, delete_orphans: bool) -> Result<(), String> {
        if !delete_orphans {
            return Ok(());
        }
        let timeout = self.drain_timeout;
        self.run_maintenance_command(
            &["--fleet-reconcile", "--delete-orphans"],
            timeout,
            "engine reconcile",
        )
        .await
    }

    async fn rebootstrap(&mut self) -> Result<(), String> {
        let timeout = self.drain_timeout;
        self.run_maintenance_command(&["--fleet-rebootstrap"], timeout, "engine rebootstrap")
            .await?;
        self.force_next_bootstrap = true;
        Ok(())
    }

    async fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> Result<(), String> {
        if self.applied_configuration.as_ref().is_some_and(|applied| {
            applied.revision == configuration.revision
                && applied.content_digest == configuration.content_digest
        }) {
            return Ok(());
        }
        stage_configuration_file(&self.configuration_path, configuration)?;
        self.applied_configuration = Some(configuration.clone());
        self.stop_child().await?;
        Ok(())
    }
}

impl EngineProcessControl for SmokeEngineProcess {
    async fn ensure_running(&mut self) -> Result<(), String> {
        self.start().await
    }

    async fn ensure_stopped(&mut self) -> Result<(), String> {
        self.stop();
        Ok(())
    }

    async fn drain_local_state(&mut self) -> Result<(), String> {
        Ok(())
    }

    async fn reconcile(&mut self, _delete_orphans: bool) -> Result<(), String> {
        Ok(())
    }

    async fn rebootstrap(&mut self) -> Result<(), String> {
        Ok(())
    }

    async fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> Result<(), String> {
        if let Some(path) = &self.configuration_path {
            stage_configuration_file(path, configuration)?;
        }
        Ok(())
    }
}

impl EngineProcessControl for SupervisorEngineProcess {
    async fn ensure_running(&mut self) -> Result<(), String> {
        match self {
            Self::Real(process) => process.ensure_running().await,
            Self::Smoke(process) => process.ensure_running().await,
        }
    }

    async fn ensure_stopped(&mut self) -> Result<(), String> {
        match self {
            Self::Real(process) => process.ensure_stopped().await,
            Self::Smoke(process) => process.ensure_stopped().await,
        }
    }

    async fn drain_local_state(&mut self) -> Result<(), String> {
        match self {
            Self::Real(process) => process.drain_local_state().await,
            Self::Smoke(process) => process.drain_local_state().await,
        }
    }

    async fn reconcile(&mut self, delete_orphans: bool) -> Result<(), String> {
        match self {
            Self::Real(process) => process.reconcile(delete_orphans).await,
            Self::Smoke(process) => process.reconcile(delete_orphans).await,
        }
    }

    async fn rebootstrap(&mut self) -> Result<(), String> {
        match self {
            Self::Real(process) => process.rebootstrap().await,
            Self::Smoke(process) => process.rebootstrap().await,
        }
    }

    async fn apply_configuration(
        &mut self,
        configuration: &ConfigurationBundle,
    ) -> Result<(), String> {
        match self {
            Self::Real(process) => process.apply_configuration(configuration).await,
            Self::Smoke(process) => process.apply_configuration(configuration).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self, std::io::Error> {
            let path = std::env::temp_dir().join(format!("fleet-agent-process-{}", Uuid::now_v7()));
            fs::create_dir(&path)?;
            set_private_test_directory_permissions(&path)?;
            Ok(Self(path))
        }

        fn config_path(&self) -> PathBuf {
            self.0.join("applied-configuration.json")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn configuration_bundle(revision: u64) -> ConfigurationBundle {
        ConfigurationBundle {
            revision,
            schema_version: 1,
            content_digest:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            document_json: r#"{"schema_version":1,"source":{},"sink":{}}"#.to_owned(),
        }
    }

    fn canonical_configuration_bundle(revision: u64) -> ConfigurationBundle {
        ConfigurationBundle {
            revision,
            schema_version: 1,
            content_digest:
                "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_owned(),
            document_json: serde_json::json!({
                "schema_version": 1,
                "engine_config_yaml": "schema_version: 1\nroles: [cdc]\nsource:\n  kind: postgres\nspecs:\n  joins: joins.yaml\n",
                "files": {
                    "joins.yaml": "joins: []\n",
                    "nested/graphql-schema.graphql": "type Query { health: Boolean }\n"
                }
            })
            .to_string(),
        }
    }

    #[cfg(unix)]
    fn set_private_test_directory_permissions(path: &Path) -> Result<(), std::io::Error> {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
    }

    #[cfg(not(unix))]
    fn set_private_test_directory_permissions(_path: &Path) -> Result<(), std::io::Error> {
        Ok(())
    }

    #[derive(Default)]
    struct FakeProcess {
        running: bool,
        starts: usize,
        stops: usize,
        drains: usize,
        reconciles: usize,
        rebootstraps: usize,
        configuration_applies: usize,
        stop_during_maintenance: bool,
    }

    impl EngineProcessControl for FakeProcess {
        async fn ensure_running(&mut self) -> Result<(), String> {
            if !self.running {
                self.running = true;
                self.starts += 1;
            }
            Ok(())
        }

        async fn ensure_stopped(&mut self) -> Result<(), String> {
            if self.running {
                self.running = false;
                self.stops += 1;
            }
            Ok(())
        }

        async fn drain_local_state(&mut self) -> Result<(), String> {
            self.drains += 1;
            Ok(())
        }

        async fn reconcile(&mut self, _delete_orphans: bool) -> Result<(), String> {
            if self.stop_during_maintenance {
                self.ensure_stopped().await?;
            }
            self.reconciles += 1;
            Ok(())
        }

        async fn rebootstrap(&mut self) -> Result<(), String> {
            if self.stop_during_maintenance {
                self.ensure_stopped().await?;
            }
            self.rebootstraps += 1;
            Ok(())
        }

        async fn apply_configuration(
            &mut self,
            _configuration: &ConfigurationBundle,
        ) -> Result<(), String> {
            if self.stop_during_maintenance {
                self.ensure_stopped().await?;
            }
            self.configuration_applies += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn adapter_is_idempotent_across_run_pause_and_drain() -> Result<(), String> {
        let mut adapter = ProcessEngineAdapter::new(FakeProcess::default());

        assert_eq!(
            adapter
                .apply_startup_gate(Some(DesiredRunState::Running))
                .await?,
            ObservedRunState::Running
        );
        assert_eq!(
            adapter.apply_run_state(DesiredRunState::Running).await?,
            ObservedRunState::Running
        );
        assert_eq!(adapter.process().starts, 1);

        assert_eq!(
            adapter.apply_run_state(DesiredRunState::Paused).await?,
            ObservedRunState::Paused
        );
        assert_eq!(adapter.process().stops, 1);
        assert_eq!(
            adapter.apply_run_state(DesiredRunState::Drained).await?,
            ObservedRunState::Drained
        );
        assert_eq!(adapter.process().stops, 1);
        assert_eq!(adapter.process().drains, 1);
        Ok(())
    }

    #[tokio::test]
    async fn absent_desired_state_starts_fail_closed() -> Result<(), String> {
        let process = FakeProcess {
            running: true,
            ..FakeProcess::default()
        };
        let mut adapter = ProcessEngineAdapter::new(process);
        assert_eq!(
            adapter.apply_startup_gate(None).await?,
            ObservedRunState::Stopped
        );
        assert_eq!(adapter.process().stops, 1);
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_restores_running_process_when_desired_running() -> Result<(), String> {
        let mut adapter = ProcessEngineAdapter::new(FakeProcess {
            stop_during_maintenance: true,
            ..FakeProcess::default()
        });
        assert_eq!(
            adapter.apply_run_state(DesiredRunState::Running).await?,
            ObservedRunState::Running
        );

        adapter.reconcile(true).await?;

        assert!(adapter.process().running);
        assert_eq!(adapter.process().starts, 2);
        assert_eq!(adapter.process().stops, 1);
        assert_eq!(adapter.process().reconciles, 1);
        Ok(())
    }

    #[tokio::test]
    async fn rebootstrap_does_not_restart_when_desired_paused() -> Result<(), String> {
        let process = FakeProcess {
            running: true,
            stop_during_maintenance: true,
            ..FakeProcess::default()
        };
        let mut adapter = ProcessEngineAdapter::new(process);
        assert_eq!(
            adapter.apply_run_state(DesiredRunState::Paused).await?,
            ObservedRunState::Paused
        );

        adapter.rebootstrap().await?;

        assert!(!adapter.process().running);
        assert_eq!(adapter.process().starts, 0);
        assert_eq!(adapter.process().stops, 1);
        assert_eq!(adapter.process().rebootstraps, 1);
        Ok(())
    }

    #[tokio::test]
    async fn apply_configuration_restores_running_process_after_staging() -> Result<(), String> {
        let mut adapter = ProcessEngineAdapter::new(FakeProcess {
            stop_during_maintenance: true,
            ..FakeProcess::default()
        });
        assert_eq!(
            adapter.apply_run_state(DesiredRunState::Running).await?,
            ObservedRunState::Running
        );

        adapter
            .apply_configuration(&configuration_bundle(2))
            .await?;

        assert!(adapter.process().running);
        assert_eq!(adapter.process().configuration_applies, 1);
        assert_eq!(adapter.process().stops, 1);
        assert_eq!(adapter.process().starts, 2);
        Ok(())
    }

    #[test]
    fn configuration_stage_file_is_private_atomic_and_enveloped() -> Result<(), String> {
        let directory = TestDirectory::new().map_err(|error| error.to_string())?;
        let path = directory.config_path();
        let configuration = configuration_bundle(9);

        stage_configuration_file(&path, &configuration)?;

        let encoded = fs::read_to_string(&path).map_err(|error| error.to_string())?;
        let staged: serde_json::Value =
            serde_json::from_str(&encoded).map_err(|error| error.to_string())?;
        assert_eq!(
            staged
                .pointer("/revision")
                .and_then(serde_json::Value::as_u64),
            Some(9)
        );
        assert_eq!(
            staged
                .pointer("/schema_version")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            staged
                .pointer("/content_digest")
                .and_then(serde_json::Value::as_str),
            Some(configuration.content_digest.as_str())
        );
        assert_eq!(
            staged
                .pointer("/document/schema_version")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(&path)
                .map_err(|error| error.to_string())?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        Ok(())
    }

    #[test]
    fn configuration_stage_writes_canonical_engine_files() -> Result<(), String> {
        let directory = TestDirectory::new().map_err(|error| error.to_string())?;
        let path = directory.config_path();

        stage_configuration_file(&path, &canonical_configuration_bundle(10))?;

        let root = path
            .parent()
            .ok_or_else(|| "config path has no parent".to_owned())?;
        assert!(root.join("applied-configuration.json").exists());
        assert_eq!(
            fs::read_to_string(root.join("ventstream.yaml")).map_err(|error| error.to_string())?,
            "schema_version: 1\nroles: [cdc]\nsource:\n  kind: postgres\nspecs:\n  joins: joins.yaml\n\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("joins.yaml")).map_err(|error| error.to_string())?,
            "joins: []\n\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("nested/graphql-schema.graphql"))
                .map_err(|error| error.to_string())?,
            "type Query { health: Boolean }\n\n"
        );
        Ok(())
    }

    #[test]
    fn configuration_stage_rejects_unsafe_managed_file_paths() -> Result<(), String> {
        let directory = TestDirectory::new().map_err(|error| error.to_string())?;
        let mut configuration = canonical_configuration_bundle(11);
        configuration.document_json = serde_json::json!({
            "schema_version": 1,
            "engine_config_yaml": "schema_version: 1\nroles: [cdc]\nsource:\n  kind: postgres\n",
            "files": {
                "../escape.yaml": "bad: true\n"
            }
        })
        .to_string();

        let error = match stage_configuration_file(&directory.config_path(), &configuration) {
            Ok(()) => return Err("unsafe path should be rejected".to_owned()),
            Err(error) => error,
        };

        assert_eq!(error, "managed configuration file path is unsafe");
        Ok(())
    }

    #[test]
    fn managed_engine_commands_run_from_the_staged_configuration_directory() -> Result<(), String> {
        let directory = TestDirectory::new().map_err(|error| error.to_string())?;
        let process = TokioEngineProcess::new(
            std::env::current_exe().map_err(|error| error.to_string())?,
            directory.config_path(),
            reqwest::Url::parse("http://127.0.0.1:4043/readyz")
                .map_err(|error| error.to_string())?,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )?;

        let command = process.engine_command();
        assert_eq!(
            command.as_std().get_current_dir(),
            Some(directory.0.as_path())
        );
        Ok(())
    }

    #[tokio::test]
    async fn smoke_process_serves_engine_health_endpoints() -> Result<(), String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|error| format!("binding test listener failed: {error}"))?;
        let listen = listener
            .local_addr()
            .map_err(|error| format!("reading test listener address failed: {error}"))?;
        drop(listener);

        let mut process = SmokeEngineProcess::new(listen);
        process.ensure_running().await?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("building test client failed: {error}"))?;
        let ready_response = client
            .get(format!("http://{listen}/readyz"))
            .send()
            .await
            .map_err(|error| format!("calling ready endpoint failed: {error}"))?;
        assert!(ready_response.status().is_success());
        let health_response = client
            .get(format!("http://{listen}/healthz"))
            .send()
            .await
            .map_err(|error| format!("calling health endpoint failed: {error}"))?;
        assert!(health_response.status().is_success());
        let missing_response = client
            .get(format!("http://{listen}/missing"))
            .send()
            .await
            .map_err(|error| format!("calling missing endpoint failed: {error}"))?;
        assert_eq!(missing_response.status().as_u16(), 404);

        process.ensure_stopped().await?;
        Ok(())
    }
}
