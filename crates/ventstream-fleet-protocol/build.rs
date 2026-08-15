//! Compiles the versioned Fleet Protobuf contract.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/ventstream/fleet/v1/agent.proto");
    // Vendored protoc keeps CI and container builds free of a system dependency.
    let mut config = tonic_prost_build::Config::new();
    if std::env::var_os("PROTOC").is_none() {
        config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    }
    tonic_prost_build::configure()
        // AgentStatus is intentionally inline: heartbeats are frequent and do not
        // warrant a heap allocation solely to reduce a generated enum's size.
        .type_attribute(
            ".ventstream.fleet.v1.AgentToServer.payload",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_with_config(
            config,
            &["../../proto/ventstream/fleet/v1/agent.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
