# VentStream standalone binary

This archive contains the open-core VentStream engine. It runs without the
Fleet control plane.

1. Copy `ventstream.example.yaml` to `ventstream.yaml`.
2. Set the environment variables referenced by the configuration.
3. Validate the complete configuration:

   ```sh
   VS_ENGINE_CONFIG=./ventstream.yaml ./ventstream --validate-config
   ```

4. Start the engine:

   ```sh
   VS_ENGINE_CONFIG=./ventstream.yaml ./ventstream
   ```

The example is a PostgreSQL CDC pipeline targeting OpenSearch. Connector and
realtime configurations are documented at https://github.com/ventstream/ventstream/tree/main/docs-site.

The standalone engine has no Fleet credentials or control-plane dependency.
Centralized CLI administration requires a separately deployed Fleet-managed
engine.
