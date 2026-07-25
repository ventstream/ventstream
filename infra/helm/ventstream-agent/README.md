# ventstream-agent (deprecated)

This chart belongs to VentStream's retired API-key telemetry control plane. It
requires `VS_CONTROL_PLANE_URL`, `VS_CONTROL_PLANE_KEY`, and a legacy agent name.
Those values are not Fleet enrollment credentials, and this chart does not run
the Fleet supervisor.

Do not use this chart for a new standalone or Fleet-managed installation.

- For standalone CDC, follow
  [`docs-site/deploy/kubernetes-standalone.mdx`](../../../docs-site/deploy/kubernetes-standalone.mdx)
  and run the engine from canonical `ventstream.yaml` without control-plane
  variables.
- For Fleet-managed CDC or realtime, use the `ventstream-managed-agent` chart
  from the sibling `ventstream-control-plane` repository.
- For standalone WebSocket or GraphQL roles, use `ventstream-gateway`.

The chart remains in the repository temporarily so existing legacy installations
can inspect and migrate their prior values. It receives no new connector or Fleet
features. In particular, its generated development Secret is not correct for every
newer connector and must not be treated as a production secret template.
