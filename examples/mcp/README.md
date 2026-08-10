# Ask an AI agent about your live data

End-to-end demo: a Postgres shop database → VentStream → Meilisearch,
with the built-in MCP server (`ventstream mcp`) letting Claude (or any
MCP client) answer questions about orders — live, without touching
Postgres.

What you get: joined order documents (order + customer + line items)
kept in sync within milliseconds, and an AI agent that can `search`
and `get_entity` over them with a scoped access token.

## 1. Start Postgres and Meilisearch

Any Postgres 14+ with `wal_level=logical` and any Meilisearch work.
Quickest path with Docker:

```bash
docker run -d --name vs-demo-pg -p 5432:5432 \
  -e POSTGRES_USER=vs -e POSTGRES_PASSWORD=vs -e POSTGRES_DB=shop \
  postgres:16 -c wal_level=logical

docker run -d --name vs-demo-meili -p 7700:7700 \
  -e MEILI_MASTER_KEY=demo-master-key \
  getmeili/meilisearch:latest
```

Seed the shop:

```bash
psql postgres://vs:vs@localhost:5432/shop -f seed.sql
```

## 2. Run the pipeline

```bash
export VS_PG_HOST=localhost VS_PG_USER=vs VS_PG_PASSWORD=vs VS_PG_DATABASE=shop
export VS_MEILI_ENDPOINT=http://localhost:7700 VS_MEILI_API_KEY=demo-master-key
VS_ENGINE_CONFIG=ventstream.yaml ventstream
```

Within seconds the `shop_orders` index holds five composed documents —
each order embedding its customer and line items.

## 3. Mint a token and start the MCP server

```bash
ventstream mcp generate-token --hash
# vsk_…            ← the agent's token (shown once)
# sha256:…         ← paste into keys.yaml
```

Copy `keys.yaml.example` to `keys.yaml` and replace the hash. Then, in
a second terminal (same env vars):

```bash
ventstream mcp --config ventstream.yaml \
  --listen 127.0.0.1:8790 \
  --keys-ref file:keys.yaml
```

## 4. Connect Claude

Claude Code:

```bash
claude mcp add --transport http ventstream http://127.0.0.1:8790/mcp \
  --header "Authorization: Bearer vsk_…"
```

Claude Desktop (stdio — no HTTP server needed) — add to
`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ventstream": {
      "command": "ventstream",
      "args": ["mcp", "--config", "/absolute/path/to/ventstream.yaml"],
      "env": {
        "VS_PG_HOST": "localhost", "VS_PG_USER": "vs",
        "VS_PG_PASSWORD": "vs", "VS_PG_DATABASE": "shop",
        "VS_MEILI_ENDPOINT": "http://localhost:7700",
        "VS_MEILI_API_KEY": "demo-master-key"
      }
    }
  }
}
```

## 5. Ask

- "What can you tell me about order 100?"
- "Find every order for Acme Corp. What did they buy?"
- "Which orders are still pending, and what's their total value?"
- "Does anyone have rack servers on order?"

The agent discovers the tools and the document shape by itself — the
joins spec doubles as its schema documentation.

## 6. Watch it stay live

Apply some changes while everything runs:

```bash
psql postgres://vs:vs@localhost:5432/shop -f live-changes.sql
```

Then ask again: "what's the status of order 104?" (shipped), "what
about order 101?" (gone — the delete propagated), "list order 100's
items" (the PDUs are gone from the embedded array). Every answer
reflects the database as of seconds ago.

## Files

| File | Purpose |
| --- | --- |
| `seed.sql` | Shop schema + data (customers, orders, order_items) |
| `joins.yaml` | The join: orders embed their customer + items |
| `ventstream.yaml` | Postgres → Meilisearch pipeline config |
| `keys.yaml.example` | Scoped access key for the agent |
| `live-changes.sql` | Mid-demo changes proving freshness |

Docs: https://ventstream.dev/docs/guides/mcp-server
