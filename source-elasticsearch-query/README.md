# Elasticsearch Query Source

Generic Airbyte source: run one or more Lucene `query_string` searches against
an Elasticsearch index (or index pattern) and emit one row per hit. Incremental
cursor is a date field (default `@timestamp`).

Each query is its own stream with its own cursor. A shared cursor would skip
hits from slower queries.

This is not Logos-specific. Filter in the query; parse `message` in dbt if you
need `peer_id` / `ip` columns.

## Config

| Field | Required | Notes |
|---|---|---|
| `endpoint` | yes | Cluster URL, no index path |
| `index` | yes | e.g. `logstash-*` |
| `start_date` | yes | ISO-8601 UTC, first-sync floor |
| `queries` | no | Array of `{name, query}`. Each item is a stream |
| `query` | no | Fallback if `queries` is empty. Stream name `documents` |
| `cursor_field` | no | Default `@timestamp` |
| `page_size` | no | Default 500 |
| `username` / `password` | no | Basic auth |
| `api_key` | no | `Authorization: ApiKey …` (wins over basic auth) |

Example for Logos Kademlia peer lines on **devnet** and **testnet**:

```json
{
  "endpoint": "https://your-elastic:9200",
  "index": "logstash-*",
  "queries": [
    {
      "name": "logos_dev_peers",
      "query": "fleet:\"logos.dev\" AND message:\"Added address /ip4/\""
    },
    {
      "name": "logos_test_peers",
      "query": "fleet:\"logos.test\" AND message:\"Added address /ip4/\""
    }
  ],
  "start_date": "2026-08-05T00:00:00Z"
}
```

If `queries` is omitted, a single `query` string still works and creates stream
`documents`.

## Destination

One stream per query. Stream names come from `queries[].name` (sanitized to
letters, numbers, underscores). Primary key `_index` + `_id`. Use
**Incremental | Append + Deduped**.

Each record is `_id`, `_index`, `query_name`, plus every field from `_source`
(`message`, `@timestamp`, `fleet`, `program`, `host`, …). All query streams
share `schemas/hits.json`; Airbyte cannot look up `schemas/{stream_name}.json`
because names come from config.

HTTP 400/401/403/404 fail the sync. Zero new hits is a successful idle run.
