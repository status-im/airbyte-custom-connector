# Elasticsearch Query Source

Generic Airbyte source: run one or more Lucene `query_string` searches against
an Elasticsearch index, and emit one row per hit. Incremental
cursor is a date field (default `@timestamp`).

Each query is its own stream with its own cursor.

Filter in the query; parse `message` in dbt to get `peer_id` / `ip` columns.

## Config

| Field | Required | Notes |
|---|---|---|
| `endpoint` | yes | Cluster URL, no index path |
| `index` | yes | e.g. `logstash-*` |
| `start_date` | yes | ISO-8601 UTC, first-sync floor |
| `queries` | yes | Array of `{name, query}`. Each item is a stream |
| `cursor_field` | no | Default `@timestamp` |
| `extra_fields` | no | Extra `_source` fields to expose as columns |
| `page_size` | no | Default 500. Lower it if the connection drops |
| `request_timeout` | no | Read timeout, seconds. Default 60 |
| `connect_timeout` | no | TCP connect timeout, seconds. Default 10 |
| `max_retries` | no | Retries per search. Default 5 |
| `username` / `password` | yes | Basic auth |

Example for Logos Kademlia peer lines on **devnet** and **testnet**:

```json
{
  "endpoint": "http://elasticsearch.example.com:9200",
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
  "start_date": "2026-08-05T07:00:00Z",
  "username": "elastic",
  "password": "<password>"
}
```

## Destination

One stream per query. Stream names come from `queries[].name`
