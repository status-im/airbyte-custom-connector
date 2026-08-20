# Logos Testnet Logs Source

Airbyte custom connector that fetches hourly blockchain node logs from the Logos
testnet index server, parses them in memory, and emits three incremental streams:

- `peers`     — peer connection events (timestamp, peer_id).
- `stake`     — `TSI update` total-stake values over time.
- `ip_peers`  — unique public (ip, peer_id) mappings.

Each hourly log file is downloaded to a temp directory, parsed once (shared
in-memory across the three streams within a single sync), then deleted
immediately. Incremental state tracks the last processed hour per node, so
already-parsed logs are never re-fetched.

## Local development

### Prerequisites

Python `>= 3.9`.

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Config

Create `secrets/config.json`:

```json
{
  "username": "strode",
  "password": "...",
  "nodes": ["0", "1", "2", "3"],
  "base_url": "https://testnet.blockchain.logos.co/internal/node-data",
  "start_date": "2026-04-15-14"
}
```

`start_date` is a UTC hour in `YYYY-MM-DD-HH` format and is **inclusive**
(the hour you name is fetched). If omitted, the connector defaults to the
last 2 hours.

## GeoIP enrichment

Every public IPv4 address emitted on the `ip_peers` stream is resolved to
geolocation fields via the free [ip-api.com](https://ip-api.com/) batch
endpoint. The `ip_peers` records carry five additional columns:

- `lat`         — latitude (float, nullable)
- `lon`         — longitude (float, nullable)
- `country`     — full country name (e.g. `"Germany"`, nullable)
- `country_code`— ISO 3166-1 alpha-2 (e.g. `"DE"`, nullable)
- `city`        — city name (nullable)

A single `GeoLocator` cache is shared across every `(node, hour)` slice of a
sync, so each unique IP is looked up at most once per sync.

Notes:

- **Free tier, no API key needed.** ip-api.com allows ~15 batch calls per
  minute; we sleep 5 s between batches to stay safely below that.
- **Fail-soft.** If the API is unreachable or returns an error, the five geo
  fields are emitted as `null` for the affected IPs and the sync continues.
- **Private IPs are already filtered out** upstream in `parse_log`, so they
  never hit the API.

## Backfilling a past day or window

The connector supports an optional `end_date` (`YYYY-MM-DD-HH`, UTC,
inclusive of the hour) which, together with `start_date` (`YYYY-MM-DD-HH`,
UTC, inclusive of the hour), bounds the fetch window to
`[start_date, end_date]`.

If `end_date` is omitted, the connector computes it once at sync start as
the last complete UTC hour (`now - 1h`, truncated to the hour) and pins it
across every node and every stream. This guarantees all three streams and
all nodes stop at the same cutoff even though new hours may roll over on
the index server during the sync, and keeps the hourly cron from ever
ingesting a partially-written current-hour log.

Example config that backfills the whole of 2026-04-10:

```json
{
  "username": "strode",
  "password": "...",
  "nodes": ["0", "1", "2", "3"],
  "start_date": "2026-04-10-00",
  "end_date":   "2026-04-10-23"
}
```

Because the streams are incremental, Airbyte will only fetch hours strictly
greater than the cursor it already has. So a backfill only works if the cursor
is either absent (fresh connection) or already earlier than `start_date`. You
have two options.

### Option A — Clear state on the existing connection

Use when you don't mind temporarily pausing hourly syncs on this connection.

1. In the Airbyte UI, open the connection and pause it.
2. Edit the source config: set `start_date` and `end_date` to the window you
   want to backfill.
3. Go to the connection's **Settings** tab → **Clear data** (or call the state
   API with an empty state). This wipes the cursor.
4. Trigger a manual sync. Only the hours inside `[start_date, end_date]` will
   be fetched. Existing rows in the destination are upserted on the primary
   keys, so re-running a day is idempotent.
5. Once the backfill finishes, edit the source config back to your normal
   settings (remove `end_date`, restore your usual `start_date`) and unpause
   the schedule.

Caveat: after the backfill, the cursor points at `end_date`. The next
hourly sync will catch up from the hour right after `end_date` to "now - 1h"
in one go. This is usually fine; if you have a long gap, expect the first
post-backfill sync to take a while.

### Option B — Dedicated backfill connection (recommended)

Use when you want your hourly sync to keep running untouched.

1. Create a **second source** in Airbyte pointing at the same base URL and
   credentials, but with `start_date` / `end_date` set to the backfill window.
2. Create a **second connection** from that source to the same destination
   (same namespace / streams as the hourly connection).
3. Trigger a manual sync. It will fetch exactly the hours in the window and
   upsert into the same destination tables.

This is the safest option.

**NB: fetching the data for 1 full day for the 4 nodes takes approximately 2h.**

### Don't put large backfills windows !

**Memory grows with the size of the window.** The `LogProcessor` caches parsed
records for every `(node, hour)` slice it has downloaded, so all three streams
(`peers`, `stake`, `ip_peers`) can share a single fetch + parse per log file.
Entries are **not** evicted until the sync ends. RAM therefore grows roughly
linearly with `nodes × hours_in_window`.

Rough scaling, dominated by `peers` (observed at ~1–5 MB of Python objects per
hour-node against real testnet data):

| window   | nodes | slices | approx. peak RAM    |
|----------|-------|--------|---------------------|
| 1 h      | 4     | 4      | a few MB            |
| 24 h     | 4     | 96     | ~100–500 MB         |
| 7 days   | 4     | 672    | ~1–3.5 GB           |
| 30 days  | 4     | 2 880  | ~5–15 GB (OOM risk) |

Airbyte workers are often capped at 2 GB by default. A 7-day × 4-node backfill
can brush against that limit; anything beyond ~10 days is very likely to
OOM-kill the sync.

**Possible solutions:**

- **Chunk the window.** Run the backfill one or two days at a time.
- **Reduce `nodes`.** Running one node at a time.
