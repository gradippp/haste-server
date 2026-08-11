# haste-server

A pastebin. Write something, save it, share the URL.

Originally [haste-server](https://github.com/toptal/haste-server) by John
Crepezzi — now a single Next.js 15 / React 19 / TypeScript app: the UI and the
API are the same server, the storage backends and key generators carried over,
and the original `/documents` API still works so existing CLI clients keep
running.

## Quick start

```bash
pnpm install
```

```bash
pnpm run dev
```

That serves on <http://localhost:3000> using the `file` store, which writes to
`./data`. For production:

```bash
pnpm run build && pnpm start
```

## Docker

```bash
docker compose up --build
```

Brings up the app on port 3000 backed by redis, with documents expiring after
30 days. The compose file has commented-out service blocks for postgres,
mariadb, memcached, and mongo — see the notes at the bottom of it.

The image ships the `redis`, `pg`, and `mysql2` drivers. Other backends need
their driver added to the `drivers` stage in the `Dockerfile`.

## API

### Current

| Route | Description |
|---|---|
| `POST /api/pastes` | Create. `multipart/form-data` or JSON with `content`, `language`, `title`. Returns `{ id, revocation_key }`. |
| `GET /api/pastes/:id` | Returns `{ content, language, title }`. |
| `GET /api/pastes/:id/meta` | Metadata only — `{ id, title, language, created_at }`. |
| `DELETE /api/pastes/:id?revocation_key=…` | Deletes. `403` without a matching key. |
| `GET /:id/raw` | The paste as `text/plain`. |

Errors carry a JSON `{ "error": "message" }` body.

Pastes are anonymous — there are no accounts. The `revocation_key` returned at
creation is the only thing that authorizes deleting a paste; the browser keeps
it in `localStorage` under `paste_<id>`, so the delete button appears only for
whoever created the paste, from the browser they created it in.

### Legacy (original haste API)

Unchanged, so `haste`, editor plugins, and shell functions keep working:

| Route | Description |
|---|---|
| `POST /documents` | Raw body, or `multipart/form-data` with a `data` field. Returns `{ key }`. |
| `GET\|HEAD /documents/:id` | Returns `{ data, key }`. |
| `GET\|HEAD /raw/:id` | The paste as `text/plain`. |

An `:id` may carry an extension (`/raw/abc.js`); it's stripped. These respond
with `{ "message": "…" }` on error, as they always did.

```bash
curl -X POST --data 'hello world' http://localhost:3000/documents
```

## Configuration

`config.json` holds the defaults; every value can be overridden by an
environment variable, which is what the Docker image and compose file use. The
variable names are the same ones the pre-rewrite `docker-entrypoint.sh`
produced.

| Variable | `config.json` key | Default | Description |
|---|---|---|---|
| `CONFIG_PATH` | — | `./config.json` | Where to read the config file from. |
| `KEY_LENGTH` | `keyLength` | `10` | Length of a generated key. |
| `MAX_LENGTH` | `maxLength` | `400000` | Longest accepted document, in characters. |
| `STORAGE_TYPE` | `storage.type` | `file` | See [Storage](#storage). |
| `STORAGE_HOST` | `storage.host` | — | Database host. |
| `STORAGE_PORT` | `storage.port` | — | Database port. |
| `STORAGE_USERNAME` | `storage.user` | — | Database user. |
| `STORAGE_PASSWORD` | `storage.password` | — | Database password. |
| `STORAGE_DB` | `storage.db` | — | Database name, or index for redis. |
| `STORAGE_CONNECTION_URL` | `storage.connectionUrl` | — | Full connection URL, where the driver supports one. |
| `STORAGE_FILEPATH` | `storage.path` | `./data` | Directory for the `file` store. |
| `STORAGE_EXPIRE_SECONDS` | `storage.expire` | — | Seconds until a document expires. Unset means never. |
| `STORAGE_AWS_BUCKET` | `storage.bucket` | — | `amazon-s3` bucket. |
| `STORAGE_AWS_REGION` | `storage.region` | — | `amazon-s3` region. |
| `KEYGENERATOR_TYPE` | `keyGenerator.type` | `random` | `random`, `phonetic`, or `dictionary`. |
| `KEYGENERATOR_KEYSPACE` | `keyGenerator.keyspace` | alphanumerics | Characters the `random` generator draws from. |
| `KEYGENERATOR_PATH` | `keyGenerator.path` | — | Word list for the `dictionary` generator. |
| `RATELIMITS_NORMAL_TOTAL_REQUESTS` | `rateLimits.categories.normal.totalRequests` | — | Requests allowed per window. |
| `RATELIMITS_NORMAL_EVERY_MILLISECONDS` | `rateLimits.categories.normal.every` | — | Window length. |
| `RATELIMITS_WHITELIST` / `RATELIMITS_BLACKLIST` | `rateLimits.whitelist` / `.blacklist` | — | Comma-separated IPs with their own `_TOTAL_REQUESTS` / `_EVERY_MILLISECONDS` limits. |
| `DOCUMENTS` | `documents` | `about=./about.md` | Comma-separated `name=path` pairs, seeded into the store and never expired. |
| `LOGGING_LEVEL` | — | `info` | `error`, `warn`, `info`, `verbose`, or `debug`. |

Rate limiting applies to document creation and counts in this process's
memory, so behind several instances the effective limit is per-instance. Put a
shared limiter in your reverse proxy if you need a global one. It's off unless
both a request count and a window are configured.

## Storage

`STORAGE_TYPE` picks the backend. `file` and `memory` work out of the box;
every other backend needs its driver installed, because they aren't
dependencies of this package:

| Type | Install | Notes |
|---|---|---|
| `file` | — | Default. Writes to `STORAGE_FILEPATH`. Cannot expire documents. |
| `memory` | — | Everything is lost on restart. For throwaway instances and tests. |
| `redis` | `pnpm add redis` | Expiry supported and bumped on read. |
| `postgres` | `pnpm add pg` | Creates its `entries` table on first use. |
| `mariadb` | `pnpm add mysql2` | Creates its `entries` table on first use. |
| `mongo` | `pnpm add mongodb` | |
| `memcached` | `pnpm add memcached` | |
| `rethinkdb` | `pnpm add rethinkdbdash` | |
| `amazon-s3` | `pnpm add @aws-sdk/client-s3` | No expiry — use a bucket lifecycle rule. |
| `google-datastore` | `pnpm add @google-cloud/datastore` | |

Deletion needs a backend that supports it. All of the above do; a backend that
didn't would make `DELETE /api/pastes/:id` answer `501`.

## Development

```bash
pnpm run lint && pnpm run typecheck && pnpm run test && pnpm run build
```

```bash
pnpm run test:e2e
```

Unit and route-handler tests are Vitest (`src/**/*.test.ts`); the route tests
call the exported handlers directly against an in-memory store. End-to-end
tests are Playwright (`e2e/`) and drive a real `next dev` server on the file
store.

### Layout

```
src/app/          pages and route handlers (API lives under app/api, plus the
                  legacy /documents and /raw routes)
src/components/   editor and viewer chrome
src/hooks/        usePaste, useShortcuts
src/lib/          config, logger, document envelope, key generators, stores
```

### A note on Node

Node 22.4+ exposes a broken global `localStorage` on the server that makes
every SSR render throw. `scripts/next.js` launches Next with
`--no-experimental-webstorage` to disable it; use the pnpm scripts rather than
calling `next` directly.

## Keyboard shortcuts

| Keys | Action |
|---|---|
| `Ctrl` + `S` | Save |
| `Alt` + `N` | New paste |
| `Ctrl` + `E` | Edit (creates a new paste — the original isn't overwritten) |
| `Alt` + `R` | View raw |
| `Ctrl` + `Shift` + `S` | Share / copy link |

## License

[MIT](LICENSE)
