# Carmine

A typed key-value store with an HTTP API, backed by [redb](https://github.com/cberner/redb).

Data is organized into **cabinets** (databases) and **shelves** (typed tables). Each shelf enforces a fixed key type and value type, so type mismatches are caught at write time rather than read time.

## Key concepts

- **Cabinet** — an isolated redb database file. Create as many as you need.
- **Shelf** — a typed table inside a cabinet. Each shelf has a fixed key type (`String`, `Int`, `Number`) and value type (`String`, `Int`, `Number`, `Object`, `Byte`).
- **System store** — internal metadata database that tracks all cabinets and their shelves.

## Getting started

```sh
cargo build --release
./target/release/carmine
```

By default, Carmine listens on `0.0.0.0:3000` and stores data in `./data`.

## Configuration

Carmine loads configuration from CLI flags, environment variables, and/or a TOML config file (searched as `carmine.toml`, `config.toml`, `.carmine.toml` in the working directory).

| Setting | CLI flag | Env var | TOML path | Default |
|---|---|---|---|---|
| Data directory | `--data-dir` | `CARMINE_DATA_DIR` | `storage.data_dir` | `./data` |
| Bind address | `--bind` | `CARMINE_BIND` | `server.bind` | `0.0.0.0:3000` |
| Cabinet cache size | `--cabinet-cache` | `CARMINE_CABINET_CACHE_SIZE` | `cache.cabinet_size` | 64 MB |
| System cache size | `--system-cache` | `CARMINE_SYSTEM_CACHE_SIZE` | `cache.system_size` | 8 MB |
| Durability | `--durability` | `CARMINE_DURABILITY` | `storage.durability` | `immediate` |
| Log level | `--log-level` | `CARMINE_LOG_LEVEL` | `logging.level` | `info` |

Example `carmine.toml`:

```toml
[server]
bind = "127.0.0.1:8080"

[storage]
data_dir = "/var/lib/carmine"
durability = "immediate"  # or "none" for faster writes without fsync

[cache]
cabinet_size = 67108864
system_size = 8388608

[logging]
level = "info"
```

## API

### System endpoints

#### Create a cabinet

```
POST /system/cabinets
```

```json
{ "name": "my_cabinet" }
```

```json
{
  "id": 1234567890,
  "name": "my_cabinet",
  "path": "./data/cabinet_1234567890",
  "shelves": []
}
```

#### List cabinets

```
GET /system/cabinets
```

```json
[
  {
    "id": 1234567890,
    "name": "my_cabinet",
    "path": "./data/cabinet_1234567890",
    "shelves": [
      { "name": "users", "key_type": "String", "value_type": "Object" }
    ]
  }
]
```

#### Get cabinet

```
GET /system/cabinets/:name
```

#### Delete cabinet

```
DELETE /system/cabinets/:name
```

#### Clean cabinet (clear all shelf data)

```
POST /system/cabinets/:name/clean
```

#### Create a shelf

```
POST /system/cabinets/:name/shelves
```

```json
{
  "name": "users",
  "key_type": "String",
  "value_type": "Object"
}
```

```json
{ "name": "users", "key_type": "String", "value_type": "Object" }
```

Valid key types: `String`, `Int`, `Number`
Valid value types: `String`, `Int`, `Number`, `Object`, `Byte`

#### List shelves

```
GET /system/cabinets/:name/shelves
```

#### Delete shelf

```
DELETE /system/cabinets/:name/shelves/:shelf
```

### Data endpoints

All data operations go through `/v1/:cabinet/:shelf/`.

#### Set (insert or update)

```
POST /v1/my_cabinet/users/set
```

```json
{ "key": "alice", "value": {"role": "admin", "active": true} }
```

Response: `204 No Content`

#### Put (insert only, fails if key exists)

```
POST /v1/my_cabinet/users/put
```

```json
{ "key": "alice", "value": {"role": "admin", "active": true} }
```

Response: `204 No Content`

#### Get

```
POST /v1/my_cabinet/users/get
```

```json
{ "key": "alice" }
```

```json
{ "value": {"role": "admin", "active": true} }
```

Returns `null` for the value if the key does not exist.

#### Delete

```
POST /v1/my_cabinet/users/delete
```

```json
{ "key": "alice" }
```

Response: `204 No Content`

#### Check existence

```
POST /v1/my_cabinet/users/exists
```

```json
{ "key": "alice" }
```

```json
{ "exists": true }
```

#### Get all entries

```
GET /v1/my_cabinet/users/all
```

```json
{ "entries": [["alice", {"role": "admin"}], ["bob", {"role": "user"}]] }
```

#### Get all keys

```
GET /v1/my_cabinet/users/keys
```

```json
{ "keys": ["alice", "bob"] }
```

#### Get all values

```
GET /v1/my_cabinet/users/values
```

```json
{ "values": [{"role": "admin"}, {"role": "user"}] }
```

#### Count entries

```
GET /v1/my_cabinet/users/count
```

```json
{ "count": 2 }
```

#### Range query

```
POST /v1/my_cabinet/users/range
```

```json
{ "start": "a", "end": "c" }
```

```json
{ "entries": [["alice", {"role": "admin"}], ["bob", {"role": "user"}]] }
```

#### Batch set

```
POST /v1/my_cabinet/users/batch/set
```

```json
{
  "entries": [
    ["alice", {"role": "admin"}],
    ["bob", {"role": "user"}]
  ]
}
```

Response: `204 No Content`

#### Batch put

```
POST /v1/my_cabinet/users/batch/put
```

Same format as batch set.

#### Batch get

```
POST /v1/my_cabinet/users/batch/get
```

```json
{ "keys": ["alice", "bob", "unknown"] }
```

```json
{ "values": [{"role": "admin"}, {"role": "user"}, null] }
```

#### Batch delete

```
POST /v1/my_cabinet/users/batch/delete
```

```json
{ "keys": ["alice", "bob"] }
```

Response: `204 No Content`

### Health check

```
GET /health
```

Returns `OK`.

## Full example

```sh
# Create a cabinet
curl -X POST http://localhost:3000/system/cabinets \
  -H 'Content-Type: application/json' \
  -d '{"name": "myapp"}'

# Create a shelf with string keys and object values
curl -X POST http://localhost:3000/system/cabinets/myapp/shelves \
  -H 'Content-Type: application/json' \
  -d '{"name": "users", "key_type": "String", "value_type": "Object"}'

# Insert a record
curl -X POST http://localhost:3000/v1/myapp/users/set \
  -H 'Content-Type: application/json' \
  -d '{"key": "alice", "value": {"email": "alice@example.com", "plan": "pro"}}'

# Read it back
curl -X POST http://localhost:3000/v1/myapp/users/get \
  -H 'Content-Type: application/json' \
  -d '{"key": "alice"}'
# => {"value":{"email":"alice@example.com","plan":"pro"}}

# Batch insert
curl -X POST http://localhost:3000/v1/myapp/users/batch/set \
  -H 'Content-Type: application/json' \
  -d '{"entries": [["bob", {"email": "bob@example.com"}], ["carol", {"email": "carol@example.com"}]]}'

# List all keys
curl http://localhost:3000/v1/myapp/users/keys
# => {"keys":["alice","bob","carol"]}

# Count
curl http://localhost:3000/v1/myapp/users/count
# => {"count":3}
```
