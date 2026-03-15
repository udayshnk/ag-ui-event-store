# ag-ui-persistence

Async Python library for persisting [AG-UI protocol](https://github.com/ag-ui-protocol/ag-ui) events, runs, and threads to SQLite or PostgreSQL. No HTTP layer — pure storage utility usable with any backend framework.

## Features

- Persists AG-UI threads, runs, and events to SQLite or PostgreSQL
- Fully async (SQLAlchemy + `aiosqlite` / `asyncpg`)
- Linked-list run model: `Thread.latest_run_id` + `Run.previous_run_id` for efficient history traversal without list queries
- Hierarchical runs: top-level runs and child (sub-agent) runs via `parent_run_id`
- Cursor-based pagination for progressive history retrieval
- Tracks run status (`running` / `completed` / `error`), title, and summary
- `persist_on_completion` — hold all events in memory and write them atomically when the run reaches a terminal state
- `merge_delta_events` — collapse consecutive streaming deltas for the same message/tool call into a single row before writing

## Installation

```bash
pip install ag-ui-persistence
```

## Usage

```python
from ag_ui_persistence import AGUIPersistence, PersistenceConfig

store = AGUIPersistence(PersistenceConfig(db_url="sqlite:///agui.db"))
# or: AGUIPersistence(PersistenceConfig(db_url="postgresql://user:pass@localhost/mydb"))
await store.initialize()  # creates tables

# During a live agent run
await store.put_run(thread_id="t1", run_id="r1", parent_run_id=None, title="User request")
await store.put_event(run_id="r1", seq=0, event_type="RUN_STARTED", data={})
await store.put_event(run_id="r1", seq=1, event_type="TEXT_MESSAGE_START", data={"message_id": "m1"})
await store.update_run(run_id="r1", status="completed", summary="Done")

# Reading history — walk backwards via linked list
thread = (await store.get_threads(limit=1))[0]
run = await store.get_run(thread.latest_run_id)   # most recent run
while run:
    events = await store.get_events(run.run_id)
    run = await store.get_run(run.previous_run_id) if run.previous_run_id else None

# Child (sub-agent) runs
await store.put_run(thread_id="t1", run_id="r1-sub", parent_run_id="r1")
child_runs = await store.get_runs(thread_id="t1", parent_run_id="r1")

await store.close()
```

## Configuration

All options are passed via `PersistenceConfig`:

```python
store = AGUIPersistence(PersistenceConfig(
    db_url="sqlite:///agui.db",        # SQLite or PostgreSQL URL
    # engine=my_async_engine,          # or pass an existing AsyncEngine directly
    enable_event_buffering=True,       # buffer events for batched writes (default: True)
    event_batch_size=200,              # flush when this many events are buffered per run
    event_flush_interval=0.02,         # flush after this many seconds of inactivity
    persist_on_completion=True,        # hold all events until run reaches terminal status
    merge_delta_events=True,           # collapse consecutive deltas for the same message/tool call
))
```

### `persist_on_completion`

When `True`, all events for a run are held in memory and written to the database only when `update_run()` is called with a terminal status (`completed` or `error`). `get_events()` returns an empty list while the run is still in progress.

This is useful when you want atomic, all-or-nothing event persistence — e.g. only store a run's events if it completes successfully.

```python
store = AGUIPersistence(PersistenceConfig(db_url="sqlite:///agui.db", persist_on_completion=True))
await store.initialize()

await store.put_run("t1", "r1", parent_run_id=None)
await store.put_event("r1", 0, "RUN_STARTED", {})
await store.put_event("r1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "m1", "delta": "Hello"})

events = await store.get_events("r1")  # [] — not written yet

await store.update_run("r1", status="completed")

events = await store.get_events("r1")  # [RUN_STARTED, TEXT_MESSAGE_CONTENT]
```

### `merge_delta_events`

When `True`, consecutive streaming delta events for the same message or tool call are merged into a single database row before writing. Supported event types:

- `TEXT_MESSAGE_CONTENT` — deltas grouped by `messageId`; `delta` fields concatenated
- `TOOL_CALL_ARGS` — deltas grouped by `toolCallId`; `delta` fields concatenated

The merged row keeps the `seq` of the first delta in the group. `started_at` comes from the first delta and `ended_at` from the last, capturing the full duration of the streaming sequence.

```python
store = AGUIPersistence(PersistenceConfig(db_url="sqlite:///agui.db", merge_delta_events=True))
await store.initialize()

await store.put_run("t1", "r1", parent_run_id=None)
await store.put_event("r1", 0, "TEXT_MESSAGE_CONTENT", {"messageId": "m1", "delta": "Hello"})
await store.put_event("r1", 1, "TEXT_MESSAGE_CONTENT", {"messageId": "m1", "delta": ", "})
await store.put_event("r1", 2, "TEXT_MESSAGE_CONTENT", {"messageId": "m1", "delta": "world"})

events = await store.get_events("r1")
# [Event(seq=0, event_type="TEXT_MESSAGE_CONTENT", data={"messageId": "m1", "delta": "Hello, world"})]
```

## API

### `AGUIPersistence(config: PersistenceConfig)`

All configuration — connection details and behaviour — is passed via a single `PersistenceConfig` dataclass.

Set `db_url` to a SQLAlchemy URL string, or `engine` to pass an existing `AsyncEngine` directly (in which case the engine is never disposed on `close()`). `sqlite://` and `postgresql://` / `postgres://` URL prefixes are automatically converted to their async-native equivalents (`sqlite+aiosqlite://`, `postgresql+asyncpg://`). Exactly one of `db_url` or `engine` must be provided.

When `enable_event_buffering=True` (the default), `put_event()` uses an internal async buffer and `get_events()` flushes buffered events for the requested run before reading, so event reads stay fresh while write throughput improves.

When `enable_event_buffering=False`, `put_event()` writes directly to the database and `get_events()` performs a normal DB read without any internal flush.

`persist_on_completion=True` implies buffering is always active regardless of `enable_event_buffering`.

#### Write

| Method | Description |
|---|---|
| `initialize()` | Create tables (idempotent) |
| `put_run(thread_id, run_id, parent_run_id, title?, status?, namespace?, user_message?)` | Upsert thread + insert run; maintains `latest_run_id` / `previous_run_id` linked list for top-level runs |
| `put_event(run_id, seq, event_type, data)` | Buffer one event for batched persistence (duplicate-safe on flush) |
| `update_run(run_id, status, summary?)` | Update run status and optional summary; triggers event flush for terminal statuses |
| `close()` | Flush pending events and dispose the engine |

#### Read

| Method | Description |
|---|---|
| `get_threads(limit?, before?, namespace?)` | List threads, newest first |
| `get_runs(thread_id, parent_run_id?, limit?, before?)` | List runs for a thread; omit `parent_run_id` for top-level only |
| `get_run(run_id)` | Fetch a single run by ID; returns `None` if not found |
| `get_events(run_id)` | List all events for a run in sequence order |

## Data Model

```
Thread
  thread_id       — primary key
  namespace       — optional partition key (e.g. project ID)
  title           — set on first run
  user_message    — first user message, set on first run
  latest_run_id   — pointer to the most recent top-level run (linked-list head)

Run
  run_id          — primary key
  thread_id       — owning thread
  parent_run_id   — NULL for top-level runs; set for child (sub-agent) runs
  previous_run_id — pointer to the prior top-level run (linked-list prev); NULL for first run
  seq             — zero-based ordinal within the thread
  status          — running | completed | error
  title / summary

Event
  run_id + seq    — composite primary key
  event_type      — AG-UI event type (e.g. TEXT_MESSAGE_START, TOOL_CALL_START)
  data            — JSON payload
  started_at      — millisecond timestamp when the event (or first delta) was received
  ended_at        — millisecond timestamp when the last delta was received (equals started_at for non-delta events)
```

## Running Tests

```bash
uv run --extra test pytest tests/
```
