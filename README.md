# ag-ui-persistence

Async Python library for persisting [AG-UI protocol](https://github.com/ag-ui-protocol/ag-ui) events, runs, and threads to SQLite or PostgreSQL. No HTTP layer — pure storage utility usable with any backend framework.

## Features

- Persists AG-UI threads, runs, and events to SQLite or PostgreSQL
- Fully async (SQLAlchemy + `aiosqlite` / `asyncpg`)
- Linked-list run model: `Thread.latest_run_id` + `Run.previous_run_id` for efficient history traversal without list queries
- Hierarchical runs: top-level runs and child (sub-agent) runs via `parent_run_id`
- Cursor-based pagination for progressive history retrieval
- Tracks run status (`running` / `completed` / `error`), title, and summary

## Installation

```bash
pip install ag-ui-persistence
```

## Usage

```python
from ag_ui_event_store import AGUIEventStore

store = AGUIEventStore("sqlite:///agui.db")
# or: AGUIEventStore("postgresql://user:pass@localhost/mydb")
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
```

## API

### `AGUIEventStore(engine)`

Accepts a SQLAlchemy URL string or an existing `AsyncEngine`. `sqlite://` and `postgresql://` / `postgres://` prefixes are automatically converted to their async-native equivalents (`sqlite+aiosqlite://`, `postgresql+asyncpg://`).

#### Write

| Method | Description |
|---|---|
| `initialize()` | Create tables (idempotent) |
| `put_run(thread_id, run_id, parent_run_id, title?, status?, namespace?, user_message?)` | Upsert thread + insert run; maintains `latest_run_id` / `previous_run_id` linked list for top-level runs |
| `put_event(run_id, seq, event_type, data)` | Persist one event (duplicate-safe) |
| `update_run(run_id, status, summary?)` | Update run status and optional summary |

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
```

## Running Tests

```bash
uv run --extra test pytest tests/
```
