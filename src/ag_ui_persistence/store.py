import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import event as sa_event, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from .models import Thread, Run, Event


@dataclass
class PersistenceConfig:
    db_url: Optional[str] = None
    engine: Optional[AsyncEngine] = None
    enable_event_buffering: bool = True
    event_batch_size: int = 200
    event_flush_interval: float = 0.02
    persist_on_completion: bool = True
    merge_delta_events: bool = True


def _normalize_url(db_url: str) -> str:
    """Convert sync driver prefixes to async-native equivalents."""
    if db_url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + db_url[len("postgresql://"):]
    if db_url.startswith("postgres://"):
        return "postgresql+asyncpg://" + db_url[len("postgres://"):]
    if db_url.startswith("sqlite://"):
        return "sqlite+aiosqlite://" + db_url[len("sqlite://"):]
    return db_url


_DDL = """
CREATE TABLE IF NOT EXISTS agui_threads (
    thread_id     TEXT PRIMARY KEY,
    namespace     TEXT,
    agent_id      TEXT,
    title         TEXT,
    user_message  TEXT,
    latest_run_id TEXT,
    created_at    BIGINT NOT NULL,
    updated_at    BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS agui_runs (
    run_id          TEXT PRIMARY KEY,
    thread_id       TEXT NOT NULL REFERENCES agui_threads(thread_id) ON DELETE CASCADE,
    parent_run_id   TEXT,
    previous_run_id TEXT,
    seq             INTEGER NOT NULL,
    status          TEXT NOT NULL,
    title           TEXT,
    summary         TEXT,
    created_at      BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS agui_events (
    run_id      TEXT NOT NULL REFERENCES agui_runs(run_id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    data        TEXT NOT NULL,
    started_at  BIGINT NOT NULL,
    ended_at    BIGINT NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_agui_threads_ns   ON agui_threads(namespace, updated_at);
CREATE INDEX IF NOT EXISTS idx_agui_runs_thread  ON agui_runs(thread_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agui_runs_parent  ON agui_runs(parent_run_id);
"""

_TERMINAL_RUN_STATUSES = {"completed", "error"}


@dataclass(slots=True)
class _BufferedEvent:
    run_id: str
    seq: int
    event_type: str
    data_json: str
    started_at: int
    ended_at: int
    token: int
    enqueued_at: float


class AGUIPersistence:
    def __init__(self, config: PersistenceConfig):
        if config.engine is not None:
            self._engine: AsyncEngine = config.engine
            self._owns_engine = False
        elif config.db_url is not None:
            self._engine = create_async_engine(_normalize_url(config.db_url))
            self._owns_engine = True
        else:
            raise ValueError("PersistenceConfig requires either 'db_url' or 'engine'")
        self._config = config
        # persist_on_completion implies buffering must be active
        self._enable_event_buffering = config.enable_event_buffering or config.persist_on_completion
        self._event_batch_size = config.event_batch_size
        self._event_flush_interval = config.event_flush_interval
        self._persist_on_completion = config.persist_on_completion
        self._merge_delta_events = config.merge_delta_events
        self._pending_events: dict[str, list[_BufferedEvent]] = defaultdict(list)
        self._inflight_event_tokens: dict[str, int] = {}
        self._flush_waiters: dict[str, list[tuple[int, asyncio.Future[None]]]] = defaultdict(list)
        self._next_event_token: dict[str, int] = defaultdict(int)
        self._committed_event_token: dict[str, int] = defaultdict(int)
        self._buffer_write_error: Optional[Exception] = None
        self._flush_requested_runs: set[str] = set()
        self._state_lock = asyncio.Lock()
        self._state_changed = asyncio.Event()
        self._flusher_task: Optional[asyncio.Task[None]] = None
        self._closing = False
        self._closed = False

    async def initialize(self) -> None:
        """Create tables if they don't exist."""
        self._ensure_open()
        # SQLite disables foreign keys by default — enable them on every connection.
        if "sqlite" in str(self._engine.url):
            @sa_event.listens_for(self._engine.sync_engine, "connect")
            def _set_sqlite_pragma(dbapi_conn, _record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        async with self._engine.begin() as conn:
            for stmt in _DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    await conn.execute(text(stmt))

    async def close(self) -> None:
        """Flush pending events and dispose the engine."""
        if self._closed or self._closing:
            return
        if not self._enable_event_buffering:
            self._closed = True
            if self._owns_engine:
                await self._engine.dispose()
            return
        self._closing = True
        failure: Optional[Exception] = self._buffer_write_error
        async with self._state_lock:
            if self._pending_events:
                if self._persist_on_completion:
                    # Discard events for runs that never reached a terminal status
                    for run_id in [r for r in self._pending_events if r not in self._flush_requested_runs]:
                        del self._pending_events[run_id]
                else:
                    self._flush_requested_runs.update(self._pending_events)
                if self._pending_events:
                    self._ensure_flusher_locked()
            self._state_changed.set()
        try:
            await self._flush_all_events()
        except Exception as exc:
            failure = exc
        self._closed = True
        self._state_changed.set()
        if self._flusher_task is not None:
            await self._flusher_task
        if self._owns_engine:
            await self._engine.dispose()
        if failure is not None:
            raise failure

    # ------------------------------------------------------------------
    # Write — called during live streaming
    # ------------------------------------------------------------------

    async def put_run(
        self,
        thread_id: str,
        run_id: str,
        parent_run_id: Optional[str],
        title: Optional[str] = None,
        status: str = "running",
        namespace: Optional[str] = None,
        user_message: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> None:
        self._ensure_open()
        now = int(time.time() * 1000)
        async with self._engine.begin() as conn:
            # Upsert thread — namespace, agent_id, title, user_message only written on first insert
            await conn.execute(
                text(
                    "INSERT INTO agui_threads (thread_id, namespace, agent_id, title, user_message, created_at, updated_at) "
                    "VALUES (:tid, :ns, :agent_id, :title, :user_message, :now, :now) "
                    "ON CONFLICT (thread_id) DO UPDATE SET updated_at = :now"
                ),
                {"tid": thread_id, "ns": namespace, "agent_id": agent_id, "title": title, "user_message": user_message, "now": now},
            )
            # For top-level runs, read current latest_run_id to form the linked list
            previous_run_id: Optional[str] = None
            if parent_run_id is None:
                row = await conn.execute(
                    text("SELECT latest_run_id FROM agui_threads WHERE thread_id = :tid"),
                    {"tid": thread_id},
                )
                previous_run_id = row.scalar()
            # Insert run — seq assigned atomically via subquery to avoid races
            await conn.execute(
                text(
                    "INSERT INTO agui_runs "
                    "(run_id, thread_id, parent_run_id, previous_run_id, seq, status, title, summary, created_at, updated_at) "
                    "VALUES (:rid, :tid, :prid, :prev_rid, "
                    "(SELECT COUNT(*) FROM agui_runs WHERE thread_id = :tid), "
                    ":status, :title, NULL, :now, :now) "
                    "ON CONFLICT (run_id) DO NOTHING"
                ),
                {
                    "rid": run_id,
                    "tid": thread_id,
                    "prid": parent_run_id,
                    "prev_rid": previous_run_id,
                    "status": status,
                    "title": title,
                    "now": now,
                },
            )
            # Update thread's latest_run_id for top-level runs
            if parent_run_id is None:
                await conn.execute(
                    text("UPDATE agui_threads SET latest_run_id = :rid WHERE thread_id = :tid"),
                    {"rid": run_id, "tid": thread_id},
                )

    async def put_event(
        self,
        run_id: str,
        seq: int,
        event_type: str,
        data: dict,
    ) -> None:
        self._ensure_open()
        now = int(time.time() * 1000)
        if not self._enable_event_buffering:
            async with self._engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO agui_events (run_id, seq, event_type, data, started_at, ended_at) "
                        "VALUES (:rid, :seq, :etype, :data, :now, :now) "
                        "ON CONFLICT (run_id, seq) DO NOTHING"
                    ),
                    {
                        "rid": run_id,
                        "seq": seq,
                        "etype": event_type,
                        "data": json.dumps(data),
                        "now": now,
                    },
                )
            return
        event = _BufferedEvent(
            run_id=run_id,
            seq=seq,
            event_type=event_type,
            data_json=json.dumps(data),
            started_at=now,
            ended_at=now,
            token=0,
            enqueued_at=time.monotonic(),
        )
        async with self._state_lock:
            self._ensure_open()
            self._ensure_flusher_locked()
            self._next_event_token[run_id] += 1
            event.token = self._next_event_token[run_id]
            self._pending_events[run_id].append(event)
            if not self._persist_on_completion and len(self._pending_events[run_id]) >= self._event_batch_size:
                self._flush_requested_runs.add(run_id)
            self._state_changed.set()

    async def update_run(
        self,
        run_id: str,
        status: str,
        summary: Optional[str] = None,
    ) -> None:
        self._ensure_open()
        now = int(time.time() * 1000)
        if self._persist_on_completion and status in _TERMINAL_RUN_STATUSES:
            await self._flush_run_atomic(run_id, status, summary, now)
            return
        if self._enable_event_buffering and status in _TERMINAL_RUN_STATUSES:
            await self._flush_run(run_id)
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE agui_runs SET status = :status, summary = :summary, updated_at = :now "
                    "WHERE run_id = :rid"
                ),
                {"rid": run_id, "status": status, "summary": summary, "now": now},
            )
            # Also bump thread updated_at
            await conn.execute(
                text(
                    "UPDATE agui_threads SET updated_at = :now "
                    "WHERE thread_id = (SELECT thread_id FROM agui_runs WHERE run_id = :rid)"
                ),
                {"rid": run_id, "now": now},
            )

    # ------------------------------------------------------------------
    # Read — progressive history retrieval
    # ------------------------------------------------------------------

    async def get_threads(
        self,
        limit: int = 20,
        before: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> list[Thread]:
        self._ensure_open()
        async with self._engine.connect() as conn:
            ns_clause = "AND t.namespace = :ns" if namespace is not None else ""
            ns_params: dict = {"ns": namespace} if namespace is not None else {}

            _select = (
                "SELECT t.thread_id, t.namespace, t.agent_id, t.title, t.user_message, "
                "t.latest_run_id, t.created_at, t.updated_at "
                "FROM agui_threads t "
            )
            if before:
                row = await conn.execute(
                    text("SELECT updated_at FROM agui_threads WHERE thread_id = :tid"),
                    {"tid": before},
                )
                ts = row.scalar()
                result = await conn.execute(
                    text(
                        f"{_select}"
                        f"WHERE (t.updated_at < :ts OR (t.updated_at = :ts AND t.thread_id < :tid)) {ns_clause} "
                        f"ORDER BY t.updated_at DESC, t.thread_id DESC LIMIT :lim"
                    ),
                    {"ts": ts, "tid": before, "lim": limit, **ns_params},
                )
            else:
                result = await conn.execute(
                    text(
                        f"{_select}"
                        f"WHERE 1=1 {ns_clause} "
                        f"ORDER BY t.updated_at DESC, t.thread_id DESC LIMIT :lim"
                    ),
                    {"lim": limit, **ns_params},
                )
            return [
                Thread(
                    thread_id=r[0],
                    namespace=r[1],
                    agent_id=r[2],
                    title=r[3],
                    user_message=r[4],
                    latest_run_id=r[5],
                    created_at=r[6],
                    updated_at=r[7],
                )
                for r in result
            ]

    async def get_runs(
        self,
        thread_id: str,
        parent_run_id: Optional[str] = None,
        limit: int = 20,
        before: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> list[Run]:
        self._ensure_open()
        async with self._engine.connect() as conn:
            params: dict = {"limit": limit}
            join = ""
            namespace_filter = ""
            if namespace is not None:
                join = " JOIN agui_threads t ON t.thread_id = agui_runs.thread_id"
                namespace_filter = " AND t.namespace = :ns"
                params["ns"] = namespace
            if parent_run_id is not None:
                where = "WHERE agui_runs.thread_id = :tid AND agui_runs.parent_run_id = :prid"
                params["tid"] = thread_id
                params["prid"] = parent_run_id
            else:
                where = "WHERE agui_runs.thread_id = :tid AND agui_runs.parent_run_id IS NULL"
                params["tid"] = thread_id

            if before:
                row = await conn.execute(
                    text("SELECT created_at FROM agui_runs WHERE run_id = :rid"),
                    {"rid": before},
                )
                ts = row.scalar()
                where += " AND agui_runs.created_at < :ts"
                params["ts"] = ts

            result = await conn.execute(
                text(
                    "SELECT agui_runs.run_id, agui_runs.thread_id, agui_runs.parent_run_id, "
                    "agui_runs.previous_run_id, agui_runs.seq, agui_runs.status, agui_runs.title, "
                    "agui_runs.summary, agui_runs.created_at, agui_runs.updated_at "
                    f"FROM agui_runs{join} {where}{namespace_filter} "
                    "ORDER BY agui_runs.created_at DESC LIMIT :limit"
                ),
                params,
            )
            return [
                Run(
                    run_id=r[0],
                    thread_id=r[1],
                    parent_run_id=r[2],
                    previous_run_id=r[3],
                    seq=r[4],
                    status=r[5],
                    title=r[6],
                    summary=r[7],
                    created_at=r[8],
                    updated_at=r[9],
                )
                for r in result
            ]

    async def get_run(self, run_id: str, namespace: Optional[str] = None) -> Optional[Run]:
        self._ensure_open()
        async with self._engine.connect() as conn:
            join = ""
            namespace_filter = ""
            params = {"rid": run_id}
            if namespace is not None:
                join = " JOIN agui_threads t ON t.thread_id = agui_runs.thread_id"
                namespace_filter = " AND t.namespace = :ns"
                params["ns"] = namespace
            result = await conn.execute(
                text(
                    "SELECT agui_runs.run_id, agui_runs.thread_id, agui_runs.parent_run_id, "
                    "agui_runs.previous_run_id, agui_runs.seq, agui_runs.status, agui_runs.title, "
                    "agui_runs.summary, agui_runs.created_at, agui_runs.updated_at "
                    f"FROM agui_runs{join} WHERE agui_runs.run_id = :rid{namespace_filter}"
                ),
                params,
            )
            r = result.fetchone()
            if r is None:
                return None
            return Run(
                run_id=r[0],
                thread_id=r[1],
                parent_run_id=r[2],
                previous_run_id=r[3],
                seq=r[4],
                status=r[5],
                title=r[6],
                summary=r[7],
                created_at=r[8],
                updated_at=r[9],
            )

    async def delete_thread(self, thread_id: str, namespace: Optional[str] = None) -> bool:
        """Delete a thread and all its runs and events. Returns True if found and deleted."""
        self._ensure_open()
        if self._enable_event_buffering:
            run_ids = await self._get_thread_run_ids(thread_id, namespace)
            if run_ids:
                await self._flush_runs(run_ids)
        async with self._engine.begin() as conn:
            query = "DELETE FROM agui_threads WHERE thread_id = :tid"
            params = {"tid": thread_id}
            if namespace is not None:
                query += " AND namespace = :ns"
                params["ns"] = namespace
            result = await conn.execute(
                text(query),
                params,
            )
        return result.rowcount > 0

    async def get_events(self, run_id: str, namespace: Optional[str] = None) -> list[Event]:
        self._ensure_open()
        if namespace is not None:
            if not await self._run_matches_namespace(run_id, namespace):
                return []
        # With persist_on_completion, events are only written on terminal status — don't flush here
        if self._enable_event_buffering and not self._persist_on_completion:
            await self._flush_run(run_id)
        async with self._engine.connect() as conn:
            join = ""
            namespace_filter = ""
            params = {"rid": run_id}
            if namespace is not None:
                join = (
                    " JOIN agui_runs r ON r.run_id = agui_events.run_id"
                    " JOIN agui_threads t ON t.thread_id = r.thread_id"
                )
                namespace_filter = " AND t.namespace = :ns"
                params["ns"] = namespace
            result = await conn.execute(
                text(
                    "SELECT agui_events.run_id, agui_events.seq, agui_events.event_type, "
                    "agui_events.data, agui_events.started_at, agui_events.ended_at "
                    f"FROM agui_events{join} WHERE agui_events.run_id = :rid{namespace_filter} "
                    "ORDER BY agui_events.seq ASC"
                ),
                params,
            )
            return [
                Event(
                    run_id=r[0],
                    seq=r[1],
                    event_type=r[2],
                    data=json.loads(r[3]),
                    started_at=r[4],
                    ended_at=r[5],
                )
                for r in result
            ]

    async def _run_matches_namespace(self, run_id: str, namespace: str) -> bool:
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT 1 "
                    "FROM agui_runs r "
                    "JOIN agui_threads t ON t.thread_id = r.thread_id "
                    "WHERE r.run_id = :rid AND t.namespace = :ns"
                ),
                {"rid": run_id, "ns": namespace},
            )
            return result.fetchone() is not None

    def _ensure_flusher_locked(self) -> None:
        if self._flusher_task is None or self._flusher_task.done():
            self._flusher_task = asyncio.create_task(self._event_flusher_loop())

    def _ensure_open(self) -> None:
        if self._closed or self._closing:
            raise RuntimeError("AGUIPersistence is closed")
        if self._buffer_write_error is not None:
            raise RuntimeError("Buffered event write failed") from self._buffer_write_error

    async def _flush_run_atomic(
        self, run_id: str, status: str, summary: Optional[str], now: int
    ) -> None:
        """Write all buffered events for run_id and the terminal status update in one transaction.

        Called by update_run() when persist_on_completion=True so that events and the
        run status are always committed together or not at all.
        """
        async with self._state_lock:
            events = self._pending_events.pop(run_id, [])
            self._flush_requested_runs.discard(run_id)
            self._state_changed.set()

        if self._merge_delta_events:
            events = self._do_merge_delta_events(events)

        rows = [
            {
                "rid": e.run_id, "seq": e.seq, "etype": e.event_type,
                "data": e.data_json, "started_at": e.started_at, "ended_at": e.ended_at,
            }
            for e in events
        ]

        async with self._engine.begin() as conn:
            if rows:
                await conn.execute(
                    text(
                        "INSERT INTO agui_events (run_id, seq, event_type, data, started_at, ended_at) "
                        "VALUES (:rid, :seq, :etype, :data, :started_at, :ended_at) "
                        "ON CONFLICT (run_id, seq) DO NOTHING"
                    ),
                    rows,
                )
            await conn.execute(
                text(
                    "UPDATE agui_runs SET status = :status, summary = :summary, updated_at = :now "
                    "WHERE run_id = :rid"
                ),
                {"rid": run_id, "status": status, "summary": summary, "now": now},
            )
            await conn.execute(
                text(
                    "UPDATE agui_threads SET updated_at = :now "
                    "WHERE thread_id = (SELECT thread_id FROM agui_runs WHERE run_id = :rid)"
                ),
                {"rid": run_id, "now": now},
            )

        if events:
            async with self._state_lock:
                self._committed_event_token[run_id] = max(
                    self._committed_event_token.get(run_id, 0), events[-1].token
                )
                waiters = self._flush_waiters.pop(run_id, [])
            for _, future in waiters:
                if not future.done():
                    future.set_result(None)

    async def _flush_run(self, run_id: str) -> None:
        self._ensure_open()
        waiter: Optional[asyncio.Future[None]] = None
        async with self._state_lock:
            target_token = self._next_event_token.get(run_id, 0)
            if self._committed_event_token.get(run_id, 0) >= target_token:
                return
            self._ensure_flusher_locked()
            waiter = asyncio.get_running_loop().create_future()
            self._flush_waiters[run_id].append((target_token, waiter))
            self._flush_requested_runs.add(run_id)
            self._state_changed.set()
        await waiter

    async def _flush_all_events(self) -> None:
        if self._closed:
            return
        async with self._state_lock:
            run_ids = set(self._pending_events) | set(self._inflight_event_tokens)
        await self._flush_runs(run_ids)

    async def _flush_runs(self, run_ids: set[str]) -> None:
        if self._closed or not run_ids:
            return
        async with self._state_lock:
            targets: dict[str, int] = {}
            for run_id in run_ids:
                target_token = max(
                    self._next_event_token.get(run_id, 0),
                    self._inflight_event_tokens.get(run_id, 0),
                )
                if self._committed_event_token.get(run_id, 0) < target_token:
                    targets[run_id] = target_token
            if not targets:
                return
            self._ensure_flusher_locked()
            waiters: list[asyncio.Future[None]] = []
            loop = asyncio.get_running_loop()
            for run_id, target_token in targets.items():
                waiter = loop.create_future()
                self._flush_waiters[run_id].append((target_token, waiter))
                self._flush_requested_runs.add(run_id)
                waiters.append(waiter)
            self._state_changed.set()
        await asyncio.gather(*waiters)

    async def _get_thread_run_ids(self, thread_id: str, namespace: Optional[str]) -> set[str]:
        query = "SELECT r.run_id FROM agui_runs r"
        params: dict[str, str] = {"tid": thread_id}
        where = " WHERE r.thread_id = :tid"
        if namespace is not None:
            query += " JOIN agui_threads t ON t.thread_id = r.thread_id"
            where += " AND t.namespace = :ns"
            params["ns"] = namespace
        async with self._engine.connect() as conn:
            result = await conn.execute(text(query + where), params)
            return {row[0] for row in result}

    async def _event_flusher_loop(self) -> None:
        stop_error: Optional[BaseException] = None
        try:
            while True:
                batch = await self._next_flush_batch()
                if batch is None:
                    return
                await self._flush_batch(batch)
        except BaseException as exc:
            stop_error = exc
            raise
        finally:
            async with self._state_lock:
                is_active_flusher = self._flusher_task is asyncio.current_task()
                if is_active_flusher:
                    self._flusher_task = None
                if is_active_flusher and stop_error is not None:
                    for waiters in self._flush_waiters.values():
                        for _, future in waiters:
                            if not future.done():
                                future.set_exception(RuntimeError("Event flusher stopped"))
                    self._flush_waiters.clear()

    async def _next_flush_batch(self) -> Optional[dict[str, list[_BufferedEvent]]]:
        while True:
            async with self._state_lock:
                if not self._pending_events:
                    self._state_changed.clear()
                    if self._flusher_task is asyncio.current_task():
                        self._flusher_task = None
                    return None
                ready_runs = self._get_ready_runs_locked()
                if ready_runs:
                    batch = {run_id: self._pending_events.pop(run_id) for run_id in ready_runs}
                    for run_id in ready_runs:
                        self._inflight_event_tokens[run_id] = batch[run_id][-1].token
                    self._flush_requested_runs.difference_update(ready_runs)
                    if not self._pending_events:
                        self._state_changed.clear()
                    return batch
                self._state_changed.clear()
            try:
                await asyncio.wait_for(self._state_changed.wait(), timeout=self._event_flush_interval)
            except asyncio.TimeoutError:
                continue

    def _get_ready_runs_locked(self) -> list[str]:
        if not self._pending_events:
            return []
        if self._closing:
            if self._persist_on_completion:
                return [r for r in self._pending_events if r in self._flush_requested_runs]
            return list(self._pending_events)
        if self._persist_on_completion:
            return [r for r in self._pending_events if r in self._flush_requested_runs]
        now = time.monotonic()
        ready_runs: list[str] = []
        for run_id, events in self._pending_events.items():
            if run_id in self._flush_requested_runs:
                ready_runs.append(run_id)
                continue
            if len(events) >= self._event_batch_size:
                ready_runs.append(run_id)
                continue
            if events and (now - events[0].enqueued_at) >= self._event_flush_interval:
                ready_runs.append(run_id)
        return ready_runs

    @staticmethod
    def _delta_key(event: _BufferedEvent) -> Optional[tuple[str, str]]:
        """Return (event_type, id) for delta events, or None for all other event types."""
        if event.event_type == "TEXT_MESSAGE_CONTENT":
            return ("TEXT_MESSAGE_CONTENT", json.loads(event.data_json).get("messageId", ""))
        if event.event_type == "TOOL_CALL_ARGS":
            return ("TOOL_CALL_ARGS", json.loads(event.data_json).get("toolCallId", ""))
        return None

    def _do_merge_delta_events(self, events: list[_BufferedEvent]) -> list[_BufferedEvent]:
        """Merge *adjacent* delta events that share the same (event_type, id) key."""
        result: list[_BufferedEvent] = []
        carry: Optional[_BufferedEvent] = None
        carry_key: Optional[tuple[str, str]] = None

        for event in events:
            key = self._delta_key(event)
            if carry is not None and key == carry_key:
                # Extend carry in-place: append delta text and stretch ended_at.
                carry_data = json.loads(carry.data_json)
                carry_data["delta"] = carry_data.get("delta", "") + json.loads(event.data_json).get("delta", "")
                carry.data_json = json.dumps(carry_data)
                carry.ended_at = event.ended_at
            else:
                if carry is not None:
                    result.append(carry)
                if key is not None:
                    # Start a new carry (copy so we don't mutate the original batch event)
                    carry = _BufferedEvent(
                        run_id=event.run_id, seq=event.seq, event_type=event.event_type,
                        data_json=event.data_json, started_at=event.started_at,
                        ended_at=event.ended_at, token=event.token, enqueued_at=event.enqueued_at,
                    )
                    carry_key = key
                else:
                    carry = None
                    carry_key = None
                    result.append(event)

        if carry is not None:
            result.append(carry)
        return result

    async def _extend_db_carry(self, conn, run_id: str, events: list[_BufferedEvent]) -> list[_BufferedEvent]:
        """
        Extend the last committed delta row for this run with leading adjacent deltas from
        the in-memory batch, handling cross-flush-boundary merging.

        Runs inside the caller's open transaction so the UPDATE and INSERT are atomic.
        Returns the slice of events that still need to be INSERTed.
        """
        if not events:
            return events
        first_key = self._delta_key(events[0])
        if first_key is None:
            return events

        row = (await conn.execute(
            text(
                "SELECT seq, event_type, data, ended_at "
                "FROM agui_events WHERE run_id = :rid ORDER BY seq DESC LIMIT 1"
            ),
            {"rid": run_id},
        )).fetchone()
        if row is None:
            return events

        db_seq, db_event_type, db_data_json, db_ended_at = row
        first_etype, first_id = first_key
        if db_event_type != first_etype:
            return events
        db_data = json.loads(db_data_json)
        id_field = "messageId" if first_etype == "TEXT_MESSAGE_CONTENT" else "toolCallId"
        if db_data.get(id_field, "") != first_id:
            return events

        # Consume all leading adjacent deltas that match first_key
        merged_delta = db_data.get("delta", "")
        merged_ended_at = db_ended_at
        consumed = 0
        for event in events:
            if self._delta_key(event) != first_key:
                break
            merged_delta += json.loads(event.data_json).get("delta", "")
            merged_ended_at = event.ended_at
            consumed += 1

        db_data["delta"] = merged_delta
        await conn.execute(
            text(
                "UPDATE agui_events SET data = :data, ended_at = :ended_at "
                "WHERE run_id = :rid AND seq = :seq"
            ),
            {"data": json.dumps(db_data), "ended_at": merged_ended_at, "rid": run_id, "seq": db_seq},
        )
        return events[consumed:]

    async def _flush_batch(self, batch: dict[str, list[_BufferedEvent]]) -> None:
        try:
            async with self._engine.begin() as conn:
                rows: list[dict] = []
                if self._merge_delta_events:
                    for run_id, events in batch.items():
                        events = self._do_merge_delta_events(events)
                        events = await self._extend_db_carry(conn, run_id, events)
                        rows.extend(
                            {"rid": e.run_id, "seq": e.seq, "etype": e.event_type,
                             "data": e.data_json, "started_at": e.started_at, "ended_at": e.ended_at}
                            for e in events
                        )
                else:
                    rows = [
                        {"rid": e.run_id, "seq": e.seq, "etype": e.event_type,
                         "data": e.data_json, "started_at": e.started_at, "ended_at": e.ended_at}
                        for events in batch.values()
                        for e in events
                    ]
                if rows:
                    await conn.execute(
                        text(
                            "INSERT INTO agui_events (run_id, seq, event_type, data, started_at, ended_at) "
                            "VALUES (:rid, :seq, :etype, :data, :started_at, :ended_at) "
                            "ON CONFLICT (run_id, seq) DO NOTHING"
                        ),
                        rows,
                    )
        except Exception as exc:
            if len(batch) > 1:
                first_error: Optional[Exception] = None
                for run_id, events in batch.items():
                    try:
                        await self._flush_batch({run_id: events})
                    except Exception as run_exc:
                        if first_error is None:
                            first_error = run_exc
                if first_error is not None:
                    raise first_error
                return
            async with self._state_lock:
                if self._buffer_write_error is None:
                    self._buffer_write_error = exc
                for run_id, events in batch.items():
                    self._inflight_event_tokens.pop(run_id, None)
                    waiters = self._flush_waiters.get(run_id, [])
                    remaining_waiters: list[tuple[int, asyncio.Future[None]]] = []
                    max_failed_token = events[-1].token
                    for target_token, future in waiters:
                        if target_token <= max_failed_token and not future.done():
                            future.set_exception(exc)
                        else:
                            remaining_waiters.append((target_token, future))
                    if remaining_waiters:
                        self._flush_waiters[run_id] = remaining_waiters
                    else:
                        self._flush_waiters.pop(run_id, None)
                self._state_changed.set()
            raise

        async with self._state_lock:
            for run_id, events in batch.items():
                self._inflight_event_tokens.pop(run_id, None)
                self._committed_event_token[run_id] = max(
                    self._committed_event_token.get(run_id, 0),
                    events[-1].token,
                )
                waiters = self._flush_waiters.get(run_id, [])
                remaining_waiters: list[tuple[int, asyncio.Future[None]]] = []
                for target_token, future in waiters:
                    if target_token <= self._committed_event_token[run_id]:
                        if not future.done():
                            future.set_result(None)
                    else:
                        remaining_waiters.append((target_token, future))
                if remaining_waiters:
                    self._flush_waiters[run_id] = remaining_waiters
                else:
                    self._flush_waiters.pop(run_id, None)
