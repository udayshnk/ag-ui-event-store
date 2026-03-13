import json
import time
from typing import Optional, Union

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from .models import Thread, Run, Event


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
    title         TEXT,
    user_message  TEXT,
    latest_run_id TEXT,
    created_at    BIGINT NOT NULL,
    updated_at    BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS agui_runs (
    run_id          TEXT PRIMARY KEY,
    thread_id       TEXT NOT NULL,
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
    run_id      TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    data        TEXT NOT NULL,
    created_at  BIGINT NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_agui_threads_ns   ON agui_threads(namespace, updated_at);
CREATE INDEX IF NOT EXISTS idx_agui_runs_thread  ON agui_runs(thread_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agui_runs_parent  ON agui_runs(parent_run_id);
"""


class AGUIEventStore:
    def __init__(self, engine: Union[AsyncEngine, str]):
        if isinstance(engine, str):
            self._engine: AsyncEngine = create_async_engine(_normalize_url(engine))
        else:
            self._engine = engine

    async def initialize(self) -> None:
        """Create tables if they don't exist."""
        async with self._engine.begin() as conn:
            for stmt in _DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    await conn.execute(text(stmt))

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
    ) -> None:
        now = int(time.time() * 1000)
        async with self._engine.begin() as conn:
            # Upsert thread — namespace, title, user_message only written on first insert
            await conn.execute(
                text(
                    "INSERT INTO agui_threads (thread_id, namespace, title, user_message, created_at, updated_at) "
                    "VALUES (:tid, :ns, :title, :user_message, :now, :now) "
                    "ON CONFLICT (thread_id) DO UPDATE SET updated_at = :now"
                ),
                {"tid": thread_id, "ns": namespace, "title": title, "user_message": user_message, "now": now},
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
        now = int(time.time() * 1000)
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO agui_events (run_id, seq, event_type, data, created_at) "
                    "VALUES (:rid, :seq, :etype, :data, :now) "
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

    async def update_run(
        self,
        run_id: str,
        status: str,
        summary: Optional[str] = None,
    ) -> None:
        now = int(time.time() * 1000)
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
        async with self._engine.connect() as conn:
            ns_clause = "AND t.namespace = :ns" if namespace is not None else ""
            ns_params: dict = {"ns": namespace} if namespace is not None else {}

            _select = (
                "SELECT t.thread_id, t.namespace, t.title, t.user_message, "
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
                    title=r[2],
                    user_message=r[3],
                    latest_run_id=r[4],
                    created_at=r[5],
                    updated_at=r[6],
                )
                for r in result
            ]

    async def get_runs(
        self,
        thread_id: str,
        parent_run_id: Optional[str] = None,
        limit: int = 20,
        before: Optional[str] = None,
    ) -> list[Run]:
        async with self._engine.connect() as conn:
            params: dict = {"limit": limit}
            if parent_run_id is not None:
                where = "WHERE thread_id = :tid AND parent_run_id = :prid"
                params["tid"] = thread_id
                params["prid"] = parent_run_id
            else:
                where = "WHERE thread_id = :tid AND parent_run_id IS NULL"
                params["tid"] = thread_id

            if before:
                row = await conn.execute(
                    text("SELECT created_at FROM agui_runs WHERE run_id = :rid"),
                    {"rid": before},
                )
                ts = row.scalar()
                where += " AND created_at < :ts"
                params["ts"] = ts

            result = await conn.execute(
                text(
                    f"SELECT run_id, thread_id, parent_run_id, previous_run_id, seq, status, title, summary, "
                    f"created_at, updated_at FROM agui_runs {where} "
                    f"ORDER BY created_at DESC LIMIT :limit"
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

    async def get_run(self, run_id: str) -> Optional[Run]:
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT run_id, thread_id, parent_run_id, previous_run_id, seq, status, title, summary, "
                    "created_at, updated_at FROM agui_runs WHERE run_id = :rid"
                ),
                {"rid": run_id},
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

    async def get_events(self, run_id: str) -> list[Event]:
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT run_id, seq, event_type, data, created_at FROM agui_events "
                    "WHERE run_id = :rid ORDER BY seq ASC"
                ),
                {"rid": run_id},
            )
            return [
                Event(
                    run_id=r[0],
                    seq=r[1],
                    event_type=r[2],
                    data=json.loads(r[3]),
                    created_at=r[4],
                )
                for r in result
            ]
