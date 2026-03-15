"""Tests for AGUIPersistence using in-memory SQLite."""
import pytest
import pytest_asyncio
from ag_ui_persistence import AGUIPersistence, Thread, Run, Event


@pytest_asyncio.fixture
async def store():
    s = AGUIPersistence("sqlite:///:memory:")
    await s.initialize()
    return s


@pytest.mark.asyncio
async def test_put_and_get_thread(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, title="Hello")
    threads = await store.get_threads()
    assert len(threads) == 1
    assert threads[0].thread_id == "thread-1"
    assert threads[0].namespace is None


@pytest.mark.asyncio
async def test_namespace_stored_on_thread(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-abc")
    threads = await store.get_threads()
    assert threads[0].namespace == "project-abc"


@pytest.mark.asyncio
async def test_namespace_filter(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-abc")
    await store.put_run("thread-2", "run-2", parent_run_id=None, namespace="project-xyz")
    await store.put_run("thread-3", "run-3", parent_run_id=None, namespace="project-abc")

    abc = await store.get_threads(namespace="project-abc")
    assert {t.thread_id for t in abc} == {"thread-1", "thread-3"}

    xyz = await store.get_threads(namespace="project-xyz")
    assert {t.thread_id for t in xyz} == {"thread-2"}


@pytest.mark.asyncio
async def test_namespace_not_overwritten_on_subsequent_runs(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="original")
    await store.put_run("thread-1", "run-2", parent_run_id=None, namespace="other")
    threads = await store.get_threads()
    assert threads[0].namespace == "original"


@pytest.mark.asyncio
async def test_no_namespace_filter_returns_all(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="ns-a")
    await store.put_run("thread-2", "run-2", parent_run_id=None, namespace="ns-b")
    all_threads = await store.get_threads()
    assert len(all_threads) == 2


@pytest.mark.asyncio
async def test_put_run_creates_thread_once(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await store.put_run("thread-1", "run-2", parent_run_id="run-1")
    threads = await store.get_threads()
    assert len(threads) == 1  # thread created only once


@pytest.mark.asyncio
async def test_get_runs_top_level(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, title="Master")
    await store.put_run("thread-1", "run-2", parent_run_id="run-1", title="Sub")
    runs = await store.get_runs("thread-1")
    assert len(runs) == 1
    assert runs[0].run_id == "run-1"


@pytest.mark.asyncio
async def test_get_runs_by_parent(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await store.put_run("thread-1", "run-2", parent_run_id="run-1")
    await store.put_run("thread-1", "run-3", parent_run_id="run-1")
    sub_runs = await store.get_runs("thread-1", parent_run_id="run-1")
    assert len(sub_runs) == 2
    assert {r.run_id for r in sub_runs} == {"run-2", "run-3"}


@pytest.mark.asyncio
async def test_get_runs_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a")
    await store.put_run("thread-2", "run-2", parent_run_id=None, namespace="project-b")

    runs = await store.get_runs("thread-1", namespace="project-a")
    assert [run.run_id for run in runs] == ["run-1"]

    hidden = await store.get_runs("thread-1", namespace="project-b")
    assert hidden == []


@pytest.mark.asyncio
async def test_update_run_status(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await store.update_run("run-1", status="completed", summary="Done")
    runs = await store.get_runs("thread-1")
    assert runs[0].status == "completed"
    assert runs[0].summary == "Done"


@pytest.mark.asyncio
async def test_get_run_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a")

    run = await store.get_run("run-1", namespace="project-a")
    assert run is not None
    assert run.run_id == "run-1"

    hidden = await store.get_run("run-1", namespace="project-b")
    assert hidden is None


@pytest.mark.asyncio
async def test_put_and_get_events(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await store.put_event("run-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await store.put_event("run-1", 1, "TEXT_MESSAGE_START", {"messageId": "msg-1"})
    await store.put_event("run-1", 2, "TEXT_MESSAGE_END", {"messageId": "msg-1"})

    events = await store.get_events("run-1")
    assert len(events) == 3
    assert events[0].event_type == "RUN_STARTED"
    assert events[1].seq == 1
    assert events[2].data == {"messageId": "msg-1"}


@pytest.mark.asyncio
async def test_duplicate_event_ignored(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await store.put_event("run-1", 0, "RUN_STARTED", {"threadId": "thread-1"})
    await store.put_event("run-1", 0, "RUN_STARTED", {"threadId": "thread-1"})  # duplicate
    events = await store.get_events("run-1")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_get_events_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a")
    await store.put_event("run-1", 0, "RUN_STARTED", {"threadId": "thread-1"})

    events = await store.get_events("run-1", namespace="project-a")
    assert len(events) == 1

    hidden = await store.get_events("run-1", namespace="project-b")
    assert hidden == []


@pytest.mark.asyncio
async def test_get_threads_before_cursor(store):
    import asyncio
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await asyncio.sleep(0.01)
    await store.put_run("thread-2", "run-2", parent_run_id=None)
    threads = await store.get_threads(limit=10)
    assert threads[0].thread_id == "thread-2"  # most recent first

    older = await store.get_threads(limit=10, before="thread-2")
    assert len(older) == 1
    assert older[0].thread_id == "thread-1"


@pytest.mark.asyncio
async def test_run_seq_increments(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None)
    await store.put_run("thread-1", "run-2", parent_run_id=None)
    runs = await store.get_runs("thread-1")
    seqs = {r.run_id: r.seq for r in runs}
    assert seqs["run-1"] == 0
    assert seqs["run-2"] == 1


@pytest.mark.asyncio
async def test_title_and_user_message_stored(store):
    msg = "Hello, this is my first user message"
    await store.put_run("thread-1", "run-1", parent_run_id=None, title=msg[:100], user_message=msg)
    threads = await store.get_threads()
    assert threads[0].user_message == msg
    assert threads[0].title == msg[:100]


@pytest.mark.asyncio
async def test_user_message_not_overwritten(store):
    first = "First user message"
    second = "Second user message"
    await store.put_run("thread-1", "run-1", parent_run_id=None, title=first[:100], user_message=first)
    await store.put_run("thread-1", "run-2", parent_run_id=None, title=second[:100], user_message=second)
    threads = await store.get_threads()
    assert threads[0].user_message == first
    assert threads[0].title == first[:100]


@pytest.mark.asyncio
async def test_delete_thread_respects_namespace(store):
    await store.put_run("thread-1", "run-1", parent_run_id=None, namespace="project-a")

    deleted = await store.delete_thread("thread-1", namespace="project-b")
    assert deleted is False
    assert len(await store.get_threads()) == 1

    deleted = await store.delete_thread("thread-1", namespace="project-a")
    assert deleted is True
    assert await store.get_threads() == []

