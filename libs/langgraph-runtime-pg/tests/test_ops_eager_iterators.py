"""Regression: ops iterators must be drainable after connect() closes.

langgraph_api often does::

    async with connect() as conn:
        it = await Assistants.get(conn, id)
    data = await fetchone(it)  # outside connect()

Awaiting DB I/O inside the returned generator re-checkouts a pool connection
that is never checked in (SQLAlchemy GC "non-checked-in connection" warning).
Converting ORM rows only at drain time is also unsafe if the session expires
objects — so we snapshot plain dicts while the session is still open.
"""

from __future__ import annotations

import ast
import gc
import uuid
import warnings
from pathlib import Path

import pytest

OPS_PATH = Path(__file__).resolve().parents[1] / "src" / "langgraph_runtime_pg" / "ops.py"


def _is_async_generator(node: ast.AsyncFunctionDef) -> bool:
    return any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(node))


def _nested_async_gens_with_await(tree: ast.AST) -> list[str]:
    """Nested async *generators* that ``await`` (hard GC-leak class).

    Ignores nested async *functions* (after-commit hooks, heartbeats) which
    may await safely — only returned iterators are drained after connect().
    """
    hits: list[str] = []

    class Visitor(ast.NodeVisitor):
        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            for child in node.body:
                if isinstance(child, ast.AsyncFunctionDef):
                    if _is_async_generator(child) and any(
                        isinstance(n, ast.Await) for n in ast.walk(child)
                    ):
                        hits.append(f"{node.name}.{child.name}")
                    self.visit(child)
                else:
                    self.visit(child)

    Visitor().visit(tree)
    return hits


def test_ops_nested_async_generators_do_not_await() -> None:
    """Static guard: returned iterators must not await (DB) I/O."""
    tree = ast.parse(OPS_PATH.read_text(encoding="utf-8"), filename=str(OPS_PATH))
    hits = _nested_async_gens_with_await(tree)
    # Top-level async generators (Runs.next, Stream.join) may await with their
    # own session scopes; nested closures returned from connect()-scoped
    # methods must not.
    assert hits == [], (
        "Nested async generators must not await — API drains them after "
        f"connect() closes. Offenders: {hits}"
    )


def test_ops_iterator_helpers_do_not_call_orm_to_dict_at_yield() -> None:
    """Static guard: ``_yield`` / ``_iter`` bodies must yield prebuilt dicts.

    Catches ``yield assistant_to_dict(row)`` / ``yield _thread_search_item(r)``
    inside nested iterators (soft DetachedInstance / expire risk).
    """
    tree = ast.parse(OPS_PATH.read_text(encoding="utf-8"), filename=str(OPS_PATH))
    forbidden_callees = {
        "assistant_to_dict",
        "assistant_version_to_dict",
        "thread_to_dict",
        "run_to_dict",
        "cron_to_dict",
        "_thread_search_item",
    }
    offenders: list[str] = []

    class Visitor(ast.NodeVisitor):
        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            for child in ast.walk(node):
                if child is node or not isinstance(child, ast.AsyncFunctionDef):
                    continue
                if not (
                    child.name.startswith("_yield")
                    or child.name.startswith("_iter")
                    or child.name == "_inflight"
                ):
                    continue
                for n in ast.walk(child):
                    if not isinstance(n, ast.Call):
                        continue
                    func = n.func
                    name = None
                    if isinstance(func, ast.Name):
                        name = func.id
                    elif isinstance(func, ast.Attribute):
                        name = func.attr
                    if name in forbidden_callees:
                        offenders.append(f"{node.name}.{child.name}->{name}")
            for child in node.body:
                if not isinstance(child, ast.AsyncFunctionDef):
                    self.visit(child)

    Visitor().visit(tree)
    assert offenders == [], (
        "Iterator helpers must snapshot dicts before return, not convert ORM "
        f"at yield time. Offenders: {offenders}"
    )


async def _drain(it) -> list:
    return [item async for item in it]


async def _seed_assistant_thread_run_cron():
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    aid = uuid.uuid4()
    tid = uuid.uuid4()
    async with connect() as conn:
        assistant = await anext(
            await ops.Assistants.put(
                conn, aid, graph_id="g1", name="eager-test", config={}, metadata={"k": "v"}
            )
        )
        thread = await anext(await ops.Threads.put(conn, tid, metadata={"assistant_id": str(aid)}))
        run = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                thread_id=tid,
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )
        cron = await anext(
            await ops.Crons.put(
                conn,
                payload={"assistant_id": str(aid)},
                schedule="0 9 * * *",
            )
        )
    return assistant, thread, run, cron


@pytest.mark.usefixtures("pg_runtime")
async def test_drain_outside_connect_after_expire_all() -> None:
    """API-shaped drain: expire ORM, close session, then iterate — must succeed."""
    from langgraph_api.utils import get_pagination_headers

    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    assistant, thread, run, cron = await _seed_assistant_thread_run_cron()
    aid = assistant["assistant_id"]
    tid = thread["thread_id"]
    rid = run["run_id"]
    cid = cron["cron_id"]

    # Patch creates a second version for get_versions / set_latest coverage.
    async with connect() as conn:
        patched = await ops.Assistants.patch(conn, aid, name="eager-test-v2")
        conn.session.expire_all()
    assert (await _drain(patched))[0]["name"] == "eager-test-v2"

    async with connect() as conn:
        get_it = await ops.Assistants.get(conn, aid)
        search_it, search_cur = await ops.Assistants.search(conn, limit=10)
        versions_it = await ops.Assistants.get_versions(conn, aid, limit=10)
        latest_it = await ops.Assistants.set_latest(conn, aid, version=1)
        threads_it, threads_cur = await ops.Threads.search(conn, limit=10)
        thread_get = await ops.Threads.get(conn, tid)
        runs_it = await ops.Runs.search(conn, tid, limit=10)
        run_get = await ops.Runs.get(conn, rid, thread_id=tid)
        crons_it, crons_cur = await ops.Crons.search(conn, limit=10)
        cron_get = await ops.Crons.get(conn, cid)
        # Force any leftover ORM identity-map rows to require a live session.
        conn.session.expire_all()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert (await _drain(get_it))[0]["assistant_id"] == aid
        assistants, _ = await get_pagination_headers(search_it, search_cur, 0)
        assert any(a["assistant_id"] == aid for a in assistants)
        versions = await _drain(versions_it)
        assert len(versions) >= 1
        assert (await _drain(latest_it))[0]["version"] == 1
        threads, _ = await get_pagination_headers(threads_it, threads_cur, 0)
        assert any(t["thread_id"] == tid for t in threads)
        assert (await _drain(thread_get))[0]["thread_id"] == tid
        runs = await _drain(runs_it)
        assert any(r["run_id"] == rid for r in runs)
        assert (await _drain(run_get))[0]["run_id"] == rid
        crons, _ = await get_pagination_headers(crons_it, crons_cur, 0)
        assert any(c["cron_id"] == cid for c in crons)
        assert (await _drain(cron_get))[0]["cron_id"] == cid
        gc.collect()

    leak_msgs = [str(w.message) for w in caught if "non-checked-in connection" in str(w.message)]
    assert leak_msgs == [], f"Pool leak warnings after drain-outside-connect: {leak_msgs}"


@pytest.mark.usefixtures("pg_runtime")
async def test_put_do_nothing_and_copy_drain_outside_connect() -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    aid = uuid.uuid4()
    tid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="x", config={}, metadata={})
        )
        await anext(await ops.Threads.put(conn, tid, metadata={}))

    async with connect() as conn:
        a_it = await ops.Assistants.put(
            conn, aid, graph_id="g1", name="x", config={}, metadata={}, if_exists="do_nothing"
        )
        t_it = await ops.Threads.put(conn, tid, metadata={}, if_exists="do_nothing")
        copy_it = await ops.Threads.copy(conn, tid)
        cron_it = await ops.Crons.put(
            conn,
            payload={"assistant_id": str(aid)},
            schedule="0 * * * *",
        )
        conn.session.expire_all()

    assert (await _drain(a_it))[0]["assistant_id"] == aid
    assert (await _drain(t_it))[0]["thread_id"] == tid
    copied = (await _drain(copy_it))[0]
    assert copied["thread_id"] != tid
    cron = (await _drain(cron_it))[0]
    assert cron["assistant_id"] == aid

    async with connect() as conn:
        upd = await ops.Crons.update(conn, cron_id=cron["cron_id"], enabled=False)
        conn.session.expire_all()
    assert (await _drain(upd))[0]["enabled"] is False
