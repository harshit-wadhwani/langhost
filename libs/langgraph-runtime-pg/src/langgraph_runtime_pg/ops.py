"""SQL-native Postgres ops for assistants, threads, runs, and crons."""

from __future__ import annotations

import asyncio
import copy
import os
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import orjson
import structlog
from langgraph.types import StateSnapshot
from sqlalchemy import delete, func, select as sa_select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from starlette.exceptions import HTTPException

from langgraph_runtime_pg.checkpoint import Checkpointer
from langgraph_runtime_pg.database import (
    PgConnectionProto,
    assistant_to_dict,
    assistant_version_to_dict,
    connect,
    cron_to_dict,
    get_session_factory,
    run_to_dict,
    thread_to_dict,
)
from langgraph_runtime_pg.models import (
    AssistantRow,
    AssistantVersionRow,
    CronRow,
    RetryCounterRow,
    RunRow,
    ThreadRow,
)
from langgraph_runtime_pg.redis_stream import (
    ContextQueue,
    Message,
    clear_run_heartbeat,
    get_stream_manager,
    has_run_heartbeat,
    heartbeat_refresh_interval_secs,
    ms_seq_id_gt,
    ms_seq_id_sort_key,
    set_run_heartbeat,
    wake_run_queue,
)

logger = structlog.stdlib.get_logger(__name__)

StreamHandler = ContextQueue


async def _empty_aiter():
    if False:
        yield


def _escape_like(value: str) -> str:
    """Escape ``%``, ``_``, and ``\\`` for SQL LIKE / ILIKE with escape='\\\\'."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _name_ilike(name: str):
    return AssistantRow.name.ilike(f"%{_escape_like(name)}%", escape="\\")


def _patch_interrupt(interrupt: Any) -> dict:
    """JSONB-safe interrupt dict."""
    from langgraph_api.state import patch_interrupt

    return cast(dict, patch_interrupt(interrupt))


def _thread_status_from_checkpoint(
    checkpoint: dict | None,
    exception: BaseException | None,
    *,
    ignore_user_control: bool = False,
) -> tuple[str, dict]:
    """Return ``(base_thread_status, interrupts)`` from a checkpoint snapshot."""
    has_next = bool(checkpoint and checkpoint.get("next"))
    if exception:
        if ignore_user_control:
            from langgraph_api.errors import UserInterrupt, UserRollback

            if not isinstance(exception, (UserInterrupt, UserRollback)):
                base = "error"
            elif has_next:
                base = "interrupted"
            else:
                base = "idle"
        else:
            base = "error"
    elif has_next:
        base = "interrupted"
    else:
        base = "idle"

    interrupts: dict = {}
    if checkpoint is not None:
        interrupts = {
            t["id"]: [_patch_interrupt(i) for i in (t.get("interrupts") or [])]
            for t in (checkpoint.get("tasks") or [])
            if t.get("interrupts")
        }
    return base, interrupts


def _test_mode() -> bool:
    return os.environ.get("LG_RUNTIME_PG_TEST", "") == "1"


def _snapshot_defaults() -> dict:
    """Kwargs for StateSnapshot on langgraph versions that require ``interrupts``."""
    if not hasattr(StateSnapshot, "interrupts"):
        return {}
    return {"interrupts": ()}


def _empty_state_snapshot() -> StateSnapshot:
    return StateSnapshot(
        values={},
        next=(),
        config=cast(Any, None),
        metadata=None,
        created_at=None,
        parent_config=None,
        tasks=(),
        **_snapshot_defaults(),
    )


async def _get_checkpointer(*, unpack_hook=None):
    from langgraph_api import config as api_config

    if getattr(api_config, "USE_CUSTOM_CHECKPOINTER", False):
        from langgraph_api import _checkpointer as api_checkpointer

        return await api_checkpointer.get_checkpointer()
    return Checkpointer(unpack_hook=unpack_hook)


async def _adelete_thread_checkpoints(thread_id: UUID | str) -> None:
    checkpointer = await _get_checkpointer()
    await checkpointer.adelete_thread(str(thread_id))


async def _acopy_thread_checkpoints(source_thread_id: str, target_thread_id: str) -> None:
    """Copy checkpoints; fall back to alist/aput when acopy_thread is unimplemented."""
    checkpointer = await _get_checkpointer()
    try:
        await checkpointer.acopy_thread(source_thread_id, target_thread_id)
        return
    except NotImplementedError:
        pass

    cfg: dict[str, Any] = {"configurable": {"thread_id": source_thread_id}}
    checkpoints = [cp async for cp in checkpointer.alist(cfg)]
    checkpoints.sort(key=lambda x: x.config["configurable"]["checkpoint_id"])
    for cp in checkpoints:
        ns = cp.config["configurable"].get("checkpoint_ns", "")
        new_config: dict[str, Any] = {
            "configurable": {
                "thread_id": target_thread_id,
                "checkpoint_ns": ns,
            }
        }
        parent_config = cp.parent_config
        if parent_config and parent_config.get("configurable"):
            parent_id = parent_config["configurable"].get("checkpoint_id")
            if parent_id is not None:
                new_config["configurable"]["checkpoint_id"] = parent_id
        new_metadata = dict(cp.metadata or {})
        if "thread_id" in new_metadata:
            new_metadata["thread_id"] = target_thread_id
        stored_config = await checkpointer.aput(
            new_config,
            cp.checkpoint,
            new_metadata,
            cp.checkpoint.get("channel_versions", {}),
        )
        if cp.pending_writes:
            writes_by_task: dict[str, list[tuple[str, Any]]] = {}
            for task_id, channel, value in cp.pending_writes:
                writes_by_task.setdefault(task_id, []).append((channel, value))
            for task_id, writes in writes_by_task.items():
                await checkpointer.aput_writes(stored_config, writes, task_id)


class Authenticated:
    resource: Literal["threads", "crons", "assistants"] = "threads"

    @classmethod
    def _context(
        cls,
        ctx: Any,
        action: str,
    ) -> Any:
        if not ctx:
            return None
        from langgraph_sdk import Auth

        return Auth.types.AuthContext(
            user=ctx.user,
            permissions=ctx.permissions,
            resource=cls.resource,
            action=cast(Any, action),
        )

    @classmethod
    async def handle_event(
        cls,
        ctx: Any,
        action: str,
        value: Any,
    ) -> Any:
        from langgraph_api.auth.custom import handle_event
        from langgraph_api.utils import get_auth_ctx

        ctx = ctx or get_auth_ctx()
        if not ctx:
            return None
        return await handle_event(cls._context(ctx, action), value)


def _run_stream_mode_matches(event_mode: str, stream_mode: list | str | None) -> bool:
    if not stream_mode:
        return True
    modes = [stream_mode] if isinstance(stream_mode, str) else list(stream_mode)
    if event_mode in modes:
        return True
    if ("messages" in modes or "messages-tuple" in modes) and event_mode.startswith("messages"):
        return True
    if "|" in event_mode:
        base_mode, _, _ = event_mode.partition("|")
        if base_mode in modes:
            return True
    return False


class WrappedHTTPException(Exception):
    def __init__(self, http_exception: HTTPException):
        self.http_exception = http_exception


def _ensure_uuid(id_: str | UUID | None) -> UUID:
    if isinstance(id_, str):
        return UUID(id_)
    if id_ is None:
        return uuid4()
    return id_


# Whitelist sort fields: getattr(Model, "metadata") is SQLAlchemy MetaData, not metadata_.
_ASSISTANT_SORT_FIELDS = frozenset({"assistant_id", "graph_id", "name", "created_at", "updated_at"})
_THREAD_SORT_FIELDS = frozenset(
    {"thread_id", "created_at", "updated_at", "state_updated_at", "status"}
)
_CRON_SORT_FIELDS = frozenset(
    {
        "cron_id",
        "assistant_id",
        "thread_id",
        "next_run_date",
        "end_time",
        "created_at",
        "updated_at",
    }
)


def _resolve_sort_field(
    sort_by: str | None,
    allowed: frozenset[str],
    default: str,
    *,
    raise_invalid: bool = False,
) -> str:
    sb = (sort_by or "").lower() if sort_by else ""
    if sb in allowed:
        return sb
    if sort_by and raise_invalid:
        opts = ", ".join(sorted(allowed))
        raise HTTPException(
            status_code=422,
            detail=f"Invalid sort_by field: '{sort_by}'. Valid options are: {opts}",
        )
    return default


def _row_sort_key(row: Any, attr: str) -> tuple:
    """Nullable-safe sort key — avoids TypeError mixing datetime with None/str."""
    val = getattr(row, attr, None)
    return (val is None, val)


def _thread_search_item(
    row: Any,
    *,
    select: list | None = None,
    extract: dict | None = None,
) -> dict[str, Any]:
    """Build a Threads.search result dict."""
    d = thread_to_dict(row)
    d.setdefault("state_updated_at", d.get("updated_at"))
    if select:
        out = {k: v for k, v in d.items() if k in select}
    else:
        out = d
    if extract:
        from langgraph_api.utils.extract import extract_path_value

        out["extracted"] = {alias: extract_path_value(d, path) for alias, path in extract.items()}
    return out


def _merge_jsonb(*objects: dict | None) -> dict:
    """Shallow JSONB-style merge (later keys win)."""
    result: dict = {}
    for obj in objects:
        if obj:
            result.update(copy.deepcopy(obj))
    return result


def _plain_metadata_filter(filters: Any | None) -> dict | None:
    """Return filters pushable as JSONB ``@>``, or None if operators need Python."""
    if not filters:
        return {}
    if not isinstance(filters, dict):
        return None
    if any(str(k).startswith("$") for k in filters):
        return None
    for value in filters.values():
        if isinstance(value, dict) and any(str(k).startswith("$") for k in value):
            return None
    return filters


def _auth_denies(metadata: dict | None, filters: Any | None) -> bool:
    return bool(filters) and not _check_filter_match(metadata or {}, filters)


def _check_filter_match(
    metadata: dict,
    filters: Any | None,
    nesting_level: int = 0,
) -> bool:
    if not filters:
        return True
    if nesting_level > 2:
        raise HTTPException(status_code=500, detail="Too many nested filter operators")

    if "$or" in filters:
        or_groups = filters["$or"]
        if not any(_check_filter_match(metadata, g, nesting_level + 1) for g in or_groups):
            return False
        remaining = {k: v for k, v in filters.items() if k != "$or"}
        if remaining:
            return _check_filter_match(metadata, remaining, nesting_level + 1)
        return True

    if "$and" in filters:
        and_groups = filters["$and"]
        if not all(_check_filter_match(metadata, g, nesting_level + 1) for g in and_groups):
            return False
        remaining = {k: v for k, v in filters.items() if k != "$and"}
        if remaining:
            return _check_filter_match(metadata, remaining, nesting_level + 1)
        return True

    for key, value in filters.items():
        if isinstance(value, dict):
            op = next(iter(value))
            filter_value = value[op]
            if op == "$eq":
                if key not in metadata or metadata[key] != filter_value:
                    return False
            elif op == "$contains":
                if key not in metadata or not isinstance(metadata[key], list):
                    return False
                if isinstance(filter_value, list):
                    for el in filter_value:
                        if el not in metadata[key]:
                            return False
                elif filter_value not in metadata[key]:
                    return False
        else:
            if key not in metadata or metadata[key] != value:
                return False
    return True


def _assert_graph_exists(graph_id: str) -> None:
    if _test_mode():
        return
    try:
        from langgraph_api.graph import assert_graph_exists

        assert_graph_exists(graph_id)
    except ImportError:
        pass


async def _aget_assistant(session, assistant_id: UUID) -> AssistantRow | None:
    return await session.get(AssistantRow, assistant_id)


async def _aget_thread(session, thread_id: UUID) -> ThreadRow | None:
    return await session.get(ThreadRow, thread_id)


async def _aget_run(session, run_id: UUID) -> RunRow | None:
    return await session.get(RunRow, run_id)


async def _aget_cron(session, cron_id: UUID) -> CronRow | None:
    return await session.get(CronRow, cron_id)


async def _adelete_retry_counters(session, run_ids: Sequence[UUID]) -> None:
    """Drop retry_counters for deleted runs (no FK cascade on that table)."""
    if not run_ids:
        return
    await session.execute(delete(RetryCounterRow).where(RetryCounterRow.run_id.in_(run_ids)))


async def _thread_has_live_worker(session, thread_id: UUID) -> bool:
    """True if any in-flight/cancelled run still heartbeats."""
    run_ids = list(
        (
            await session.execute(
                sa_select(RunRow.run_id).where(
                    RunRow.thread_id == thread_id,
                    RunRow.status.in_(("pending", "running", "interrupted")),
                )
            )
        )
        .scalars()
        .all()
    )
    for rid in run_ids:
        alive = await has_run_heartbeat(rid)
        if alive is not False:
            return True
    return False


async def _thread_has_inflight_work(session, thread_id: UUID) -> bool:
    """Pending/running rows, or a cancelled worker that is still heartbeating."""
    pending = int(
        await session.scalar(
            sa_select(func.count())
            .select_from(RunRow)
            .where(
                RunRow.thread_id == thread_id,
                RunRow.status.in_(("pending", "running")),
            )
        )
        or 0
    )
    if pending:
        return True
    return await _thread_has_live_worker(session, thread_id)


class Assistants(Authenticated):
    resource = "assistants"

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        *,
        graph_id: str | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str | None = None,
        sort_order: str | None = None,
        select: list | None = None,
        ctx: Any = None,
    ) -> tuple[AsyncIterator, int | None]:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx,
            "search",
            Auth.types.AssistantsSearch(
                graph_id=graph_id, metadata=metadata, limit=limit, offset=offset
            ),
        )
        if graph_id is not None:
            _assert_graph_exists(graph_id)

        q = sa_select(AssistantRow)
        if graph_id:
            q = q.where(AssistantRow.graph_id == graph_id)
        if name:
            q = q.where(_name_ilike(name))
        if metadata:
            q = q.where(AssistantRow.metadata_.contains(metadata))
        plain = _plain_metadata_filter(filters)
        if plain:
            q = q.where(AssistantRow.metadata_.contains(plain))
        elif filters:
            rows = list((await conn.session.execute(q)).scalars())
            rows = [r for r in rows if _check_filter_match(r.metadata_ or {}, filters)]
            sb = _resolve_sort_field(sort_by, _ASSISTANT_SORT_FIELDS, "created_at")
            reverse = not (sort_order and sort_order.upper() == "ASC")
            rows.sort(key=lambda r: _row_sort_key(r, sb), reverse=reverse)
            page = rows[offset : offset + limit]
            cursor = offset + limit if len(rows) > offset + limit else None
            items = []
            for r in page:
                d = assistant_to_dict(r)
                items.append({k: v for k, v in d.items() if k in select} if select else d)

            async def _iter_filtered():
                for d in items:
                    yield d

            return _iter_filtered(), cursor

        sb = _resolve_sort_field(sort_by, _ASSISTANT_SORT_FIELDS, "created_at")
        col = getattr(AssistantRow, sb)
        reverse = not (sort_order and sort_order.upper() == "ASC")
        q = q.order_by(col.desc() if reverse else col.asc())
        q = q.offset(offset).limit(limit + 1)

        rows = list((await conn.session.execute(q)).scalars())
        cursor = offset + limit if len(rows) > limit else None
        page = rows[:limit]
        # Materialize dicts while the session is still open (API paginates outside connect()).
        items = []
        for r in page:
            d = assistant_to_dict(r)
            items.append({k: v for k, v in d.items() if k in select} if select else d)

        async def _iter():
            for d in items:
                yield d

        return _iter(), cursor

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        filters = await Assistants.handle_event(
            ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
        )
        # Eagerly load before returning the iterator. langgraph_api often does
        # ``async with connect(): it = await Assistants.get(...)`` then
        # ``await fetchone(it)`` *outside* the connect block. A lazy DB await
        # inside ``_yield`` would re-checkout a pool connection after the
        # session closed and never check it in (GC non-checked-in warning).
        row = await _aget_assistant(conn.session, assistant_id)
        data = None
        if row is not None and (not filters or _check_filter_match(row.metadata_ or {}, filters)):
            data = copy.deepcopy(assistant_to_dict(row))

        async def _yield():
            if data is not None:
                yield data

        return _yield()

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        *,
        graph_id: str,
        config: dict | None = None,
        context: dict | None = None,
        metadata: dict | None = None,
        if_exists: str = "raise",
        name: str = "",
        description: str | None = None,
        ctx: Any = None,
        system: bool = False,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        config = config or {}
        context = context or {}
        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx,
            "create",
            Auth.types.AssistantsCreate(
                assistant_id=assistant_id,
                graph_id=graph_id,
                config=config,
                context=context,
                metadata=metadata,
                name=name,
            ),
        )
        _assert_graph_exists(graph_id)

        if config.get("configurable") and context:
            raise HTTPException(
                status_code=400,
                detail="Cannot specify both configurable and context.",
            )
        if config.get("configurable"):
            context = config["configurable"]
        elif context:
            config["configurable"] = context

        existing = await _aget_assistant(conn.session, assistant_id)
        if existing:
            if filters and not _check_filter_match(existing.metadata_ or {}, filters):
                raise HTTPException(
                    status_code=409, detail=f"Assistant {assistant_id} already exists"
                )
            if if_exists == "raise":
                raise HTTPException(
                    status_code=409, detail=f"Assistant {assistant_id} already exists"
                )
            if if_exists == "do_nothing":
                # Snapshot while session is open — API may drain after connect().
                data = assistant_to_dict(existing)

                async def _yield_existing():
                    yield data

                return _yield_existing()

        now = datetime.now(UTC)
        # ON CONFLICT so concurrent replica startups (system assistants) are safe.
        ins = (
            pg_insert(AssistantRow)
            .values(
                assistant_id=assistant_id,
                graph_id=graph_id,
                config=config,
                context=context,
                metadata_=metadata,
                name=name,
                description=description,
                version=1,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=["assistant_id"])
        )
        result = await conn.session.execute(ins)
        if result.rowcount == 0:
            existing = await _aget_assistant(conn.session, assistant_id)
            # Re-check auth: another replica may have inserted first.
            if existing is None or (
                filters and not _check_filter_match(existing.metadata_ or {}, filters)
            ):
                raise HTTPException(
                    status_code=409, detail=f"Assistant {assistant_id} already exists"
                )
            if if_exists == "raise":
                raise HTTPException(
                    status_code=409, detail=f"Assistant {assistant_id} already exists"
                )

            data = assistant_to_dict(existing)

            async def _yield_raced():
                yield data

            return _yield_raced()

        await conn.session.execute(
            pg_insert(AssistantVersionRow)
            .values(
                assistant_id=assistant_id,
                version=1,
                graph_id=graph_id,
                config=config,
                context=context,
                metadata_=metadata,
                name=name,
                description=description,
                created_at=now,
            )
            .on_conflict_do_nothing(index_elements=["assistant_id", "version"])
        )
        await conn.session.flush()
        row = await _aget_assistant(conn.session, assistant_id)
        data = assistant_to_dict(row)

        async def _yield_new():
            yield data

        return _yield_new()

    @staticmethod
    async def patch(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        *,
        config: dict | None = None,
        context: dict | None = None,
        graph_id: str | None = None,
        metadata: dict | None = None,
        name: str | None = None,
        description: str | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        config = config if config is not None else {}
        metadata = metadata if metadata is not None else {}
        filters = await Assistants.handle_event(
            ctx,
            "update",
            Auth.types.AssistantsUpdate(
                assistant_id=assistant_id,
                graph_id=graph_id,
                config=config,
                metadata=metadata,
            ),
        )

        if graph_id is not None:
            _assert_graph_exists(graph_id)
        if config.get("configurable") and context:
            raise HTTPException(
                status_code=400, detail="Cannot specify both configurable and context."
            )
        if config.get("configurable"):
            context = config["configurable"]
        elif context:
            config["configurable"] = context

        # Lock so concurrent patches cannot allocate the same version (PK on assistant_versions).
        assistant = (
            await conn.session.execute(
                sa_select(AssistantRow)
                .where(AssistantRow.assistant_id == assistant_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if not assistant:
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")
        if filters and not _check_filter_match(assistant.metadata_ or {}, filters):
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

        now = datetime.now(UTC)
        max_ver = await conn.session.scalar(
            sa_select(func.coalesce(func.max(AssistantVersionRow.version), 0)).where(
                AssistantVersionRow.assistant_id == assistant_id
            )
        )
        new_version_num = int(max_ver or 0) + 1

        new_graph = graph_id if graph_id is not None else assistant.graph_id
        new_config = config if config else assistant.config
        new_context = context if context is not None else (assistant.context or {})
        new_meta = (
            {**(assistant.metadata_ or {}), **metadata} if metadata else (assistant.metadata_ or {})
        )
        new_name = name if name is not None else assistant.name
        new_desc = description if description is not None else assistant.description

        conn.session.add(
            AssistantVersionRow(
                assistant_id=assistant_id,
                version=new_version_num,
                graph_id=new_graph,
                config=new_config,
                context=new_context,
                metadata_=new_meta,
                name=new_name,
                description=new_desc,
                created_at=now,
            )
        )
        assistant.graph_id = new_graph
        assistant.config = new_config
        assistant.context = new_context
        assistant.metadata_ = new_meta
        assistant.name = new_name
        assistant.description = new_desc
        assistant.updated_at = now
        assistant.version = new_version_num
        await conn.session.flush()
        data = assistant_to_dict(assistant)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def delete(
        conn: PgConnectionProto | None,
        assistant_id: UUID | str,
        ctx: Any = None,
        *,
        delete_threads: bool = False,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        async with AsyncExitStack() as stack:
            if conn is None:
                conn = await stack.enter_async_context(connect())

            assistant_id = _ensure_uuid(assistant_id)
            filters = await Assistants.handle_event(
                ctx, "delete", Auth.types.AssistantsDelete(assistant_id=assistant_id)
            )
            assistant = await _aget_assistant(conn.session, assistant_id)
            if not assistant:
                raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")
            if filters and not _check_filter_match(assistant.metadata_ or {}, filters):
                raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

            if delete_threads:
                result = await conn.session.execute(
                    sa_select(ThreadRow.thread_id).where(
                        ThreadRow.metadata_.contains({"assistant_id": str(assistant_id)})
                    )
                )
                for (thread_id,) in result.all():
                    try:
                        async for _ in await Threads.delete(conn, thread_id, ctx=ctx):
                            pass
                    except HTTPException:
                        await logger.awarning(
                            "Skipping thread deletion during cascade delete",
                            thread_id=str(thread_id),
                            assistant_id=str(assistant_id),
                        )

            await Runs.cancel(conn, assistant_id=assistant_id, action="interrupt", ctx=ctx)

            await conn.session.execute(
                delete(AssistantVersionRow).where(AssistantVersionRow.assistant_id == assistant_id)
            )
            await conn.session.execute(delete(CronRow).where(CronRow.assistant_id == assistant_id))
            await conn.session.delete(assistant)
            await conn.session.flush()

            async def _yield():
                yield assistant_id

            return _yield()

    @staticmethod
    async def set_latest(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        version: int,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        filters = await Assistants.handle_event(
            ctx,
            "update",
            Auth.types.AssistantsUpdate(assistant_id=assistant_id, version=version),
        )
        assistant = await _aget_assistant(conn.session, assistant_id)
        if not assistant:
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")
        if filters and not _check_filter_match(assistant.metadata_ or {}, filters):
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

        version_data = await conn.session.get(AssistantVersionRow, (assistant_id, version))
        if not version_data:
            raise HTTPException(status_code=404, detail=f"Version {version} not found")

        assistant.graph_id = version_data.graph_id
        assistant.config = version_data.config
        assistant.context = version_data.context
        assistant.metadata_ = version_data.metadata_
        assistant.version = version_data.version
        assistant.updated_at = datetime.now(UTC)
        assistant.name = version_data.name
        assistant.description = version_data.description
        await conn.session.flush()
        data = assistant_to_dict(assistant)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def get_versions(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        metadata: dict | None = None,
        limit: int = 10,
        offset: int = 0,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
        )
        assistant = await _aget_assistant(conn.session, assistant_id)
        if not assistant:
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

        q = sa_select(AssistantVersionRow).where(AssistantVersionRow.assistant_id == assistant_id)
        if metadata:
            q = q.where(AssistantVersionRow.metadata_.contains(metadata))
        q = q.order_by(AssistantVersionRow.version.desc()).offset(offset).limit(limit)
        rows = list((await conn.session.execute(q)).scalars())
        # Materialize while session is open — API drains versions outside connect().
        default_name = assistant.name
        default_description = assistant.description
        items: list[dict] = []
        for r in rows:
            d = assistant_version_to_dict(r)
            if filters and not _check_filter_match(d.get("metadata") or {}, filters):
                continue
            d.setdefault("name", default_name)
            d.setdefault("description", default_description)
            items.append(d)

        async def _yield():
            for d in items:
                yield d

        return _yield()

    @staticmethod
    async def count(
        conn: PgConnectionProto,
        *,
        graph_id: str | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        ctx: Any = None,
    ) -> int:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx,
            "search",
            Auth.types.AssistantsSearch(graph_id=graph_id, metadata=metadata, limit=0, offset=0),
        )
        if graph_id is not None:
            _assert_graph_exists(graph_id)

        plain = _plain_metadata_filter(filters)
        if filters and plain is None:
            q = sa_select(AssistantRow)
            if graph_id:
                q = q.where(AssistantRow.graph_id == graph_id)
            if name:
                q = q.where(_name_ilike(name))
            if metadata:
                q = q.where(AssistantRow.metadata_.contains(metadata))
            rows = list((await conn.session.execute(q)).scalars())
            return sum(1 for r in rows if _check_filter_match(r.metadata_ or {}, filters))

        count_q = sa_select(func.count()).select_from(AssistantRow)
        if graph_id:
            count_q = count_q.where(AssistantRow.graph_id == graph_id)
        if name:
            count_q = count_q.where(_name_ilike(name))
        if metadata:
            count_q = count_q.where(AssistantRow.metadata_.contains(metadata))
        if plain:
            count_q = count_q.where(AssistantRow.metadata_.contains(plain))
        return int(await conn.session.scalar(count_q) or 0)


class Threads(Authenticated):
    resource = "threads"

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        *,
        ids: list | None = None,
        metadata: dict | None = None,
        values: dict | None = None,
        status: str | None = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str | None = None,
        sort_order: str | None = None,
        select: list | None = None,
        extract: dict | None = None,
        ctx: Any = None,
    ) -> tuple[AsyncIterator, int | None]:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        values = values or {}
        filters = await Threads.handle_event(
            ctx,
            "search",
            Auth.types.ThreadsSearch(
                metadata=metadata,
                values=values,
                status=cast(Any, status),
                limit=limit,
                offset=offset,
            ),
        )
        q = sa_select(ThreadRow)
        if ids:
            id_set = [_ensure_uuid(i) for i in ids]
            q = q.where(ThreadRow.thread_id.in_(id_set))
        if metadata:
            q = q.where(ThreadRow.metadata_.contains(metadata))
        if values:
            q = q.where(ThreadRow.values_.contains(values))
        if status:
            q = q.where(ThreadRow.status == status)
        plain = _plain_metadata_filter(filters)
        if plain:
            q = q.where(ThreadRow.metadata_.contains(plain))
        elif filters:
            rows = list((await conn.session.execute(q)).scalars())
            rows = [r for r in rows if _check_filter_match(r.metadata_ or {}, filters)]
            sb = _resolve_sort_field(sort_by, _THREAD_SORT_FIELDS, "updated_at")
            reverse = not (sort_order and sort_order.upper() == "ASC")
            rows.sort(key=lambda r: _row_sort_key(r, sb), reverse=reverse)
            page = rows[offset : offset + limit]
            cursor = offset + limit if len(rows) > offset + limit else None
            # Materialize while session is open — API paginates outside connect().
            items = [_thread_search_item(r, select=select, extract=extract) for r in page]

            async def _iter_filtered():
                for d in items:
                    yield d

            return _iter_filtered(), cursor

        sb = _resolve_sort_field(sort_by, _THREAD_SORT_FIELDS, "updated_at")
        col = getattr(ThreadRow, sb)
        reverse = not (sort_order and sort_order.upper() == "ASC")
        q = q.order_by(col.desc() if reverse else col.asc())
        q = q.offset(offset).limit(limit + 1)

        rows = list((await conn.session.execute(q)).scalars())
        cursor = offset + limit if len(rows) > limit else None
        page = rows[:limit]
        # Materialize while session is open — API paginates outside connect().
        items = [_thread_search_item(r, select=select, extract=extract) for r in page]

        async def _iter():
            for d in items:
                yield d

        return _iter(), cursor

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        ctx: Any = None,
        include_ttl: bool = False,
        read_mask_paths: list | None = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Threads.handle_event(
            ctx, "read", Auth.types.ThreadsRead(thread_id=thread_id)
        )
        row = await _aget_thread(conn.session, thread_id)
        if not row or (filters and not _check_filter_match(row.metadata_ or {}, filters)):
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
        d = thread_to_dict(row)
        d.setdefault("state_updated_at", d.get("updated_at"))

        async def _yield():
            yield d

        return _yield()

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        *,
        metadata: dict | None = None,
        if_exists: str = "raise",
        ttl: Any = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        metadata = metadata or {}
        filters = await Threads.handle_event(
            ctx,
            "create",
            Auth.types.ThreadsCreate(
                thread_id=thread_id,
                metadata=metadata,
                if_exists=cast(Any, if_exists),
            ),
        )

        existing = await _aget_thread(conn.session, thread_id)
        if existing:
            if filters and not _check_filter_match(existing.metadata_ or {}, filters):
                raise HTTPException(status_code=409, detail=f"Thread {thread_id} already exists")
            if if_exists == "raise":
                raise HTTPException(status_code=409, detail=f"Thread {thread_id} already exists")
            if if_exists == "do_nothing":
                data = thread_to_dict(existing)

                async def _yield_existing():
                    yield data

                return _yield_existing()

        now = datetime.now(UTC)
        # ON CONFLICT so concurrent replica creates of the same id are safe.
        ins = (
            pg_insert(ThreadRow)
            .values(
                thread_id=thread_id,
                created_at=now,
                updated_at=now,
                state_updated_at=now,
                metadata_=copy.deepcopy(metadata),
                status="idle",
                config={},
                values_=None,
                interrupts={},
            )
            .on_conflict_do_nothing(index_elements=["thread_id"])
        )
        result = await conn.session.execute(ins)
        if result.rowcount == 0:
            existing = await _aget_thread(conn.session, thread_id)
            # Re-check auth: another replica may have inserted first.
            if existing is None or (
                filters and not _check_filter_match(existing.metadata_ or {}, filters)
            ):
                raise HTTPException(status_code=409, detail=f"Thread {thread_id} already exists")
            if if_exists == "raise":
                raise HTTPException(status_code=409, detail=f"Thread {thread_id} already exists")

            data = thread_to_dict(existing)

            async def _yield_raced():
                yield data

            return _yield_raced()

        await conn.session.flush()
        row = await _aget_thread(conn.session, thread_id)
        data = thread_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def patch(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        *,
        metadata: dict | None = None,
        ttl: Any = None,
        ctx: Any = None,
        read_mask_paths: list | None = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Threads.handle_event(
            ctx,
            "update",
            Auth.types.ThreadsUpdate(thread_id=thread_id, metadata=cast(Any, metadata or {})),
        )
        row = await _aget_thread(conn.session, thread_id)
        if row is not None and (not filters or _check_filter_match(row.metadata_ or {}, filters)):
            if metadata:
                row.metadata_ = {**(row.metadata_ or {}), **metadata}
                row.updated_at = datetime.now(UTC)
                await conn.session.flush()
            d = thread_to_dict(row)
            d.setdefault("state_updated_at", d.get("updated_at"))

            async def _yield():
                yield d

            return _yield()

        return _empty_aiter()

    @staticmethod
    async def delete(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Threads.handle_event(
            ctx, "delete", Auth.types.ThreadsDelete(thread_id=thread_id)
        )
        row = await _aget_thread(conn.session, thread_id)
        if row is None or (filters and not _check_filter_match(row.metadata_ or {}, filters)):
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")

        run_ids = list(
            (
                await conn.session.execute(
                    sa_select(RunRow.run_id).where(RunRow.thread_id == thread_id)
                )
            )
            .scalars()
            .all()
        )
        await _adelete_retry_counters(conn.session, run_ids)
        await conn.session.execute(delete(RunRow).where(RunRow.thread_id == thread_id))
        await conn.session.execute(delete(CronRow).where(CronRow.thread_id == thread_id))
        await conn.session.delete(row)
        await conn.session.flush()
        # Checkpointer is a separate autocommit pool — delete after ops commit so we never roll back the thread after checkpoints are gone.
        deleted_id = thread_id

        async def _cleanup_checkpoints() -> None:
            try:
                await _adelete_thread_checkpoints(deleted_id)
            except Exception:
                logger.exception(
                    "Failed to delete checkpoints for thread %s",
                    deleted_id,
                )

        conn.schedule_after_commit(_cleanup_checkpoints)

        async def _yield():
            yield deleted_id

        return _yield()

    @staticmethod
    async def set_joint_status(
        conn: PgConnectionProto,
        thread_id: UUID,
        run_id: UUID,
        run_status: str,
        graph_id: str,
        checkpoint: dict | None = None,
        exception: BaseException | None = None,
    ) -> None:
        """Atomically update run + thread status; uses a fresh session (worker may close outer conn)."""
        from langgraph_api.serde import json_dumpb, json_loads

        thread_id = _ensure_uuid(thread_id)
        run_id = _ensure_uuid(run_id)

        base_thread_status, interrupts = _thread_status_from_checkpoint(
            checkpoint, exception, ignore_user_control=True
        )
        now = datetime.now(UTC)
        values = (
            json_loads(json_dumpb(checkpoint.get("values"))) if checkpoint is not None else None
        )
        error = json_loads(json_dumpb(exception)) if exception else None

        sf = get_session_factory()
        async with sf() as session:
            thread = await _aget_thread(session, thread_id)
            if thread is None:
                raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
            run = await _aget_run(session, run_id)
            if run is None or run.thread_id != thread_id:
                raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

            if run_status == "rollback":
                await _adelete_retry_counters(session, [run_id])
                await session.delete(run)
                await session.flush()
            else:
                run.status = run_status
                run.updated_at = now
                await session.flush()

            pending = int(
                await session.scalar(
                    sa_select(func.count())
                    .select_from(RunRow)
                    .where(
                        RunRow.thread_id == thread_id,
                        RunRow.status.in_(("pending", "running")),
                    )
                )
                or 0
            )

            meta = dict(thread.metadata_ or {})
            meta["graph_id"] = graph_id
            thread.metadata_ = meta
            thread.status = "busy" if pending else base_thread_status
            thread.updated_at = now
            thread.state_updated_at = now
            thread.interrupts = interrupts
            thread.error = error
            # Always write values (including None) when a checkpoint is present so stale values cannot linger.
            if checkpoint is not None:
                thread.values_ = values
            await session.commit()

    @staticmethod
    async def set_status(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        checkpoint: dict | None,
        exception: BaseException | None,
    ) -> None:
        from langgraph_api.serde import json_dumpb, json_loads

        thread_id = _ensure_uuid(thread_id)
        thread = await _aget_thread(conn.session, thread_id)
        if not thread:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")

        status, interrupts = _thread_status_from_checkpoint(checkpoint, exception)
        pending = await conn.session.scalar(
            sa_select(func.count())
            .select_from(RunRow)
            .where(
                RunRow.thread_id == thread_id,
                RunRow.status.in_(("pending", "running")),
            )
        )
        if pending:
            status = "busy"

        now = datetime.now(UTC)
        thread.updated_at = now
        thread.state_updated_at = now
        thread.status = status
        if checkpoint is not None:
            thread.values_ = json_loads(json_dumpb(checkpoint.get("values")))
        thread.interrupts = interrupts
        thread.error = json_loads(json_dumpb(exception)) if exception else None
        await conn.session.flush()

    @staticmethod
    async def copy(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        read_filters = await Threads.handle_event(
            ctx, "read", Auth.types.ThreadsRead(thread_id=thread_id)
        )
        original = await _aget_thread(conn.session, thread_id)
        if not original or (
            read_filters and not _check_filter_match(original.metadata_ or {}, read_filters)
        ):
            return _empty_aiter()

        new_id = uuid4()
        meta = copy.deepcopy(original.metadata_ or {})
        await Threads.handle_event(
            ctx,
            "create",
            Auth.types.ThreadsCreate(thread_id=new_id, metadata=meta, if_exists="raise"),
        )
        now = datetime.now(UTC)
        row = ThreadRow(
            thread_id=new_id,
            created_at=now,
            updated_at=now,
            state_updated_at=now,
            metadata_=meta,
            status="idle",
            config=copy.deepcopy(original.config or {}),
            values_=copy.deepcopy(original.values_),
            interrupts=copy.deepcopy(original.interrupts or {}),
        )
        conn.session.add(row)
        await conn.session.flush()
        try:
            await _acopy_thread_checkpoints(str(thread_id), str(new_id))
        except Exception:
            # Separate autocommit pool — scrub target checkpoints if copy fails before this conn commits.
            try:
                await _adelete_thread_checkpoints(new_id)
            except Exception:
                logger.exception(
                    "Failed to scrub checkpoints after copy failure",
                    source=str(thread_id),
                    target=str(new_id),
                )
            raise

        # Checkpoints already durable; scrub target on ops txn rollback to avoid orphans.
        copied_target = new_id

        async def _scrub_orphaned_checkpoints() -> None:
            try:
                await _adelete_thread_checkpoints(copied_target)
            except Exception:
                logger.exception(
                    "Failed to scrub checkpoints after thread copy rollback",
                    source=str(thread_id),
                    target=str(copied_target),
                )

        conn.schedule_after_rollback(_scrub_orphaned_checkpoints)
        data = thread_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def count(
        conn: PgConnectionProto,
        *,
        metadata: dict | None = None,
        values: dict | None = None,
        status: str | None = None,
        ctx: Any = None,
    ) -> int:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        values = values or {}
        filters = await Threads.handle_event(
            ctx,
            "search",
            Auth.types.ThreadsSearch(
                metadata=metadata,
                values=values,
                status=cast(Any, status),
                limit=0,
                offset=0,
            ),
        )
        plain = _plain_metadata_filter(filters)
        if filters and plain is None:
            q = sa_select(ThreadRow)
            if metadata:
                q = q.where(ThreadRow.metadata_.contains(metadata))
            if values:
                q = q.where(ThreadRow.values_.contains(values))
            if status:
                q = q.where(ThreadRow.status == status)
            rows = list((await conn.session.execute(q)).scalars())
            return sum(1 for r in rows if _check_filter_match(r.metadata_ or {}, filters))

        count_q = sa_select(func.count()).select_from(ThreadRow)
        if metadata:
            count_q = count_q.where(ThreadRow.metadata_.contains(metadata))
        if values:
            count_q = count_q.where(ThreadRow.values_.contains(values))
        if status:
            count_q = count_q.where(ThreadRow.status == status)
        if plain:
            count_q = count_q.where(ThreadRow.metadata_.contains(plain))
        return int(await conn.session.scalar(count_q) or 0)

    @staticmethod
    async def prune(
        thread_ids: Sequence[str] | Sequence[UUID],
        strategy: Literal["delete", "keep_latest"] = "delete",
        batch_size: int = 100,
        ctx: Any = None,
    ) -> int:
        """Prune threads by ID. ``langgraph_api`` calls this without a conn."""
        del batch_size  # reserved for batched deletes; unused for now
        if not thread_ids:
            return 0
        if strategy == "keep_latest":
            # AsyncPostgresSaver inherits a stub aprune that raises NotImplementedError.
            checkpointer = await _get_checkpointer()
            try:
                await checkpointer.aprune(list(thread_ids), strategy=strategy)
            except (NotImplementedError, RuntimeError) as exc:
                raise HTTPException(
                    status_code=422,
                    detail="keep_latest strategy is not supported by this checkpointer",
                ) from exc
            return len(thread_ids)

        pruned = 0
        async with connect() as conn:
            for tid in thread_ids:
                try:
                    result = await Threads.delete(conn, tid, ctx=ctx)
                    async for _ in result:
                        pruned += 1
                except HTTPException:
                    pass
        return pruned

    @staticmethod
    async def sweep_ttl(
        conn: PgConnectionProto,
        *,
        limit: int | None = None,
        batch_size: int = 100,
    ) -> tuple[int, int]:
        """TTL sweep — no thread TTL column yet; no-op."""
        del conn, limit, batch_size
        return (0, 0)

    class State(Authenticated):
        resource = "threads"

        @staticmethod
        async def get(
            conn: PgConnectionProto,
            config: dict,
            subgraphs: bool = False,
            ctx: Any = None,
        ):
            from langgraph_api.graph import get_graph
            from langgraph_api.store import get_store

            checkpointer = await _get_checkpointer()
            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            try:
                thread = await anext(await Threads.get(conn, thread_id, ctx=ctx))
            except (HTTPException, StopAsyncIteration):
                return _empty_state_snapshot()

            metadata = thread.get("metadata", {}) or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }

            graph_id = metadata.get("graph_id")
            if not graph_id:
                result = await conn.session.execute(
                    sa_select(RunRow)
                    .where(RunRow.thread_id == thread_id)
                    .order_by(RunRow.created_at.desc())
                    .limit(1)
                )
                run = result.scalar_one_or_none()
                if run is not None:
                    try:
                        graph_id = (
                            (run.kwargs or {})
                            .get("config", {})
                            .get("configurable", {})
                            .get("graph_id")
                        )
                    except Exception:
                        graph_id = None

            if not graph_id:
                return _empty_state_snapshot()

            if hasattr(checkpointer, "latest_iter"):
                checkpointer.latest_iter = await checkpointer.aget(config)
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.read",
            ) as graph:
                result = await graph.aget_state(config, subgraphs=subgraphs)
                if (
                    result.metadata is not None
                    and "checkpoint_ns" in result.metadata
                    and result.metadata["checkpoint_ns"] == ""
                ):
                    result.metadata.pop("checkpoint_ns")
                return result

        @staticmethod
        async def post(
            conn: PgConnectionProto,
            config: dict,
            values: Any = None,
            as_node: str | None = None,
            ctx: Any = None,
        ):
            from langgraph_api.graph import get_graph
            from langgraph_api.schema import ThreadUpdateResponse
            from langgraph_api.serde import json_dumpb
            from langgraph_api.state import (
                state_snapshot_to_thread_state,
            )
            from langgraph_api.store import get_store
            from langgraph_api.utils import fetchone
            from langgraph_sdk import Auth

            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            filters = await Threads.handle_event(
                ctx,
                "update",
                Auth.types.ThreadsUpdate(thread_id=thread_id),
            )
            checkpointer = await _get_checkpointer()
            thread = await fetchone(
                await Threads.get(conn, thread_id, ctx=ctx),
                not_found_detail=f"Thread {thread_id} not found.",
            )
            if filters and not _check_filter_match(thread.get("metadata") or {}, filters):
                raise HTTPException(status_code=403, detail="Forbidden")

            if await _thread_has_inflight_work(conn.session, thread_id):
                raise HTTPException(
                    status_code=409,
                    detail=f"Thread {thread_id} has in-flight runs",
                )

            metadata = thread.get("metadata") or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }
            graph_id = metadata.get("graph_id")
            if not graph_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Thread '{thread_id}' has no assigned graph ID. "
                        "Make a run first or set metadata.graph_id."
                    ),
                )

            config["configurable"].setdefault("graph_id", graph_id)
            if hasattr(checkpointer, "latest_iter"):
                checkpointer.latest_iter = await checkpointer.aget(config)
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.update",
            ) as graph:
                update_config = config.copy()
                update_config["configurable"] = {
                    **config["configurable"],
                    "checkpoint_ns": config["configurable"].get("checkpoint_ns", ""),
                }
                next_config = await graph.aupdate_state(update_config, values, as_node=as_node)
                state = await Threads.State.get(conn, config, subgraphs=False, ctx=ctx)
                await Threads.set_status(
                    conn,
                    thread_id,
                    {
                        "next": list(state.next),
                        "values": state.values,
                        "tasks": [
                            {"id": t.id, "interrupts": list(t.interrupts)} for t in state.tasks
                        ],
                    },
                    None,
                )
                await Threads.Stream.publish(
                    thread_id,
                    "state_update",
                    json_dumpb(
                        {
                            "state": state_snapshot_to_thread_state(state),
                            "thread_id": str(thread_id),
                        }
                    ),
                )
                return ThreadUpdateResponse(
                    checkpoint=next_config["configurable"],
                    configurable=next_config["configurable"],
                    checkpoint_id=next_config["configurable"]["checkpoint_id"],
                )

        @staticmethod
        async def bulk(
            conn: PgConnectionProto,
            *,
            config: dict,
            supersteps: Sequence[dict],
            ctx: Any = None,
        ):
            """Apply a batch of state updates."""
            from langgraph.types import StateUpdate
            from langgraph_api.command import map_cmd
            from langgraph_api.graph import get_graph
            from langgraph_api.schema import ThreadUpdateResponse
            from langgraph_api.serde import json_dumpb
            from langgraph_api.state import (
                state_snapshot_to_thread_state,
            )
            from langgraph_api.store import get_store
            from langgraph_api.utils import fetchone
            from langgraph_sdk import Auth

            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            filters = await Threads.handle_event(
                ctx,
                "update",
                Auth.types.ThreadsUpdate(thread_id=thread_id),
            )
            thread = await fetchone(
                await Threads.get(conn, thread_id, ctx=ctx),
                not_found_detail=f"Thread {thread_id} not found.",
            )
            if filters and not _check_filter_match(thread.get("metadata") or {}, filters):
                raise HTTPException(status_code=403, detail="Forbidden")

            if await _thread_has_inflight_work(conn.session, thread_id):
                raise HTTPException(
                    status_code=409,
                    detail=f"Thread {thread_id} has in-flight runs",
                )

            metadata = thread.get("metadata") or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }
            graph_id = metadata.get("graph_id")
            if not graph_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Thread '{thread_id}' has no assigned graph ID. "
                        "Make a run first or set metadata.graph_id."
                    ),
                )

            config["configurable"].setdefault("graph_id", graph_id)
            config["configurable"].setdefault("checkpoint_ns", "")
            checkpointer = await _get_checkpointer()
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.update",
            ) as graph:
                next_config = await graph.abulk_update_state(
                    config,
                    [
                        [
                            StateUpdate(
                                (
                                    map_cmd(update.get("command"))
                                    if update.get("command")
                                    else update.get("values")
                                ),
                                update.get("as_node"),
                            )
                            for update in superstep.get("updates", [])
                        ]
                        for superstep in supersteps
                    ],
                )
                state = await Threads.State.get(conn, config, subgraphs=False, ctx=ctx)
                await Threads.set_status(
                    conn,
                    thread_id,
                    {
                        "next": list(state.next),
                        "values": state.values,
                        "tasks": [
                            {"id": t.id, "interrupts": list(t.interrupts)} for t in state.tasks
                        ],
                    },
                    None,
                )
                await Threads.Stream.publish(
                    thread_id,
                    "state_update",
                    json_dumpb(
                        {
                            "state": state_snapshot_to_thread_state(state),
                            "thread_id": str(thread_id),
                        }
                    ),
                )
                return ThreadUpdateResponse(checkpoint=next_config["configurable"])

        @staticmethod
        async def list(
            conn: PgConnectionProto,
            *,
            config: dict,
            limit: int = 1,
            before: Any = None,
            metadata: dict | None = None,
            ctx: Any = None,
        ) -> list:
            from langgraph_api.graph import get_graph
            from langgraph_api.store import get_store
            from langgraph_api.utils import fetchone

            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            thread = await fetchone(await Threads.get(conn, thread_id, ctx=ctx))
            thread_metadata = thread.get("metadata") or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }
            graph_id = thread_metadata.get("graph_id")
            if not graph_id:
                return []

            checkpointer = await _get_checkpointer()
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.read",
            ) as graph:
                before_param = (
                    {"configurable": {"checkpoint_id": before}}
                    if isinstance(before, str)
                    else before
                )
                return [
                    state
                    async for state in graph.aget_state_history(
                        config, limit=limit, filter=metadata, before=before_param
                    )
                ]

    class Stream(Authenticated):
        resource = "threads"

        @staticmethod
        async def subscribe(
            conn: PgConnectionProto,
            thread_id: UUID,
            seen_runs: set[UUID],
            *,
            after_id: str | None = None,
            replay: bool = True,
        ):
            sm = get_stream_manager()
            queues = []
            thread_id = _ensure_uuid(thread_id)
            if thread_id not in seen_runs:
                q = await sm.add_thread_stream(thread_id)
                queues.append((thread_id, q))
                seen_runs.add(thread_id)
            result = await conn.session.execute(
                sa_select(RunRow).where(RunRow.thread_id == thread_id)
            )
            for run in result.scalars():
                rid = run.run_id
                if rid not in seen_runs:
                    q = await sm.add_queue(rid, thread_id, after_id=after_id, replay=replay)
                    queues.append((rid, q))
                    seen_runs.add(rid)
            return queues

        @staticmethod
        async def join(
            thread_id: UUID,
            *,
            last_event_id: str | None = None,
            stream_modes: list | None = None,
            ctx: Any = None,
        ) -> AsyncIterator[tuple[bytes, bytes, bytes | None]]:
            async for event, payload, stream_id, _run_id in Threads.Stream.join_event_streaming(
                thread_id,
                last_event_id=last_event_id,
                stream_modes=stream_modes or [],
                ctx=ctx,
            ):
                yield event, payload, stream_id

        @staticmethod
        async def join_event_streaming(
            thread_id: UUID,
            *,
            last_event_id: str | None = None,
            stream_modes: list | None = None,
            ctx: Any = None,
        ) -> AsyncIterator[tuple[bytes, bytes, bytes | None, str | None]]:
            """Stream thread output (Protocol v3)."""
            await Threads.Stream.check_thread_stream_auth(thread_id, ctx)
            from langgraph_api.utils.stream_codec import (
                decode_stream_message,
            )

            stream_modes = list(stream_modes or [])

            def should_filter_event(event_name: str, message_bytes: bytes) -> bool:
                if "run_modes" in stream_modes and event_name != "state_update":
                    return False
                if "state_update" in stream_modes and event_name == "state_update":
                    return False
                if "lifecycle" in stream_modes and event_name == "metadata":
                    try:
                        message_data = orjson.loads(message_bytes)
                        if message_data.get("status") == "run_done":
                            return False
                        if "attempt" in message_data and "run_id" in message_data:
                            return False
                    except (orjson.JSONDecodeError, TypeError):
                        pass
                return True

            stream_manager = get_stream_manager()
            seen_runs: set[UUID] = set()
            created_queues: list[tuple[UUID, asyncio.Queue]] = []
            thread_id = _ensure_uuid(thread_id)
            # "-" restores from Redis Streams; local replay is empty on a cold replica.
            from_beginning = last_event_id in ("-", "")
            resume_cursor = None if last_event_id is None or from_beginning else last_event_id
            restore_cursor = "-" if from_beginning else resume_cursor
            emitted_ids: set[str] = set()

            def _accept(message: Message) -> bool:
                """True if this frame should be emitted (cursor + dedup)."""
                if not message.id:
                    return True
                mid = message.id.decode() if isinstance(message.id, bytes) else str(message.id)
                if mid in emitted_ids:
                    return False
                if resume_cursor is not None and not ms_seq_id_gt(mid, resume_cursor):
                    return False
                emitted_ids.add(mid)
                return True

            try:
                await logger.ainfo("Joined thread stream", thread_id=str(thread_id))

                # Dedup restore vs later add_queue replay via emitted_ids.
                if restore_cursor is not None:
                    store_key = thread_id
                    all_events = []
                    run_ids: set[UUID] = set(
                        stream_manager.message_stores.get(store_key, {}).keys()
                    )
                    async with connect() as conn:
                        result = await conn.session.execute(
                            sa_select(RunRow.run_id).where(RunRow.thread_id == thread_id)
                        )
                        run_ids.update(row[0] for row in result.all())
                    for run_id in run_ids:
                        for message in await stream_manager.restore_messages_async(
                            run_id, thread_id, restore_cursor
                        ):
                            all_events.append((message, run_id))
                    all_events.sort(key=lambda x: ms_seq_id_sort_key((x[0].id or b"").decode()))
                    for message, run_id in all_events:
                        if not _accept(message):
                            continue
                        try:
                            decoded = decode_stream_message(message.data, channel=message.topic)
                        except (ValueError, KeyError):
                            continue
                        event_bytes = decoded.event_bytes
                        message_bytes = decoded.message_bytes
                        if event_bytes == b"control" and message_bytes == b"done":
                            event_bytes = b"metadata"
                            message_bytes = orjson.dumps(
                                {"status": "run_done", "run_id": str(run_id)}
                            )
                        if not should_filter_event(event_bytes.decode("utf-8"), message_bytes):
                            yield (
                                event_bytes,
                                message_bytes,
                                message.id,
                                str(run_id),
                            )

                # Do not open connect() every drain tick — dual SSE connections exhaust the pool.
                last_subscribe_mono = 0.0
                subscribe_interval = 0.15
                while True:
                    now_mono = time.monotonic()
                    if now_mono - last_subscribe_mono >= subscribe_interval or not created_queues:
                        async with connect() as conn:
                            new_queue_tuples = await Threads.Stream.subscribe(
                                conn,
                                thread_id,
                                seen_runs,
                                after_id=resume_cursor,
                            )
                        for run_id, queue in new_queue_tuples:
                            created_queues.append((run_id, queue))
                        last_subscribe_mono = now_mono

                    drained_any = False
                    for run_id, queue in created_queues:
                        while True:
                            try:
                                message = queue.get_nowait()
                            except asyncio.QueueEmpty:
                                break
                            if not _accept(message):
                                continue
                            try:
                                decoded = decode_stream_message(message.data, channel=message.topic)
                            except (ValueError, KeyError):
                                continue
                            event = decoded.event_bytes
                            event_name = event.decode("utf-8")
                            payload = decoded.message_bytes

                            if event == b"control" and payload == b"done":
                                topic = (
                                    message.topic.decode()
                                    if isinstance(message.topic, bytes)
                                    else message.topic
                                )
                                done_run_id = topic.split("run:")[1].split(":")[0]
                                meta_event = b"metadata"
                                meta_payload = orjson.dumps(
                                    {"status": "run_done", "run_id": done_run_id}
                                )
                                if not should_filter_event("metadata", meta_payload):
                                    yield (
                                        meta_event,
                                        meta_payload,
                                        message.id,
                                        done_run_id,
                                    )
                                    drained_any = True
                            elif not should_filter_event(event_name, payload):
                                yield (
                                    event,
                                    payload,
                                    message.id,
                                    str(run_id),
                                )
                                drained_any = True
                                await asyncio.sleep(0)

                    if drained_any:
                        await asyncio.sleep(0)
                    else:
                        await asyncio.sleep(0.02)
            except WrappedHTTPException as e:
                raise e.http_exception from None
            except asyncio.CancelledError:
                await logger.awarning(
                    "Thread stream client disconnected",
                    thread_id=str(thread_id),
                )
                raise
            finally:
                for key, queue in created_queues:
                    try:
                        if key == thread_id:
                            await stream_manager.remove_thread_stream(thread_id, queue)
                        else:
                            await stream_manager.remove_queue(key, thread_id, queue)
                    except Exception:
                        pass

        @staticmethod
        async def publish(thread_id: UUID | str, event: str, message: bytes) -> None:
            from langgraph_api.utils.stream_codec import STREAM_CODEC

            topic = f"thread:{thread_id}:stream".encode()
            sm = get_stream_manager()
            payload = STREAM_CODEC.encode(event, message)
            await sm.put_thread(_ensure_uuid(thread_id), Message(topic=topic, data=payload))

        @staticmethod
        async def check_thread_stream_auth(
            thread_id: UUID,
            ctx: Any = None,
        ) -> None:
            from langgraph_sdk import Auth

            async with connect() as conn:
                filters = await Threads.Stream.handle_event(
                    ctx,
                    "read",
                    Auth.types.ThreadsRead(thread_id=thread_id),
                )
                if filters:
                    try:
                        thread = await anext(await Threads.get(conn, thread_id, ctx=ctx))
                    except (HTTPException, StopAsyncIteration):
                        thread = None
                    if not thread or not _check_filter_match(thread.get("metadata") or {}, filters):
                        raise HTTPException(status_code=404, detail="Thread not found")


class Runs(Authenticated):
    resource = "threads"

    @staticmethod
    async def stats(conn: PgConnectionProto) -> dict:
        pending = int(
            await conn.session.scalar(
                sa_select(func.count()).select_from(RunRow).where(RunRow.status == "pending")
            )
            or 0
        )
        running = int(
            await conn.session.scalar(
                sa_select(func.count()).select_from(RunRow).where(RunRow.status == "running")
            )
            or 0
        )
        return {
            "n_pending": pending,
            "n_running": running,
            "pending_runs_wait_time_max_secs": None,
            "pending_runs_wait_time_med_secs": None,
            "pending_unblocked_runs_wait_time_max_secs": None,
        }

    @staticmethod
    async def pool_stats() -> dict:
        """Connection-pool stats for /info and self-hosted metrics."""
        return {}

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        kwargs: dict,
        *,
        thread_id: UUID | None = None,
        user_id: str | None = None,
        run_id: UUID | None = None,
        status: str | None = "pending",
        metadata: dict | None = None,
        prevent_insert_if_inflight: bool = False,
        multitask_strategy: str = "reject",
        if_not_exists: str = "reject",
        after_seconds: int = 0,
        ctx: Any = None,
        langsmith_session_name: str | None = None,
        **_: Any,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        thread_id = _ensure_uuid(thread_id) if thread_id else None
        run_id = _ensure_uuid(run_id) if run_id else uuid4()
        metadata = metadata or {}
        config = kwargs.get("config", {})
        temporary = bool(kwargs.get("temporary", False))

        filters = await Runs.handle_event(
            ctx,
            "create_run",
            Auth.types.RunsCreate(
                thread_id=None if temporary else thread_id,
                assistant_id=assistant_id,
                run_id=run_id,
                status=cast(Any, status),
                metadata=metadata,
                prevent_insert_if_inflight=prevent_insert_if_inflight,
                multitask_strategy=cast(Any, multitask_strategy),
                if_not_exists=cast(Any, if_not_exists),
                after_seconds=after_seconds,
                kwargs=kwargs,
            ),
        )

        assistant = await _aget_assistant(conn.session, assistant_id)
        if not assistant:
            return _empty_aiter()
        if (assistant.metadata_ or {}).get("created_by") != "system":
            assistant_filters = await Assistants.handle_event(
                ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
            )
            if _auth_denies(assistant.metadata_, assistant_filters):
                return _empty_aiter()

        existing_thread = await _aget_thread(conn.session, thread_id) if thread_id else None
        if existing_thread and _auth_denies(existing_thread.metadata_, filters):
            return _empty_aiter()

        created_thread = False
        if not existing_thread and (thread_id is None or if_not_exists == "create"):
            if thread_id is None:
                thread_id = uuid4()
            now = datetime.now(UTC)
            thread_meta = {
                "graph_id": assistant.graph_id,
                "assistant_id": str(assistant_id),
                **(config.get("metadata") or {}),
                **metadata,
            }
            thread_config = _merge_jsonb(
                assistant.config or {},
                config,
                {
                    "configurable": _merge_jsonb(
                        (assistant.config or {}).get("configurable") or {},
                    )
                },
            )
            ins = (
                pg_insert(ThreadRow)
                .values(
                    thread_id=thread_id,
                    status="busy",
                    metadata_=thread_meta,
                    config=thread_config,
                    created_at=now,
                    updated_at=now,
                    state_updated_at=now,
                    values_=None,
                    interrupts={},
                )
                .on_conflict_do_nothing(index_elements=["thread_id"])
            )
            result = await conn.session.execute(ins)
            if result.rowcount == 0:
                existing_thread = await _aget_thread(conn.session, thread_id)
            else:
                await conn.session.flush()
                existing_thread = await _aget_thread(conn.session, thread_id)
                created_thread = True
            if existing_thread is None:
                return _empty_aiter()
        elif not existing_thread:
            return _empty_aiter()

        # Lock before inflight check + insert so concurrent reject/prevent_insert serialize; also promote busy after create-conflict.
        locked_thread = (
            await conn.session.execute(
                sa_select(ThreadRow).where(ThreadRow.thread_id == thread_id).with_for_update()
            )
        ).scalar_one_or_none()
        if locked_thread is None:
            return _empty_aiter()
        existing_thread = locked_thread
        # Re-check auth after lock/conflict: another principal may own the thread.
        if not created_thread and _auth_denies(existing_thread.metadata_, filters):
            return _empty_aiter()

        if not created_thread and existing_thread.status != "busy":
            existing_thread.status = "busy"
            existing_thread.metadata_ = _merge_jsonb(
                existing_thread.metadata_ or {},
                {
                    "graph_id": assistant.graph_id,
                    "assistant_id": str(assistant_id),
                },
            )
            # Thread configurable from assistant+thread only; caller configurable goes on run kwargs later.
            existing_thread.config = _merge_jsonb(
                assistant.config or {},
                existing_thread.config or {},
                config,
                {
                    "configurable": _merge_jsonb(
                        (assistant.config or {}).get("configurable") or {},
                        (existing_thread.config or {}).get("configurable") or {},
                    )
                },
            )
            existing_thread.updated_at = datetime.now(UTC)

        inflight_rows = list(
            (
                await conn.session.execute(
                    sa_select(RunRow).where(
                        RunRow.thread_id == thread_id,
                        RunRow.status.in_(("pending", "running")),
                    )
                )
            ).scalars()
        )
        inflight = [run_to_dict(r) for r in inflight_rows]
        if prevent_insert_if_inflight and inflight:

            async def _inflight():
                for r in inflight:
                    yield r

            return _inflight()

        # Merge (do not replace) caller configurable — dropping __event_streaming_v2 disables the messages/tools stream path.
        incoming_configurable = dict(config.get("configurable") or {})
        thread_configurable = dict(
            (existing_thread.config or {}).get("configurable") or {} if existing_thread else {}
        )
        assistant_configurable = dict((assistant.config or {}).get("configurable") or {})
        configurable = {
            **assistant_configurable,
            **thread_configurable,
            **incoming_configurable,
            "run_id": str(run_id),
            "thread_id": str(thread_id),
            "graph_id": assistant.graph_id,
            "assistant_id": str(assistant_id),
            "user_id": (
                incoming_configurable.get("user_id")
                or thread_configurable.get("user_id")
                or assistant_configurable.get("user_id")
                or user_id
            ),
        }
        merged_metadata = {
            **(assistant.metadata_ or {}),
            **((existing_thread.metadata_ or {}) if existing_thread else {}),
            **(config.get("metadata") or {}),
            **metadata,
            "assistant_id": str(assistant_id),
        }

        now = datetime.now(UTC)
        effective_status = status or "pending"
        row = RunRow(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            metadata_=merged_metadata,
            status=effective_status,
            kwargs={
                **kwargs,
                "config": {
                    **(assistant.config or {}),
                    **config,
                    "configurable": configurable,
                    "metadata": merged_metadata,
                },
                "context": {
                    **(assistant.context or {}),
                    **(kwargs.get("context") or {}),
                },
            },
            multitask_strategy=multitask_strategy,
            # Postgres clock for created_at so Runs.next is immune to host/container skew.
            updated_at=now,
        )
        conn.session.add(row)
        await conn.session.flush()
        await conn.session.execute(
            text(
                "UPDATE runs SET created_at = now() + make_interval(secs => :secs) "
                "WHERE run_id = :run_id"
            ),
            {"secs": int(after_seconds or 0), "run_id": run_id},
        )
        await conn.session.refresh(row)
        new_run = run_to_dict(row)

        if effective_status == "pending":
            # Wake after connect() commits so workers cannot claim a run whose thread is still invisible.
            conn.schedule_after_commit(wake_run_queue)

        async def _yield():
            yield new_run
            for r in inflight:
                yield r

        return _yield()

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        run_id: UUID | str,
        *,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        run_id = _ensure_uuid(run_id)
        thread_id = _ensure_uuid(thread_id)
        filters = await Runs.handle_event(ctx, "read", Auth.types.ThreadsRead(thread_id=thread_id))
        # Eager load — API may drain the iterator after connect() closes.
        row = await _aget_run(conn.session, run_id)
        data = None
        if row is not None and row.thread_id == thread_id:
            if filters:
                thread = await _aget_thread(conn.session, thread_id)
                if thread is not None and not _auth_denies(thread.metadata_, filters):
                    data = run_to_dict(row)
            else:
                data = run_to_dict(row)

        async def _yield():
            if data is not None:
                yield data

        return _yield()

    @staticmethod
    async def delete(
        conn: PgConnectionProto,
        run_id: UUID | str,
        *,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        run_id = _ensure_uuid(run_id)
        thread_id = _ensure_uuid(thread_id)
        filters = await Runs.handle_event(
            ctx,
            "delete",
            Auth.types.ThreadsDelete(run_id=run_id, thread_id=thread_id),
        )
        row = await _aget_run(conn.session, run_id)
        if row is None or row.thread_id != thread_id:
            raise HTTPException(status_code=404, detail="Run not found")
        if filters:
            thread = await _aget_thread(conn.session, thread_id)
            if thread is None or _auth_denies(thread.metadata_, filters):
                raise HTTPException(status_code=404, detail="Run not found")
        await _adelete_retry_counters(conn.session, [run_id])
        await conn.session.delete(row)
        await conn.session.flush()

        async def _yield():
            yield run_id

        return _yield()

    @staticmethod
    async def cancel(
        conn: PgConnectionProto,
        run_ids: Sequence[UUID | str] | None = None,
        *,
        action: str = "interrupt",
        thread_id: UUID | None = None,
        status: str | None = None,
        assistant_id: UUID | None = None,
        ctx: Any = None,
    ) -> None:
        from langgraph_sdk import Auth

        # Mutually exclusive filter modes so callers cannot accidentally broaden a cancel.
        if assistant_id is not None:
            if thread_id is not None or run_ids is not None or status is not None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Cannot specify 'thread_id', 'run_ids', or 'status' "
                        "when using 'assistant_id'"
                    ),
                )
            assistant_id = _ensure_uuid(assistant_id)
        elif status is not None:
            if thread_id is not None or run_ids is not None:
                raise HTTPException(
                    status_code=422,
                    detail="Cannot specify 'thread_id' or 'run_ids' when using 'status'",
                )
        else:
            if thread_id is None or run_ids is None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Must provide either a status, an assistant_id, "
                        "or both 'thread_id' and 'run_ids'"
                    ),
                )

        if run_ids is not None:
            run_ids = [_ensure_uuid(rid) for rid in run_ids]
        if thread_id is not None:
            thread_id = _ensure_uuid(thread_id)

        filters = await Runs.handle_event(
            ctx,
            "update",
            Auth.types.ThreadsUpdate(
                thread_id=thread_id,  # type: ignore[typeddict-item]
                action=cast(Any, action),
                metadata={"run_ids": run_ids, "status": status},
            ),
        )
        q = sa_select(RunRow)
        if assistant_id is not None:
            q = q.where(
                RunRow.assistant_id == assistant_id,
                RunRow.status.in_(("pending", "running")),
            )
        elif status is not None:
            if status == "all":
                statuses: tuple[str, ...] = ("pending", "running")
            elif status in ("pending", "running"):
                statuses = (status,)
            else:
                raise HTTPException(
                    status_code=422,
                    detail="Invalid status: must be 'pending', 'running', or 'all'",
                )
            q = q.where(RunRow.status.in_(statuses))
        else:
            q = q.where(
                RunRow.run_id.in_(cast(Sequence[UUID], run_ids)),
                RunRow.thread_id == thread_id,
            )

        candidates = list((await conn.session.execute(q)).scalars())
        if filters:
            allowed: list[RunRow] = []
            for r in candidates:
                if r.thread_id is None:
                    continue
                thread = await _aget_thread(conn.session, r.thread_id)
                if thread is not None and not _auth_denies(thread.metadata_, filters):
                    allowed.append(r)
            candidates = allowed
        if not candidates and assistant_id is None:
            raise HTTPException(status_code=404, detail="No runs found to cancel")

        sm = get_stream_manager()
        now = datetime.now(UTC)
        affected_threads: set[UUID] = set()
        cancelled_any = False
        for r in candidates:
            # Re-lock and re-read so we cannot race Runs.next; SKIP LOCKED claimants wait/skip while we hold FOR UPDATE.
            locked = (
                await conn.session.execute(
                    sa_select(RunRow)
                    .where(
                        RunRow.run_id == r.run_id,
                        RunRow.status.in_(("pending", "running")),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if locked is None:
                continue
            cancelled_any = True
            was_pending = locked.status == "pending"
            run_thread_id = locked.thread_id
            control = Message(
                topic=f"run:{locked.run_id}:control".encode(),
                data=action.encode(),
            )
            await sm.put(locked.run_id, run_thread_id, control)
            # Delete pending on rollback only with no queues; otherwise interrupt so subscribers see a terminal status.
            queues = sm.get_queues(locked.run_id, run_thread_id)
            delete_pending = action == "rollback" and was_pending and not queues
            if delete_pending:
                await _adelete_retry_counters(conn.session, [locked.run_id])
                await conn.session.delete(locked)
            else:
                locked.status = "interrupted"
                locked.updated_at = now
            if was_pending and not queues:
                # Clear control keys for pending with no subscribers; keep buffers when queues exist so clients can drain.
                try:
                    await sm.clear_run_buffers(locked.run_id, run_thread_id, local_grace_secs=0.0)
                except Exception:
                    logger.debug("clear_run_buffers after pending cancel failed", exc_info=True)
            if run_thread_id is None:
                continue
            if was_pending:
                affected_threads.add(run_thread_id)
            else:
                # Only idle running→interrupted when the worker heartbeat is gone; else set_joint_status races State.post.
                alive = await has_run_heartbeat(locked.run_id)
                if alive is False:
                    affected_threads.add(run_thread_id)
        await conn.session.flush()

        # Clear busy when nothing in-flight remains (pending cancels; running only if heartbeat gone).
        for tid in affected_threads:
            pending = int(
                await conn.session.scalar(
                    sa_select(func.count())
                    .select_from(RunRow)
                    .where(
                        RunRow.thread_id == tid,
                        RunRow.status.in_(("pending", "running")),
                    )
                )
                or 0
            )
            if pending:
                continue
            thread = await _aget_thread(conn.session, tid)
            if thread is not None and thread.status == "busy":
                thread.status = "idle"
                thread.updated_at = now
        await conn.session.flush()

        # Wake even if thread stays busy so a pending multitask-interrupt run becomes claimable when heartbeat drops.
        if cancelled_any:
            try:
                await wake_run_queue()
            except Exception:
                logger.debug("wake_run_queue after cancel failed", exc_info=True)

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        *,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
        select: list | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Runs.handle_event(
            ctx,
            "search",
            Auth.types.ThreadsSearch(thread_id=thread_id, limit=limit, offset=offset),
        )
        if filters:
            thread = await _aget_thread(conn.session, thread_id)
            if thread is None or _auth_denies(thread.metadata_, filters):
                return _empty_aiter()

        q = sa_select(RunRow).where(RunRow.thread_id == thread_id)
        if status is not None:
            q = q.where(RunRow.status == status)
        q = q.order_by(RunRow.created_at.desc()).offset(offset).limit(limit)
        rows = list((await conn.session.execute(q)).scalars())
        # Materialize while session is open — API drains runs outside connect().
        items: list[dict] = []
        for r in rows:
            d = run_to_dict(r)
            items.append({k: v for k, v in d.items() if k in select} if select else d)

        async def _yield():
            for d in items:
                yield d

        return _yield()

    @staticmethod
    async def set_status(
        conn: PgConnectionProto,
        run_id: UUID | str,
        status: str,
    ) -> dict | None:
        run_id = _ensure_uuid(run_id)
        run = await _aget_run(conn.session, run_id)
        if run:
            run.status = status
            run.updated_at = datetime.now(UTC)
            await conn.session.flush()
            return run_to_dict(run)
        return None

    @staticmethod
    async def next(wait: bool = False, limit: int = 1) -> AsyncIterator:
        """Claim pending runs via FOR UPDATE SKIP LOCKED (multi-replica safe)."""
        from sqlalchemy.exc import IntegrityError

        from langgraph_runtime_pg.database import PgRetryCounter, get_session_factory

        if wait:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0)

        sf = get_session_factory()
        claimed = 0
        conflict_budget = max(limit * 8, 8)  # bound retries on a hot thread
        blocked_threads: list[UUID] = []  # skipped while a cancelled sibling still heartbeats
        while claimed < limit and conflict_budget > 0:
            run_id: UUID | None = None
            claimed_payload: tuple[dict[str, Any], int] | None = None
            try:
                async with sf() as session:
                    exclude_sql = ""
                    params: dict[str, Any] = {}
                    if blocked_threads:
                        placeholders = ", ".join(f":bt_{i}" for i in range(len(blocked_threads)))
                        exclude_sql = (
                            f"AND (r.thread_id IS NULL OR r.thread_id NOT IN ({placeholders}))"
                        )
                        params = {f"bt_{i}": t for i, t in enumerate(blocked_threads)}
                    result = await session.execute(
                        text(
                            f"""
                            UPDATE runs SET status = 'running', updated_at = now()
                            WHERE run_id = (
                                SELECT r.run_id FROM runs r
                                WHERE r.status = 'pending'
                                  AND r.created_at <= now()
                                  AND NOT EXISTS (
                                      SELECT 1 FROM runs r2
                                      WHERE r2.thread_id IS NOT NULL
                                        AND r2.thread_id = r.thread_id
                                        AND r2.status = 'running'
                                  )
                                  {exclude_sql}
                                ORDER BY r.created_at
                                FOR UPDATE SKIP LOCKED
                                LIMIT 1
                            )
                            RETURNING run_id
                            """
                        ),
                        params,
                    )
                    row = result.first()
                    if row is None:
                        break
                    run_id = row[0]
                    run_row = await session.get(RunRow, run_id)
                    if run_row is None:
                        await session.commit()
                        break
                    # Skip claim while a cancelled sibling still heartbeats (fail-closed on Redis None); only check non-terminal siblings to avoid stale-heartbeat starvation.
                    if run_row.thread_id is not None:
                        sibling_ids = list(
                            (
                                await session.execute(
                                    sa_select(RunRow.run_id).where(
                                        RunRow.thread_id == run_row.thread_id,
                                        RunRow.run_id != run_id,
                                        RunRow.status.in_(("running", "interrupted")),
                                    )
                                )
                            )
                            .scalars()
                            .all()
                        )
                        blocked_by_hb = False
                        for sid in sibling_ids:
                            if await has_run_heartbeat(sid) is not False:
                                blocked_by_hb = True
                                break
                        if blocked_by_hb:
                            run_row.status = "pending"
                            await session.commit()
                            blocked_threads.append(run_row.thread_id)
                            conflict_budget -= 1
                            continue
                    # Same transaction as the claim so a failed commit does not inflate attempts.
                    attempt = await PgRetryCounter(sf).increment(run_id, session=session)
                    run_dict = run_to_dict(run_row)
                    # Heartbeat before commit — otherwise sweep can reclaim and a second replica may double-execute.
                    await set_run_heartbeat(run_id)
                    try:
                        await session.commit()
                    except Exception:
                        await clear_run_heartbeat(run_id)
                        raise
                    claimed_payload = (run_dict, attempt)
                # Yield after the session closes so a slow consumer does not hold a pool connection.
                if claimed_payload is None:
                    break
                claimed += 1
                yield claimed_payload
            except IntegrityError:
                # Lost one-running-per-thread race — leave pending for a later claim.
                if run_id is not None:
                    await clear_run_heartbeat(run_id)
                conflict_budget -= 1
                continue

    @staticmethod
    @asynccontextmanager
    async def enter(
        run_id: UUID,
        thread_id: UUID | None,
        loop: asyncio.AbstractEventLoop,
        resumable: bool,
    ) -> AsyncIterator[Any]:
        """Enter a run, heartbeat + cancellation listen, signal done."""
        from langgraph_api.asyncio import SimpleTaskGroup, ValueEvent
        from langgraph_api.utils.stream_codec import STREAM_CODEC

        stream_manager = get_stream_manager()
        control_queue = await stream_manager.add_control_queue(run_id, thread_id)

        async def _heartbeat_loop() -> None:
            interval = heartbeat_refresh_interval_secs()
            while True:
                await set_run_heartbeat(run_id)
                await asyncio.sleep(interval)

        try:
            await set_run_heartbeat(run_id)
            async with SimpleTaskGroup(cancel=True, taskgroup_name="Runs.enter") as tg:
                done = ValueEvent()
                tg.create_task(_heartbeat_loop())
                tg.create_task(listen_for_cancellation(control_queue, run_id, thread_id, done))
                yield done
                control_message = Message(topic=f"run:{run_id}:control".encode(), data=b"done")
                await stream_manager.put(run_id, thread_id, control_message)
                stream_message = Message(
                    topic=f"run:{run_id}:stream".encode(),
                    data=STREAM_CODEC.encode("control", b"done"),
                )
                await stream_manager.put(run_id, thread_id, stream_message, resumable=resumable)
                await stream_manager.remove_control_queue(run_id, thread_id, control_queue)
        finally:
            await clear_run_heartbeat(run_id)
            try:
                await stream_manager.clear_run_buffers(run_id, thread_id)
            except Exception:
                logger.debug("clear_run_buffers failed", exc_info=True)

    @staticmethod
    async def sweep() -> list[UUID]:
        """Reclaim running runs whose Redis heartbeat has expired."""
        from langgraph_api import config

        from langgraph_runtime_pg.database import PgRetryCounter, get_session_factory

        sf = get_session_factory()
        reclaimed: list[UUID] = []
        max_retries = int(getattr(config, "BG_JOB_MAX_RETRIES", 3))
        retry_counter = PgRetryCounter(sf)

        async with sf() as session:
            result = await session.execute(
                sa_select(RunRow.run_id).where(RunRow.status == "running")
            )
            running_ids = [row[0] for row in result.all()]

        for run_id in running_ids:
            alive = await has_run_heartbeat(run_id)
            if alive is None:
                # Redis unavailable — do not reclaim (avoid false positives).
                continue
            if alive:
                continue

            attempts = await retry_counter.get(run_id)
            # Next claim would bump attempts; if that exceeds max, mark error instead of looping.
            new_status = "error" if attempts >= max_retries else "pending"
            async with sf() as session:
                result = await session.execute(
                    text(
                        """
                        UPDATE runs
                        SET status = :status, updated_at = now()
                        WHERE run_id = :run_id AND status = 'running'
                        RETURNING run_id, thread_id
                        """
                    ),
                    {"status": new_status, "run_id": run_id},
                )
                row = result.first()
                if row is not None and new_status == "error" and row[1] is not None:
                    # Terminal reclaim — do not leave the thread stuck busy.
                    tid = row[1]
                    pending = int(
                        await session.scalar(
                            sa_select(func.count())
                            .select_from(RunRow)
                            .where(
                                RunRow.thread_id == tid,
                                RunRow.status.in_(("pending", "running")),
                            )
                        )
                        or 0
                    )
                    if pending == 0:
                        thread = await _aget_thread(session, tid)
                        if thread is not None:
                            thread.status = "error"
                            thread.updated_at = datetime.now(UTC)
                            if thread.error is None:
                                thread.error = {
                                    "error": "RuntimeError",
                                    "message": "Run swept after exceeding max retries",
                                }
                await session.commit()
            if row is None:
                continue
            await clear_run_heartbeat(run_id)
            reclaimed.append(run_id)
            await logger.awarning(
                "Swept stale run",
                run_id=str(run_id),
                new_status=new_status,
                attempts=attempts,
            )

        # Idle busy threads left behind when a cancelled worker dies without set_joint_status.
        idled = False
        async with sf() as session:
            busy_threads = list(
                (await session.execute(sa_select(ThreadRow).where(ThreadRow.status == "busy")))
                .scalars()
                .all()
            )
            now = datetime.now(UTC)
            for thread in busy_threads:
                if await _thread_has_inflight_work(session, thread.thread_id):
                    continue
                thread.status = "idle"
                thread.updated_at = now
                idled = True
            if idled:
                await session.commit()

        if reclaimed or idled:
            try:
                await wake_run_queue()
            except Exception:
                logger.debug("wake_run_queue after sweep failed", exc_info=True)
        return reclaimed

    class Stream:
        @staticmethod
        async def subscribe(run_id: UUID, thread_id: UUID | None = None) -> ContextQueue:
            sm = get_stream_manager()
            return cast(ContextQueue, await sm.add_queue(_ensure_uuid(run_id), thread_id))

        @staticmethod
        async def join(
            run_id: UUID,
            *,
            stream_channel: asyncio.Queue,
            thread_id: UUID,
            ignore_404: bool = False,
            cancel_on_disconnect: bool = False,
            stream_mode: list | str | None = None,
            last_event_id: str | None = None,
            ctx: Any = None,
        ) -> AsyncIterator:
            """Stream run output using short-lived DB sessions (avoids pool exhaustion)."""
            from langgraph_api.asyncio import create_task
            from langgraph_api.serde import json_dumpb
            from langgraph_api.utils.stream_codec import (
                decode_stream_message,
            )

            queue = stream_channel
            resume_cursor = (
                last_event_id
                if last_event_id is not None and last_event_id not in ("-", "")
                else None
            )
            emitted_ids: set[str] = set()

            def _accept(message: Message) -> bool:
                if not message.id:
                    return True
                mid = message.id.decode() if isinstance(message.id, bytes) else str(message.id)
                if mid in emitted_ids:
                    return False
                if resume_cursor is not None and not ms_seq_id_gt(mid, resume_cursor):
                    return False
                emitted_ids.add(mid)
                return True

            try:
                try:
                    await Runs.Stream.check_run_stream_auth(run_id, thread_id, ctx)
                except HTTPException as e:
                    raise WrappedHTTPException(e) from None

                async with connect() as conn:
                    run_iter = await Runs.get(conn, run_id, thread_id=thread_id, ctx=ctx)
                    run = await anext(run_iter, None)

                for message in await get_stream_manager().restore_messages_async(
                    run_id, thread_id, last_event_id
                ):
                    if not _accept(message):
                        continue
                    data, mid = message.data, message.id
                    decoded = decode_stream_message(data, channel=message.topic)
                    mode = decoded.event_bytes.decode("utf-8")
                    payload = decoded.message_bytes
                    if mode == "control":
                        if payload == b"done":
                            return
                    elif _run_stream_mode_matches(mode, stream_mode):
                        yield mode.encode(), payload, mid

                while True:
                    try:
                        message = await asyncio.wait_for(queue.get(), timeout=0.5)
                        if not _accept(message):
                            continue
                        data, mid = message.data, message.id
                        decoded = decode_stream_message(data, channel=message.topic)
                        mode = decoded.event_bytes.decode("utf-8")
                        payload = decoded.message_bytes
                        if mode == "control":
                            if payload == b"done":
                                break
                        elif _run_stream_mode_matches(mode, stream_mode):
                            stream_id = (
                                mid if (run or {}).get("kwargs", {}).get("resumable") else None
                            )
                            yield mode.encode(), payload, stream_id
                    except TimeoutError:
                        async with connect() as conn:
                            run_iter = await Runs.get(conn, run_id, thread_id=thread_id, ctx=ctx)
                            run = await anext(run_iter, None)
                        if ignore_404 and run is None:
                            break
                        elif run is None:
                            yield (
                                b"error",
                                json_dumpb(HTTPException(status_code=404, detail="Run not found")),
                                None,
                            )
                            break
                        elif run["status"] not in ("pending", "running"):
                            break
            except WrappedHTTPException as e:
                raise e.http_exception from None
            except Exception:
                if cancel_on_disconnect:
                    create_task(cancel_run(thread_id, run_id))
                raise
            finally:
                await get_stream_manager().remove_queue(run_id, thread_id, queue)

        @staticmethod
        async def check_run_stream_auth(
            run_id: UUID,
            thread_id: UUID,
            ctx: Any = None,
        ) -> None:
            from langgraph_sdk import Auth

            del run_id  # auth is thread-scoped
            async with connect() as conn:
                filters = await Runs.handle_event(
                    ctx,
                    "read",
                    Auth.types.ThreadsRead(thread_id=thread_id),
                )
                if filters:
                    thread = await _aget_thread(conn.session, thread_id)
                    if thread is None or _auth_denies(thread.metadata_, filters):
                        raise HTTPException(status_code=404, detail="Thread not found")

        # Monotonic time of last custom publish; gives dual SSE consumers a beat before interrupt updates.
        _last_custom_publish_mono: float = 0.0

        @staticmethod
        async def publish(
            run_id: UUID | str,
            event: str,
            message: bytes,
            *,
            thread_id: UUID | str | None = None,
            resumable: bool = False,
        ) -> None:
            from langgraph_api.utils.stream_codec import STREAM_CODEC

            topic = f"run:{run_id}:stream".encode()
            sm = get_stream_manager()
            payload = STREAM_CODEC.encode(event, message)
            mode = event.split("|", 1)[0]

            # Brief yield after custom before interrupt-bearing values/updates so dual SSE does not close extension iterators early.
            if mode in ("updates", "values"):
                elapsed = time.monotonic() - Runs.Stream._last_custom_publish_mono
                if elapsed < 0.008:
                    await asyncio.sleep(0.008 - elapsed)

            await sm.put(run_id, thread_id, Message(topic=topic, data=payload), resumable)
            if mode == "custom" or mode.startswith("custom:"):
                Runs.Stream._last_custom_publish_mono = time.monotonic()


class Crons(Authenticated):
    resource = "crons"

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        *,
        payload: dict,
        schedule: str,
        cron_id: UUID | None = None,
        thread_id: UUID | None = None,
        end_time: datetime | None = None,
        metadata: dict | None = None,
        enabled: bool = True,
        timezone: str | None = None,
        on_run_completed: str | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        import croniter as croniter_mod
        from langgraph_api.graph import SYSTEM_ASSISTANT_IDS, get_assistant_id
        from langgraph_api.utils import get_auth_ctx, next_cron_date, uuid7
        from langgraph_sdk import Auth

        if not croniter_mod.croniter.is_valid(schedule):
            raise HTTPException(status_code=422, detail=f"Invalid cron schedule: '{schedule}'")

        ctx = ctx or get_auth_ctx()
        user_id = ctx.user.identity if ctx is not None else None
        cron_id = cron_id or uuid7()
        metadata = metadata or {}
        payload = dict(payload or {})
        config = dict(payload.get("config") or {})
        configurable = dict(config.get("configurable") or {})
        configurable["cron_id"] = str(cron_id)
        config["configurable"] = configurable
        payload["config"] = config
        assistant_id = _ensure_uuid(get_assistant_id(str(payload.get("assistant_id"))))
        payload["assistant_id"] = str(assistant_id)
        thread_uuid = _ensure_uuid(thread_id) if thread_id else None

        filters = await Crons.handle_event(
            ctx,
            "create",
            Auth.types.CronsCreate(
                payload=payload,
                schedule=schedule,
                cron_id=cron_id,
                thread_id=thread_uuid,
                end_time=end_time,
            ),
        )

        assistant = await _aget_assistant(conn.session, assistant_id)
        if assistant is None:
            raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found")
        if str(assistant_id) not in SYSTEM_ASSISTANT_IDS:
            assistant_filters = await Assistants.handle_event(
                ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
            )
            if _auth_denies(assistant.metadata_, assistant_filters):
                raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found")
        if thread_uuid is not None:
            thread = await _aget_thread(conn.session, thread_uuid)
            thread_filters = await Threads.handle_event(
                ctx, "read", Auth.types.ThreadsRead(thread_id=thread_uuid)
            )
            if thread is None or _auth_denies(thread.metadata_ if thread else None, thread_filters):
                raise HTTPException(status_code=404, detail=f"Thread '{thread_uuid}' not found")

        existing = await _aget_cron(conn.session, cron_id)
        if existing:
            if _auth_denies(existing.metadata_, filters):
                raise HTTPException(status_code=404, detail=f"Cron '{cron_id}' not found")

            data = cron_to_dict(existing)

            async def _yield_existing():
                yield data

            return _yield_existing()

        now = datetime.now(UTC)
        next_run = next_cron_date(schedule, now, timezone=timezone)
        row = CronRow(
            cron_id=cron_id,
            assistant_id=assistant_id,
            thread_id=thread_uuid,
            schedule=schedule,
            payload=payload,
            metadata_=metadata,
            next_run_date=next_run,
            end_time=end_time,
            user_id=user_id,
            timezone=timezone,
            on_run_completed=on_run_completed,
            enabled=enabled,
            created_at=now,
            updated_at=now,
        )
        conn.session.add(row)
        await conn.session.flush()
        data = cron_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def update(
        conn: PgConnectionProto,
        *,
        cron_id: UUID,
        schedule: str | None = None,
        end_time: datetime | None = None,
        enabled: bool | None = None,
        on_run_completed: str | None = None,
        payload: dict | None = None,
        metadata: dict | None = None,
        timezone: str | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        import croniter as croniter_mod
        from langgraph_api.utils import next_cron_date
        from langgraph_sdk import Auth

        has_updates = any(
            v is not None
            for v in [
                schedule,
                end_time,
                enabled,
                on_run_completed,
                payload,
                metadata,
                timezone,
            ]
        )
        if not has_updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        cron_id = _ensure_uuid(cron_id)
        filters = await Crons.handle_event(
            ctx,
            "update",
            Auth.types.CronsUpdate(cron_id=cron_id, payload=payload, schedule=schedule),
        )
        row = await _aget_cron(conn.session, cron_id)
        if row is None or _auth_denies(row.metadata_, filters):
            raise HTTPException(status_code=404, detail=f"Cron '{cron_id}' not found")

        if timezone is not None:
            row.timezone = timezone

        if schedule is not None:
            if not croniter_mod.croniter.is_valid(schedule):
                raise HTTPException(status_code=422, detail=f"Invalid cron schedule: '{schedule}'")
            row.schedule = schedule
            row.next_run_date = next_cron_date(schedule, datetime.now(UTC), timezone=row.timezone)
        elif timezone is not None:
            row.next_run_date = next_cron_date(row.schedule, datetime.now(UTC), timezone=timezone)

        if end_time is not None:
            row.end_time = end_time
        if enabled is not None:
            row.enabled = enabled
        if on_run_completed is not None:
            row.on_run_completed = on_run_completed
        if metadata is not None:
            row.metadata_ = {**(row.metadata_ or {}), **metadata}
        if payload is not None:
            existing_payload = dict(row.payload or {})
            merged = {**existing_payload, **payload}
            merged["assistant_id"] = existing_payload.get(
                "assistant_id", merged.get("assistant_id")
            )
            merged_config = dict(merged.get("config") or {})
            merged_configurable = dict(merged_config.get("configurable") or {})
            merged_configurable["cron_id"] = str(cron_id)
            merged_config["configurable"] = merged_configurable
            merged["config"] = merged_config
            row.payload = merged

        row.updated_at = datetime.now(UTC)
        await conn.session.flush()
        data = cron_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        cron_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        cron_id = _ensure_uuid(cron_id)
        filters = await Crons.handle_event(ctx, "read", Auth.types.CronsRead(cron_id=cron_id))
        # Eager load — API may drain the iterator after connect() closes.
        row = await _aget_cron(conn.session, cron_id)
        data = None
        if row is not None and not _auth_denies(row.metadata_, filters):
            data = copy.deepcopy(cron_to_dict(row))

        async def _yield():
            if data is not None:
                yield data

        return _yield()

    @staticmethod
    async def delete(
        conn: PgConnectionProto,
        cron_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        cron_id = _ensure_uuid(cron_id)
        filters = await Crons.handle_event(ctx, "delete", Auth.types.CronsDelete(cron_id=cron_id))
        row = await _aget_cron(conn.session, cron_id)
        deleted = row is not None and not _auth_denies(row.metadata_, filters)
        if deleted:
            await conn.session.delete(row)
            await conn.session.flush()

        async def _yield():
            if deleted:
                yield cron_id

        return _yield()

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        *,
        assistant_id: UUID | None = None,
        thread_id: UUID | None = None,
        enabled: bool | None = None,
        limit: int = 10,
        offset: int = 0,
        select: list | None = None,
        ctx: Any = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        metadata: dict | None = None,
    ) -> tuple[AsyncIterator, int | None]:
        from langgraph_sdk import Auth

        filters = await Crons.handle_event(
            ctx,
            "search",
            Auth.types.CronsSearch(
                assistant_id=assistant_id,
                thread_id=thread_id,
                limit=limit,
                offset=offset,
            ),
        )
        q = sa_select(CronRow)
        if assistant_id is not None:
            q = q.where(CronRow.assistant_id == _ensure_uuid(assistant_id))
        if thread_id is not None:
            q = q.where(CronRow.thread_id == _ensure_uuid(thread_id))
        if enabled is not None:
            q = q.where(CronRow.enabled == enabled)
        if metadata:
            q = q.where(CronRow.metadata_.contains(metadata))
        plain = _plain_metadata_filter(filters)
        if plain:
            q = q.where(CronRow.metadata_.contains(plain))

        sb = _resolve_sort_field(sort_by, _CRON_SORT_FIELDS, "created_at", raise_invalid=True)
        col = getattr(CronRow, sb)
        reverse = not (sort_order and sort_order.upper() == "ASC")
        q = q.order_by(col.desc() if reverse else col.asc())

        if filters and plain is None:
            rows = [
                r
                for r in (await conn.session.execute(q)).scalars()
                if not _auth_denies(r.metadata_, filters)
            ]
            page = rows[offset : offset + limit]
            cursor = offset + limit if len(rows) > offset + limit else None
        else:
            q = q.offset(offset).limit(limit + 1)
            rows = list((await conn.session.execute(q)).scalars())
            cursor = offset + limit if len(rows) > limit else None
            page = rows[:limit]

        # Materialize while session is open — API paginates outside connect().
        items: list[dict] = []
        for r in page:
            d = cron_to_dict(r)
            items.append({k: v for k, v in d.items() if k in select} if select else d)

        async def _iter():
            for d in items:
                yield d

        return _iter(), cursor

    @staticmethod
    async def count(
        conn: PgConnectionProto,
        *,
        assistant_id: UUID | None = None,
        thread_id: UUID | None = None,
        ctx: Any = None,
        metadata: dict | None = None,
    ) -> int:
        from langgraph_sdk import Auth

        filters = await Crons.handle_event(
            ctx,
            "search",
            Auth.types.CronsSearch(
                assistant_id=assistant_id, thread_id=thread_id, limit=0, offset=0
            ),
        )
        plain = _plain_metadata_filter(filters)
        if filters and plain is None:
            q = sa_select(CronRow)
            if assistant_id is not None:
                q = q.where(CronRow.assistant_id == _ensure_uuid(assistant_id))
            if thread_id is not None:
                q = q.where(CronRow.thread_id == _ensure_uuid(thread_id))
            if metadata:
                q = q.where(CronRow.metadata_.contains(metadata))
            rows = list((await conn.session.execute(q)).scalars())
            return sum(1 for r in rows if not _auth_denies(r.metadata_, filters))

        count_q = sa_select(func.count()).select_from(CronRow)
        if assistant_id is not None:
            count_q = count_q.where(CronRow.assistant_id == _ensure_uuid(assistant_id))
        if thread_id is not None:
            count_q = count_q.where(CronRow.thread_id == _ensure_uuid(thread_id))
        if metadata:
            count_q = count_q.where(CronRow.metadata_.contains(metadata))
        if plain:
            count_q = count_q.where(CronRow.metadata_.contains(plain))
        return int(await conn.session.scalar(count_q) or 0)

    @staticmethod
    async def next(conn: PgConnectionProto, ctx: Any = None) -> AsyncIterator:
        """Yield due crons, advancing next_run_date under SKIP LOCKED."""
        from langgraph_api.utils import next_cron_date

        del ctx
        now = datetime.now(UTC)
        # Claim due rows; advance next_run_date immediately so concurrent replicas skip them.
        result = await conn.session.execute(
            text(
                """
                SELECT cron_id FROM crons
                WHERE enabled = true
                  AND (end_time IS NULL OR end_time >= :now)
                  AND (next_run_date IS NULL OR next_run_date <= :now)
                ORDER BY next_run_date ASC NULLS FIRST
                FOR UPDATE SKIP LOCKED
                """
            ),
            {"now": now},
        )
        cron_ids = [row[0] for row in result.all()]
        for cron_id in cron_ids:
            row = await _aget_cron(conn.session, cron_id)
            if row is None:
                continue
            payload = cron_to_dict(row)
            # Advance before yield (commit happens when connect() exits).
            row.next_run_date = next_cron_date(row.schedule, now, timezone=row.timezone)
            row.updated_at = datetime.now(UTC)
            await conn.session.flush()
            yield {**payload, "now": now}

    @staticmethod
    async def set_next_run_date(
        conn: PgConnectionProto,
        cron_id: UUID,
        next_run_date: datetime,
        ctx: Any = None,
    ) -> None:
        cron_id = _ensure_uuid(cron_id)
        row = await _aget_cron(conn.session, cron_id)
        if row is None:
            return
        row.next_run_date = next_run_date
        row.updated_at = datetime.now(UTC)
        await conn.session.flush()


async def listen_for_cancellation(
    queue: asyncio.Queue, run_id: UUID, thread_id: UUID | None, done: Any
) -> None:
    """Listen for cancellation messages and set the done event accordingly."""
    from langgraph_api.errors import UserInterrupt, UserRollback

    stream_manager = get_stream_manager()

    # Hydrate from Redis so a cancel published before this worker subscribed is still observed.
    if control_key := await stream_manager.aget_control_key(run_id, thread_id):
        payload = control_key.data
        if payload == b"rollback":
            done.set(UserRollback())
        elif payload == b"interrupt":
            done.set(UserInterrupt())

    while not done.is_set():
        try:
            # Do not break on timeout — long runs stay quiet until interrupt/rollback/done.
            message = await asyncio.wait_for(queue.get(), timeout=240)
            payload = message.data
            if payload == b"rollback":
                done.set(UserRollback())
            elif payload == b"interrupt":
                done.set(UserInterrupt())
            elif payload == b"done":
                done.set()
                break
        except TimeoutError:
            continue


async def cancel_run(thread_id: UUID, run_id: UUID, ctx: Any = None) -> None:
    async with connect() as conn:
        await Runs.cancel(conn, [run_id], thread_id=thread_id, ctx=ctx)


__all__ = [
    "Assistants",
    "Crons",
    "Runs",
    "StreamHandler",
    "Threads",
]
