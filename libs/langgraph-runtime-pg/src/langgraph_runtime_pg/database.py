"""Postgres pool and connect() over SQLAlchemy async sessions."""

from __future__ import annotations

import asyncio
import inspect
import os
import ssl as ssl_module
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from langgraph_runtime_pg.models import Base, RetryCounterRow
from langgraph_runtime_pg.redis_stream import start_stream, stop_stream

logger = structlog.stdlib.get_logger(__name__)

_ENGINE = None
_SESSION_FACTORY: async_sessionmaker[AsyncSession] | None = None

# libpq SSL query keys — strip from URL; asyncpg only accepts ssl= connect arg.
_LIBPQ_SSL_QUERY_KEYS = frozenset(
    {
        "sslmode",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "sslcrl",
        "sslpassword",
        "channel_binding",
        "gssencmode",
    }
)


def get_database_uri() -> str:
    uri = os.environ.get("DATABASE_URI")
    if not uri:
        raise RuntimeError("DATABASE_URI is required for langgraph_runtime_pg")
    return uri


def to_psycopg_uri(uri: str | None = None) -> str:
    """Normalize DATABASE_URI to a psycopg-style ``postgresql://`` URL."""
    uri = uri or get_database_uri()
    for prefix in (
        "postgresql+asyncpg://",
        "postgres+asyncpg://",
        "postgresql+psycopg://",
        "postgresql+psycopg2://",
        "postgres+psycopg://",
        "postgres+psycopg2://",
        "postgresql://",
        "postgres://",
    ):
        if uri.startswith(prefix):
            return "postgresql://" + uri[len(prefix) :]
    if "://" in uri:
        raise ValueError(
            f"Unsupported DATABASE_URI scheme {uri.split('://', 1)[0]!r}; "
            "expected postgres/postgresql (optionally +asyncpg/+psycopg/+psycopg2)"
        )
    return uri


def to_async_sqlalchemy_uri(uri: str | None = None) -> str:
    """Normalize DATABASE_URI to ``postgresql+asyncpg://``."""
    uri = uri or get_database_uri()
    if uri.startswith("postgresql+asyncpg://"):
        return uri
    if uri.startswith("postgres+asyncpg://"):
        return "postgresql+asyncpg://" + uri[len("postgres+asyncpg://") :]
    bare = to_psycopg_uri(uri)
    if bare.startswith("postgresql://"):
        return "postgresql+asyncpg://" + bare.removeprefix("postgresql://")
    return bare


def asyncpg_engine_args(uri: str | None = None) -> tuple[str, dict[str, Any]]:
    """Return ``(async_uri, connect_args)`` with libpq sslmode translated for asyncpg."""
    async_uri = to_async_sqlalchemy_uri(uri)
    parts = urlsplit(async_uri)
    kept: list[tuple[str, str]] = []
    sslmode: str | None = None
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None
    sslpassword: str | None = None

    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        kl = key.lower()
        if kl == "sslmode":
            sslmode = value.lower()
        elif kl == "sslrootcert":
            sslrootcert = value
        elif kl == "sslcert":
            sslcert = value
        elif kl == "sslkey":
            sslkey = value
        elif kl == "sslpassword":
            sslpassword = value
        elif kl in _LIBPQ_SSL_QUERY_KEYS:
            continue
        else:
            kept.append((key, value))

    connect_args: dict[str, Any] = {}
    if sslmode in ("require", "verify-ca", "verify-full") or any((sslrootcert, sslcert, sslkey)):
        if sslmode == "require" and not sslrootcert and not sslcert and not sslkey:
            connect_args["ssl"] = True
        else:
            ctx = (
                ssl_module.create_default_context(cafile=sslrootcert)
                if sslrootcert
                else ssl_module.create_default_context()
            )
            if sslmode == "require":
                # libpq require encrypts without CA verification.
                ctx.check_hostname = False
                ctx.verify_mode = ssl_module.CERT_NONE
            if sslcert and sslkey:
                ctx.load_cert_chain(sslcert, keyfile=sslkey, password=sslpassword)
            connect_args["ssl"] = ctx
    elif sslmode == "disable":
        connect_args["ssl"] = False

    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))
    return clean, connect_args


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _SESSION_FACTORY is None:
        raise RuntimeError("Call start_pool() before get_session_factory()")
    return _SESSION_FACTORY


_ASSISTANT_KEYS = [
    "assistant_id",
    "graph_id",
    "name",
    "description",
    "config",
    "context",
    "metadata",
    "version",
    "created_at",
    "updated_at",
]

_ASSISTANT_VERSION_KEYS = [
    "assistant_id",
    "version",
    "graph_id",
    "config",
    "context",
    "metadata",
    "name",
    "description",
    "created_at",
]

_THREAD_KEYS = [
    "thread_id",
    "status",
    "metadata",
    "config",
    "values",
    "interrupts",
    "error",
    "created_at",
    "updated_at",
    "state_updated_at",
]

_RUN_KEYS = [
    "run_id",
    "thread_id",
    "assistant_id",
    "status",
    "metadata",
    "kwargs",
    "multitask_strategy",
    "created_at",
    "updated_at",
]

_CRON_KEYS = [
    "cron_id",
    "assistant_id",
    "thread_id",
    "schedule",
    "payload",
    "metadata",
    "next_run_date",
    "end_time",
    "user_id",
    "timezone",
    "on_run_completed",
    "enabled",
    "created_at",
    "updated_at",
]


def row_to_dict(row: Any, keys: list[str]) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for k in keys:
        attr = k + "_" if k in ("metadata", "values") else k
        val = getattr(row, attr, getattr(row, k, None))
        d[k] = val
    return d


def assistant_to_dict(r: Any) -> dict:
    return row_to_dict(r, _ASSISTANT_KEYS)


def assistant_version_to_dict(r: Any) -> dict:
    return row_to_dict(r, _ASSISTANT_VERSION_KEYS)


def thread_to_dict(r: Any) -> dict:
    return row_to_dict(r, _THREAD_KEYS)


def run_to_dict(r: Any) -> dict:
    return row_to_dict(r, _RUN_KEYS)


def cron_to_dict(r: Any) -> dict:
    return row_to_dict(r, _CRON_KEYS)


_INCREMENT_SQL = text(
    """
    INSERT INTO retry_counters (run_id, count) VALUES (:run_id, 1)
    ON CONFLICT (run_id) DO UPDATE SET count = retry_counters.count + 1
    RETURNING count
    """
)


class PgRetryCounter:
    """Async retry counter over the retry_counters table."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._sf = session_factory

    async def increment(self, run_id: UUID, *, session: AsyncSession | None = None) -> int:
        """Atomically bump the counter; pass ``session`` when already in a txn."""
        if session is not None:
            result = await session.execute(_INCREMENT_SQL, {"run_id": run_id})
            return int(result.scalar_one())
        async with self._sf() as own:
            result = await own.execute(_INCREMENT_SQL, {"run_id": run_id})
            count = int(result.scalar_one())
            await own.commit()
            return count

    async def get(self, run_id: UUID) -> int:
        async with self._sf() as session:
            row = await session.get(RetryCounterRow, run_id)
            return int(row.count) if row is not None else 0


class _LockedSession:
    """Serialize AsyncSession ops so asyncio.gather on one conn is safe."""

    def __init__(self, session: AsyncSession, lock: asyncio.Lock):
        self._session = session
        self._lock = lock
        self._wrapped: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        cached = self._wrapped.get(name)
        if cached is not None:
            return cached
        attr = getattr(self._session, name)
        if not callable(attr):
            return attr
        if not inspect.iscoroutinefunction(attr):
            return attr

        async def _locked(*args: Any, **kwargs: Any):
            async with self._lock:
                return await attr(*args, **kwargs)

        self._wrapped[name] = _locked
        return _locked


class PgConnectionProto:
    """Connection handle with AsyncSession + after-commit/rollback hooks."""

    def __init__(
        self,
        session: AsyncSession,
        retry_counter: PgRetryCounter,
        session_factory: async_sessionmaker[AsyncSession],
    ):
        self._raw_session = session
        self._lock = asyncio.Lock()
        self.session = _LockedSession(session, self._lock)
        self.retry_counter = retry_counter
        self.can_execute = False
        self._sf = session_factory
        self._after_commit: list = []
        self._after_rollback: list = []
        # Empty lists kept for callers that still introspect conn.store keys.
        self.store: dict[str, list] = {
            "assistants": [],
            "assistant_versions": [],
            "threads": [],
            "runs": [],
            "crons": [],
        }

    def schedule_after_commit(self, cb) -> None:
        """Run ``cb`` (async zero-arg) after the enclosing ``connect()`` commits."""
        self._after_commit.append(cb)

    def schedule_after_rollback(self, cb) -> None:
        """Run ``cb`` (async zero-arg) if the enclosing ``connect()`` rolls back."""
        self._after_rollback.append(cb)

    @asynccontextmanager
    async def pipeline(self):
        yield None

    async def execute(self, query: str, *args: Any, **kwargs: Any):
        return None

    async def commit(self) -> None:
        async with self._lock:
            await self._raw_session.commit()

    def clear(self) -> None:
        for k in self.store:
            self.store[k] = []


def _auto_migrate_enabled() -> bool:
    raw = os.environ.get("LG_RUNTIME_PG_AUTO_MIGRATE", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


async def start_pool() -> None:
    global _ENGINE, _SESSION_FACTORY
    from langgraph_runtime_pg.checkpoint import setup_checkpointer
    from langgraph_runtime_pg.migrate import upgrade_head
    from langgraph_runtime_pg.store import setup_store

    if _ENGINE is not None and _SESSION_FACTORY is not None:
        logger.info("PG pool already started")
        return
    if _ENGINE is not None or _SESSION_FACTORY is not None:
        await stop_pool()

    raw_uri = get_database_uri()
    # Set LG_RUNTIME_PG_AUTO_MIGRATE=false when migrations run as a pre-deploy job.
    if _auto_migrate_enabled():
        upgrade_head(raw_uri)
        logger.info("Postgres schema at Alembic head")
    pool_size = int(os.environ.get("PG_POOL_SIZE", "20"))
    max_overflow = int(os.environ.get("PG_MAX_OVERFLOW", "20"))
    engine_uri, connect_args = asyncpg_engine_args(raw_uri)
    engine = create_async_engine(
        engine_uri,
        echo=False,
        pool_pre_ping=True,
        pool_size=max(pool_size, 5),
        max_overflow=max(max_overflow, 0),
        connect_args=connect_args,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    # Assign globals first so stop_pool() can dispose if a dependent fails.
    try:
        _ENGINE = engine
        _SESSION_FACTORY = session_factory
        await setup_checkpointer()
        await setup_store()
        await start_stream()
    except Exception:
        await stop_pool()
        raise
    logger.info("PG pool started", uri=engine_uri.split("@")[-1])


async def stop_pool() -> None:
    global _ENGINE, _SESSION_FACTORY
    from langgraph_runtime_pg.checkpoint import teardown_checkpointer
    from langgraph_runtime_pg.store import teardown_store

    await stop_stream()
    await teardown_store()
    await teardown_checkpointer()
    if _ENGINE is not None:
        await _ENGINE.dispose()
        _ENGINE = None
    _SESSION_FACTORY = None
    logger.info("PG pool stopped")


@asynccontextmanager
async def connect(
    *, supports_core_api: bool = False, __test__: bool = False
) -> AsyncIterator[PgConnectionProto]:
    del __test__  # accepted for API parity; unused
    if _SESSION_FACTORY is None:
        raise RuntimeError("Call start_pool() before connect()")
    async with _SESSION_FACTORY() as session:
        proto = PgConnectionProto(
            session=session,
            retry_counter=PgRetryCounter(_SESSION_FACTORY),
            session_factory=_SESSION_FACTORY,
        )
        try:
            yield proto
            async with proto._lock:
                await session.commit()
        except Exception:
            async with proto._lock:
                await session.rollback()
            for cb in proto._after_rollback:
                try:
                    await cb()
                except Exception:
                    logger.debug("after_rollback callback failed", exc_info=True)
            raise
        else:
            for cb in proto._after_commit:
                try:
                    await cb()
                except Exception:
                    logger.debug("after_commit callback failed", exc_info=True)


async def healthcheck(*, check_db: bool = True) -> None:
    if check_db and _ENGINE is not None:
        async with _ENGINE.connect() as conn:
            await conn.execute(text("SELECT 1"))


def pool_stats(*args: Any, **kwargs: Any) -> dict[str, dict[str, int]]:
    return {}


# Checkpoint/store tables owned outside Base.metadata; truncate in tests too.
_EXTERNAL_TRUNCATE_TABLES = (
    "checkpoints",
    "checkpoint_blobs",
    "checkpoint_writes",
    "store",
)


async def truncate_all() -> None:
    """Truncate ORM + checkpoint/store tables (tests)."""
    if _ENGINE is None:
        return
    orm_tables = [t.name for t in reversed(Base.metadata.sorted_tables)]
    async with _ENGINE.begin() as conn:
        existing = {
            row[0]
            for row in (
                await conn.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
            ).all()
        }
        tables = [t for t in (*orm_tables, *_EXTERNAL_TRUNCATE_TABLES) if t in existing]
        if not tables:
            return
        await conn.execute(text(f"TRUNCATE TABLE {', '.join(tables)} CASCADE"))
