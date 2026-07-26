"""Shared fixtures. Postgres + Redis must already be up (compose / CI services)."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest

# libs/langgraph-runtime-pg/tests → repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
INTEGRATION_DIR = REPO_ROOT / ".tests" / "langgraph_api" / "libs" / "sdk-py" / "integration"
LANGGRAPH_JSON = INTEGRATION_DIR / "langgraph.json"


def _load_dotenv() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip())


_load_dotenv()
os.environ.setdefault("REDIS_URI", "redis://localhost:6379/0")
os.environ.setdefault(
    "DATABASE_URI",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/langgraph",
)


def _purge_runtime_modules() -> None:
    for key in list(sys.modules):
        if key.startswith("langgraph_runtime") or key.startswith("langgraph_api"):
            del sys.modules[key]


def _sdk_py_langserve_graphs() -> str:
    """LANGSERVE_GRAPHS JSON with absolute paths from sdk-py integration/langgraph.json."""
    if not LANGGRAPH_JSON.is_file():
        pytest.skip(
            "upstream sdk-py graphs missing — run ./scripts/test.sh "
            "(or sparse-checkout libs/sdk-py into .tests/langgraph_api) first"
        )
    raw = json.loads(LANGGRAPH_JSON.read_text())["graphs"]
    absolute = {}
    for name, spec in raw.items():
        path_part, _, export = spec.partition(":")
        resolved = (INTEGRATION_DIR / path_part).resolve()
        if not resolved.is_file():
            pytest.skip(f"sdk-py graph file missing: {resolved}")
        absolute[name] = f"{resolved}:{export}" if export else str(resolved)
    return json.dumps(absolute)


@pytest.fixture
async def pg_runtime():
    """Fresh PG pool + empty tables for ops / stream tests."""
    os.environ["LANGGRAPH_RUNTIME_EDITION"] = "pg"
    os.environ["LG_RUNTIME_PG_TEST"] = "1"

    from langgraph_runtime_pg.database import start_pool, stop_pool, truncate_all
    from langgraph_runtime_pg.store import reset_store

    await start_pool()
    await truncate_all()
    reset_store()
    try:
        yield
    finally:
        reset_store()
        await truncate_all()
        await stop_pool()


@pytest.fixture
async def api_lifespan_no_queue(tmp_path: Path):
    """langgraph_api lifespan with graphs loaded and N_JOBS=0 (queue off)."""
    from asgi_lifespan import LifespanManager

    _purge_runtime_modules()

    os.environ["LANGGRAPH_RUNTIME_EDITION"] = "pg"
    os.environ["LG_RUNTIME_PG_TEST"] = "1"
    os.environ["LANGSERVE_GRAPHS"] = _sdk_py_langserve_graphs()
    os.environ["REDIS_URI"] = os.environ.get("REDIS_URI", "redis://localhost:6379/0")
    os.environ["N_JOBS_PER_WORKER"] = "0"
    os.environ["LANGGRAPH_ALLOW_BLOCKING"] = "true"
    os.environ["LG_BG_JOB_HEARTBEAT"] = "2"
    os.environ.pop("LANGGRAPH_AUTH", None)
    os.environ.setdefault("BG_JOB_ISOLATED_LOOPS", "false")

    prev_cwd = os.getcwd()
    work = tmp_path / "work"
    work.mkdir(parents=True, exist_ok=True)
    os.chdir(work)

    try:
        from langgraph_api.server import app

        async with LifespanManager(app, startup_timeout=60, shutdown_timeout=15):
            from langgraph_runtime_pg.database import truncate_all
            from langgraph_runtime_pg.store import reset_store

            await truncate_all()
            reset_store()
            yield
            reset_store()
            await truncate_all()
    finally:
        os.chdir(prev_cwd)
        _purge_runtime_modules()


BASE_URL = os.environ.get("LANGGRAPH_INTEGRATION_URL", "http://localhost:2024")


@pytest.fixture
def live_api() -> str:
    """Skip unless langgraph-api is reachable at LANGGRAPH_INTEGRATION_URL."""
    try:
        resp = httpx.get(f"{BASE_URL}/ok", timeout=2.0)
        resp.raise_for_status()
    except Exception as err:
        pytest.skip(
            f"langgraph-api not reachable at {BASE_URL}: {err!r}. "
            "Bring up via ./scripts/test.sh (sdk-py graphs)."
        )
    return BASE_URL


@pytest.fixture
async def async_sdk(live_api: str):
    from langgraph_sdk import get_client

    client = get_client(url=live_api)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def sync_sdk(live_api: str):
    from langgraph_sdk import get_sync_client

    client = get_sync_client(url=live_api)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
async def async_threads(live_api: str) -> AsyncIterator[tuple[object, httpx.AsyncClient]]:
    from langgraph_sdk._async.http import HttpClient
    from langgraph_sdk._async.threads import ThreadsClient

    raw = httpx.AsyncClient(base_url=live_api, timeout=60.0)
    try:
        yield ThreadsClient(HttpClient(raw)), raw
    finally:
        await raw.aclose()


@pytest.fixture
def sync_threads(live_api: str) -> Iterator[tuple[object, httpx.Client]]:
    from langgraph_sdk._sync.http import SyncHttpClient
    from langgraph_sdk._sync.threads import SyncThreadsClient

    raw = httpx.Client(base_url=live_api, timeout=60.0)
    try:
        yield SyncThreadsClient(SyncHttpClient(raw)), raw
    finally:
        raw.close()
