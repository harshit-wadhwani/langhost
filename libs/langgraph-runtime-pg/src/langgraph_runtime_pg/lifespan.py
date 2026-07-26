"""Starlette lifespan: PG pool, Redis, graphs, and queue."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import structlog
from langchain_core.runnables.config import RunnableConfig, var_child_runnable_config
from langgraph.constants import CONF
from starlette.applications import Starlette

from langgraph_runtime_pg import queue
from langgraph_runtime_pg.database import start_pool, stop_pool

logger = structlog.stdlib.get_logger(__name__)

_LAST_LIFESPAN_ERROR: BaseException | None = None


def get_last_error() -> BaseException | None:
    return _LAST_LIFESPAN_ERROR


@asynccontextmanager
async def lifespan(
    app: Starlette | None = None,
    cancel_event: asyncio.Event | None = None,
    taskset: set[asyncio.Task] | None = None,
    **kwargs: Any,
) -> AsyncIterator[None]:
    import langgraph_api.config as config
    from langgraph_api import (
        __version__,
        _checkpointer as api_checkpointer,
        feature_flags,
        graph,
        store as api_store,
    )
    from langgraph_api.asyncio import SimpleTaskGroup, set_event_loop
    from langgraph_api.http import (
        start_http_client,
        stop_http_client,
        stop_webhook_http_client,
    )
    from langgraph_api.js.ui import start_ui_bundler, stop_ui_bundler
    from langgraph_api.metadata import metadata_loop

    from langgraph_runtime_pg import __version__ as runtime_version

    await logger.ainfo(
        f"Starting PG runtime with langgraph-api={__version__} "
        f"and langgraph-runtime-pg={runtime_version}",
        version=__version__,
        runtime_version=runtime_version,
    )
    try:
        current_loop = asyncio.get_running_loop()
        set_event_loop(current_loop)
    except RuntimeError:
        await logger.aerror("Failed to set loop")

    global _LAST_LIFESPAN_ERROR
    _LAST_LIFESPAN_ERROR = None

    async def _log_graph_load_failure(err: graph.GraphLoadError) -> None:
        cause = err.__cause__ or err.cause
        log_fields = err.log_fields()
        log_fields["action"] = "fix_user_graph"
        await logger.aerror(
            f"Graph '{err.spec.id}' failed to load: {err.cause_message}",
            **log_fields,
        )
        await logger.adebug(
            "Full graph load failure traceback (internal)",
            **{k: v for k, v in log_fields.items() if k != "user_traceback"},
            exc_info=cause,
        )

    started_http = False
    started_pool = False
    started_checkpointer = False
    started_ui = False
    try:
        await start_http_client()
        started_http = True
        await start_pool()
        started_pool = True
        await api_checkpointer.start_checkpointer()
        started_checkpointer = True
        await start_ui_bundler()
        started_ui = True

        async with SimpleTaskGroup(
            cancel=True,
            cancel_event=cancel_event,
            taskgroup_name="Lifespan",
        ) as tg:
            tg.create_task(metadata_loop())
            await api_store.collect_store_from_env()
            store_instance = await api_store.get_store()
            if not api_store.CUSTOM_STORE:
                tg.create_task(store_instance.start_ttl_sweeper())
            else:
                await logger.ainfo("Using custom store. Skipping store TTL sweeper.")

            if feature_flags.USE_RUNTIME_CONTEXT_API:
                from langgraph._internal._constants import (
                    CONFIG_KEY_RUNTIME,
                )
                from langgraph.runtime import Runtime

                langgraph_config = cast(
                    RunnableConfig,
                    {CONF: {CONFIG_KEY_RUNTIME: Runtime(store=store_instance)}},
                )
            else:
                from langgraph.constants import CONFIG_KEY_STORE

                langgraph_config = cast(
                    RunnableConfig,
                    {CONF: {CONFIG_KEY_STORE: store_instance}},
                )

            var_child_runnable_config.set(langgraph_config)

            graph.patch_packages_distributions()
            try:
                await graph.collect_graphs_from_env(True)
            except graph.GraphLoadError as exc:
                _LAST_LIFESPAN_ERROR = exc
                await _log_graph_load_failure(exc)
                raise
            if config.N_JOBS_PER_WORKER > 0:
                tg.create_task(queue_with_signal())

            from langgraph_api import cron_scheduler

            tg.create_task(cron_scheduler.cron_scheduler())

            yield
    except graph.GraphLoadError as exc:
        _LAST_LIFESPAN_ERROR = exc
        raise
    except asyncio.CancelledError:
        pass
    finally:
        try:
            await api_store.exit_store()
        except Exception:
            logger.debug("exit_store failed during lifespan cleanup", exc_info=True)
        if started_checkpointer:
            try:
                await api_checkpointer.exit_checkpointer()
            except Exception:
                logger.debug("exit_checkpointer failed during lifespan cleanup", exc_info=True)
        if started_ui:
            try:
                await stop_ui_bundler()
            except Exception:
                logger.debug("stop_ui_bundler failed during lifespan cleanup", exc_info=True)
        try:
            await graph.stop_remote_graphs()
        except Exception:
            logger.debug("stop_remote_graphs failed during lifespan cleanup", exc_info=True)
        if started_http:
            try:
                await stop_http_client()
            except Exception:
                logger.debug("stop_http_client failed during lifespan cleanup", exc_info=True)
            try:
                await stop_webhook_http_client()
            except Exception:
                logger.debug(
                    "stop_webhook_http_client failed during lifespan cleanup",
                    exc_info=True,
                )
        if started_pool:
            try:
                await stop_pool()
            except Exception:
                logger.debug("stop_pool failed during lifespan cleanup", exc_info=True)


async def queue_with_signal() -> None:
    try:
        await queue.queue()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.exception("Queue failed. Signaling shutdown", exc_info=exc)
        signal.raise_signal(signal.SIGINT)


lifespan.get_last_error = get_last_error  # type: ignore[attr-defined]
