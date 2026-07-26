"""Background run queue (Redis wake, SKIP LOCKED claim, worker dispatch)."""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import threading
from collections.abc import Callable, Coroutine
from contextlib import ExitStack
from typing import cast

import structlog

from langgraph_runtime_pg import database, ops
from langgraph_runtime_pg.redis_stream import (
    bg_job_heartbeat_secs,
    wait_for_queue_wake,
)

logger = structlog.stdlib.get_logger(__name__)

_WORKERS_LOCK = threading.Lock()
WORKERS: set = set()

SHUTDOWN_GRACE_PERIOD_SECS = 5


def _workers_add(item: object) -> None:
    with _WORKERS_LOCK:
        WORKERS.add(item)


def _workers_discard(item: object) -> None:
    with _WORKERS_LOCK:
        WORKERS.discard(item)


def _workers_snapshot() -> list:
    with _WORKERS_LOCK:
        return list(WORKERS)


class BgLoopRunner(asyncio.Runner):  # type: ignore[misc]
    """asyncio.Runner that owns a loop in a dedicated thread."""

    executor: concurrent.futures.ThreadPoolExecutor

    def __init__(self, idx: int):
        super().__init__()
        self.idx = idx

    def __enter__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(
            1, thread_name_prefix=f"bg-loop-{self.idx}"
        )
        self.executor.submit(self.get_loop).result()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            loop = self.get_loop()
            for task in asyncio.all_tasks(loop):
                task.cancel("Stopping background loop")
        except Exception:
            pass
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass
        return super().__exit__(exc_type, exc_val, exc_tb)

    def submit(
        self,
        coro: Coroutine,
        *,
        name: str | None = None,
        callback: Callable | None = None,
    ):
        fut = self.executor.submit(self.run, coro, name=name)
        _workers_add(fut)
        if callback:
            fut.add_done_callback(callback)
        return fut

    def run(self, coro: Coroutine, *, name: str | None = None):  # type: ignore[override]
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError("Runner.run() cannot be called from a running event loop")
        self._lazy_init()  # type: ignore[attr-defined]
        task = self._loop.create_task(coro, name=name)  # type: ignore[attr-defined]
        try:
            return self._loop.run_until_complete(task)  # type: ignore[attr-defined]
        except asyncio.exceptions.CancelledError:
            raise


def get_num_workers() -> int:
    with _WORKERS_LOCK:
        return len(WORKERS)


async def queue() -> None:
    from langgraph_api import config, graph, webhook, worker
    from langgraph_api.asyncio import AsyncQueue

    concurrency = config.N_JOBS_PER_WORKER
    loop = asyncio.get_running_loop()
    last_stats_secs: float | None = None
    last_sweep_secs: float | None = None
    runners = AsyncQueue[BgLoopRunner](concurrency)
    WEBHOOKS: set = set()

    with ExitStack() as stack:
        if config.BG_JOB_ISOLATED_LOOPS:
            await logger.ainfo("Starting queue with isolated loops")
            executor = stack.enter_context(concurrent.futures.ThreadPoolExecutor())
            RUNNERS = {stack.enter_context(BgLoopRunner(idx)) for idx in range(concurrency)}
            for r in RUNNERS:
                runners.put_nowait(r)
                r.get_loop().set_default_executor(executor)
        else:
            await logger.ainfo("Starting queue with shared loop")
            for _ in range(concurrency):
                runners.put_nowait(cast(BgLoopRunner, object()))
        expired_runners: list[BgLoopRunner] = []

        def cleanup(task, runner: BgLoopRunner):
            _workers_discard(task)
            try:
                if config.BG_JOB_ISOLATED_LOOPS:
                    loop.call_soon_threadsafe(runners.put_nowait, runner)
                else:
                    runners.put_nowait(runner)
            except Exception as exc:
                expired_runners.append(runner)
                logger.exception("Background worker cleanup failed", exc_info=exc)

            try:
                if task.cancelled():
                    return
                task_exc = task.exception()
                if task_exc:
                    if not isinstance(task_exc, asyncio.CancelledError):
                        logger.exception(
                            f"Background worker failed for task {task}",
                            exc_info=task_exc,
                        )
                    return
                result = task.result()
                if result and result.get("webhook"):
                    if config.BG_JOB_ISOLATED_LOOPS:
                        hook_fut = asyncio.run_coroutine_threadsafe(
                            webhook.call_webhook(result), loop
                        )
                        WEBHOOKS.add(hook_fut)
                        hook_fut.add_done_callback(WEBHOOKS.remove)
                    else:
                        hook_task = loop.create_task(
                            webhook.call_webhook(result),
                            name=f"webhook-{result['run']['run_id']}",
                        )
                        WEBHOOKS.add(hook_task)
                        hook_task.add_done_callback(WEBHOOKS.remove)
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.exception("Background worker cleanup failed", exc_info=exc)

        await logger.ainfo(f"Starting {concurrency} background workers")
        try:
            run = None
            while True:
                if expired_runners:
                    for runner in expired_runners:
                        await runners.put(runner)
                    expired_runners.clear()
                await runners.wait()
                try:
                    sweep_every = bg_job_heartbeat_secs() * 2
                    do_sweep = (
                        last_sweep_secs is None or loop.time() - last_sweep_secs > sweep_every
                    )
                    if calc_stats := (
                        last_stats_secs is None
                        or loop.time() - last_stats_secs > config.STATS_INTERVAL_SECS
                    ):
                        last_stats_secs = loop.time()
                        active = get_num_workers()
                        await logger.ainfo(
                            "Worker stats",
                            max=concurrency,
                            available=concurrency - active,
                            active=active,
                        )

                    if run is None and last_stats_secs is not None:
                        await wait_for_queue_wake(timeout=0.5)

                    run = None
                    async for run, attempt in ops.Runs.next(wait=False, limit=runners.qsize()):
                        runner = runners.get_nowait()
                        graph_id = (
                            run["kwargs"].get("config", {}).get("configurable", {}).get("graph_id")
                        )
                        task_name = f"run-{run['run_id']}-attempt-{attempt}"
                        if not config.BG_JOB_ISOLATED_LOOPS or (
                            graph_id and graph.is_js_graph(graph_id)
                        ):
                            task = asyncio.create_task(
                                worker.worker(run, attempt, loop),
                                name=task_name,
                            )
                            task.add_done_callback(functools.partial(cleanup, runner=runner))
                            _workers_add(task)
                        else:
                            runner.submit(
                                worker.worker(run, attempt, loop),
                                name=task_name,
                                callback=functools.partial(cleanup, runner=runner),
                            )

                    if calc_stats or do_sweep:
                        async with database.connect() as conn:
                            if calc_stats:
                                stats = await ops.Runs.stats(conn)
                                await logger.ainfo("Queue stats", **stats)
                            if do_sweep:
                                last_sweep_secs = loop.time()
                                swept = await ops.Runs.sweep()
                                if swept:
                                    await logger.awarning(
                                        "Swept stale runs",
                                        count=len(swept),
                                    )
                except Exception as exc:
                    logger.exception("Background worker scheduler failed", exc_info=exc)
                    await asyncio.sleep(1)
        finally:
            logger.info("Shutting down background workers")
            workers = _workers_snapshot()
            webhooks = list(WEBHOOKS)
            for task in workers:
                task.cancel()
            for task in webhooks:
                task.cancel()
            # Isolated loops return concurrent.futures.Future; bridge to asyncio.
            from langgraph_api.utils.future import chain_future

            futs: list[asyncio.Future] = []
            if config.BG_JOB_ISOLATED_LOOPS:
                futs.extend(
                    cast(asyncio.Future, chain_future(f, loop.create_future())) for f in workers
                )
                futs.extend(
                    cast(asyncio.Future, chain_future(f, loop.create_future())) for f in webhooks
                )
            else:
                futs.extend(cast(asyncio.Future, f) for f in workers)
                futs.extend(cast(asyncio.Future, f) for f in webhooks)
            if futs:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*futs, return_exceptions=True),
                        SHUTDOWN_GRACE_PERIOD_SECS,
                    )
                except TimeoutError:
                    logger.warning(
                        "Background workers did not finish within grace period",
                        timeout=SHUTDOWN_GRACE_PERIOD_SECS,
                    )
