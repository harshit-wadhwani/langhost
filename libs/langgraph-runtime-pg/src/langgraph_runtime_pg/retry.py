"""DB retry helpers matching the inmem surface."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class RetryableException(Exception):
    pass


RETRIABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (RetryableException,)
OVERLOADED_EXCEPTIONS: tuple[type[BaseException], ...] = ()


def retry_db(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    """Retry ``RETRIABLE_EXCEPTIONS`` up to 3 times."""

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last: BaseException | None = None
        for i in range(3):
            try:
                return await func(*args, **kwargs)
            except RETRIABLE_EXCEPTIONS as exc:
                last = exc
                if i == 2:
                    raise
                await asyncio.sleep(0.01)
        assert last is not None  # pragma: no cover
        raise last  # pragma: no cover

    return wrapper
