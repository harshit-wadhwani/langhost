"""Minimal CLI for self-hosted LangGraph Agent Server (edition=pg)."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("langhost")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["__version__"]
