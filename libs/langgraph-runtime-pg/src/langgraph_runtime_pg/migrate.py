"""Alembic migration runner (``langgraph-runtime-pg-migrate``)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from langgraph_runtime_pg.database import get_database_uri, to_psycopg_uri

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def to_sync_url(uri: str) -> str:
    """Convert DATABASE_URI to a sync psycopg URL for Alembic."""
    return "postgresql+psycopg://" + to_psycopg_uri(uri).removeprefix("postgresql://")


def alembic_config(database_uri: str | None = None) -> Config:
    uri = to_sync_url(database_uri or get_database_uri())
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", uri)
    cfg.attributes["configure_logger"] = False
    return cfg


def _head_revision() -> str:
    heads = ScriptDirectory(str(MIGRATIONS_DIR)).get_heads()
    return heads[0] if len(heads) == 1 else "head"


def upgrade_head(database_uri: str | None = None) -> str:
    """Apply all pending migrations; return the head revision id."""
    cfg = alembic_config(database_uri)
    command.upgrade(cfg, "head")
    return _head_revision()


def stamp_head(database_uri: str | None = None) -> None:
    """Mark DB at head without running DDL."""
    command.stamp(alembic_config(database_uri), "head")


def current(database_uri: str | None = None) -> None:
    command.current(alembic_config(database_uri))


def history(database_uri: str | None = None) -> None:
    command.history(alembic_config(database_uri))


def downgrade(revision: str, database_uri: str | None = None) -> None:
    command.downgrade(alembic_config(database_uri), revision)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="langgraph-runtime-pg-migrate",
        description="Apply langgraph_runtime_pg Postgres schema migrations",
    )
    parser.add_argument(
        "command",
        choices=("upgrade", "stamp", "current", "history", "downgrade"),
        help="Migration action",
    )
    parser.add_argument(
        "revision",
        nargs="?",
        default="head",
        help="For downgrade: target revision (default unused for upgrade)",
    )
    args = parser.parse_args(argv)

    try:
        if args.command == "upgrade":
            rev = upgrade_head()
            print(f"upgraded to {rev}")
        elif args.command == "stamp":
            stamp_head()
            print("stamped head")
        elif args.command == "current":
            current()
        elif args.command == "history":
            history()
        elif args.command == "downgrade":
            target = args.revision if args.revision != "head" else "-1"
            downgrade(target)
            print(f"downgraded to {target}")
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
