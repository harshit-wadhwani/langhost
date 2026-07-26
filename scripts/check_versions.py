#!/usr/bin/env python3
"""After `uv lock --check`, verify lockstep versions in uv.lock."""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    pkgs = {
        p["name"]: p["version"] for p in tomllib.loads((ROOT / "uv.lock").read_text())["package"]
    }
    runtime = pkgs["langgraph-runtime-pg"]
    langhost = pkgs["langhost"]
    api = pkgs["langgraph-api"]

    if runtime != langhost:
        raise SystemExit(f"lockstep: runtime-pg={runtime!r} langhost={langhost!r}")
    if runtime.split(".post")[0] != api:
        raise SystemExit(f"{runtime!r} base != langgraph-api=={api}")

    root_license = (ROOT / "LICENSE").read_text()
    for rel in (
        "libs/langhost/LICENSE",
        "libs/langgraph-runtime-pg/LICENSE",
    ):
        pkg_license = (ROOT / rel).read_text()
        if pkg_license != root_license:
            raise SystemExit(f"{rel} must match root LICENSE (copy after editing)")

    print(f"ok: {runtime} ↔ langgraph-api=={api}")


if __name__ == "__main__":
    main()
