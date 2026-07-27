from pathlib import Path
from typing import Any

from langhost import cli as cli_module


def test_banner_uses_resolved_port() -> None:
    rendered = cli_module._langhost_welcome(
        host="127.0.0.1",
        port=51234,
        ssl=False,
        studio_origin=None,
        mount_prefix=None,
    )

    assert "http://127.0.0.1:51234" in rendered
    assert "31296" not in rendered


def test_serve_passes_resolved_port_to_banner_and_server(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_module, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_module, "validate_config_file", lambda _config: {})

    def resolve_port(host: str, port: int) -> int:
        calls["resolved"] = (host, port)
        return 51234

    def welcome(**kwargs: Any) -> str:
        calls["welcome"] = kwargs
        return "welcome"

    def run_server(host: str, port: int, *_args: Any, **_kwargs: Any) -> None:
        calls["server"] = (host, port)

    monkeypatch.setattr(cli_module, "_resolve_port", resolve_port)
    monkeypatch.setattr(cli_module, "_langhost_welcome", welcome)
    monkeypatch.setattr(cli_module, "run_server", run_server)

    cli_module.serve.callback(
        host="127.0.0.1",
        port=31296,
        config=tmp_path / "langgraph.json",
        env_file=None,
        database_uri="postgresql://example",
        redis_uri="redis://example",
        reload=False,
        reload_includes=(),
        reload_excludes=(),
        workers=1,
        n_jobs_per_worker=None,
        browser=False,
        studio_url=None,
        tunnel=False,
        debug_port=None,
        wait_for_client=False,
        allow_blocking=False,
        server_log_level="INFO",
        ssl_certfile=None,
        ssl_keyfile=None,
    )

    assert calls["resolved"] == ("127.0.0.1", 31296)
    assert calls["welcome"]["port"] == 51234
    assert calls["server"] == ("127.0.0.1", 51234)
