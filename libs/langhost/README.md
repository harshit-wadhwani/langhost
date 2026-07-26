# langhost

Minimal CLI for running the official LangGraph Agent Server on the open
[`langgraph-runtime-pg`](https://pypi.org/project/langgraph-runtime-pg/) backend
(Postgres + Redis).

## Command

```bash
langhost serve [OPTIONS]
```

Requires a `langgraph.json` in the working directory (same format as
[`langgraph-cli`](https://github.com/langchain-ai/langgraph/tree/main/libs/cli))
and `DATABASE_URI` / `REDIS_URI` (see repo `.env.example`). Bring your own
Postgres + Redis (for example via the repo `docker-compose.yml`).

```bash
cp .env.example .env
docker compose up -d postgres redis

# Dev (hot reload)
langhost serve --reload -c langgraph.json

# Prod (multi-process)
langhost serve --host 0.0.0.0 --workers 4 -c langgraph.json
```

Run `langhost serve --help` for the full option list.
