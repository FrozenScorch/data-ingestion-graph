# Ingestion Graph - Project Instructions

## Overview
Visual, node-based data ingestion pipeline builder. Users construct DAGs on a Svelte Flow canvas. Backend executes with checkpointing, parallel fan-out/fan-in, retry logic, data lineage, provenance, DLQ, and run replay.

## Tech Stack
- **Backend:** FastAPI + Python 3.11, SQLAlchemy async + asyncpg
- **Frontend:** SvelteKit + TypeScript + Tailwind + @xyflow/svelte (Phase 2+)
- **Database:** PostgreSQL 16 + pgvector
- **Cache/Queue:** Redis 7
- **AI Provider:** OpenRouter (OpenAI SDK with custom baseURL)
- **Docker:** network_mode: host

## API Port
- **Backend API:** 8040
- **Frontend:** 3000 (adapter-node)
- **PostgreSQL:** 5432
- **Redis:** 6379

## AI/LLM Preferences
- **OpenRouter** (`https://openrouter.ai/api/v1`) is the inference and embedding provider.
- API key stored in `.env` as `OPENROUTER_API_KEY`. Never commit API keys to git.
- **Model dropdown ordering:** Sort models by cost (cheapest first). Display cost per 1M tokens visibly in the UI.
- **Cost control -- TESTING ONLY:** When testing endpoints or building, use ONLY free models:
  - `z-ai/glm-4.5-air:free`
  - `qwen/qwen3.5-9b`
- **NEVER use paid models for testing** without explicit user approval.

## Deployment
- All containers use `network_mode: host` - service URLs must use `localhost:PORT`
- Volumes: `ingestion_postgres_data`, `ingestion_redis_data`, `./data/uploads`, `./data/temp`

## Code Patterns
- Lifespan context manager for app startup (init DB, init Redis, load node registry)
- Pydantic Settings for .env configuration
- SQLAlchemy async engine + session factory (asyncpg)
- JWT middleware for `/api/*` routes
- pgvector columns for embedding storage

## Frontend Preferences
- **Svelte** is the preferred frontend framework. Do not use React, Vue, or Angular unless explicitly requested.
- **Three.js** is the preferred library for any 3D graphics, WebGL, or visual rendering needs.

## Important Constraints
- Always check existing comments and unit tests before modifying code
- Only modify code necessary to fix bugs or implement features
- If you don't understand the entire code, explore the codebase first
- use port 8000 for file website. do not use other ports for a file share based projects. state this assumption when used.
