"""Command-line entry point for embedded ingestion pipelines."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from ingestion_graph import __version__
from ingestion_graph.errors import IngestionError
from ingestion_graph.plugins import PLUGIN_GROUPS, discover_plugins


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingestion-graph")
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command")

    plugins = subparsers.add_parser("plugins", help="List installed connector plugins")
    plugins.add_argument("--json", action="store_true", dest="as_json")

    validate = subparsers.add_parser(
        "validate", help="Validate a portable JSON or TOML pipeline specification"
    )
    validate.add_argument("spec", type=Path)
    validate.add_argument("--json", action="store_true", dest="as_json")

    plan = subparsers.add_parser(
        "plan", help="Inspect resolved plugins, state identity, and static warnings"
    )
    plan.add_argument("spec", type=Path)
    plan.add_argument("--json", action="store_true", dest="as_json")

    run = subparsers.add_parser("run", help="Run a portable JSON or TOML pipeline specification")
    run.add_argument("spec", type=Path)

    discord = subparsers.add_parser(
        "discord-sync", help="Synchronize Discord channel history to local JSONL"
    )
    discord.add_argument("--channel", action="append", required=True, dest="channels")
    discord.add_argument("--output", type=Path, required=True)
    discord.add_argument("--state", type=Path, default=Path(".ingestion/state.db"))
    discord.add_argument("--token-env", default="DISCORD_BOT_TOKEN")
    discord.add_argument("--pipeline", default="discord-history")

    ingest = subparsers.add_parser(
        "ingest-jsonl", help="Ingest a local JSONL file into a searchable SQLite collection"
    )
    ingest.add_argument("input", type=Path)
    ingest.add_argument("--collection", type=Path, default=Path(".ingestion/query.db"))
    ingest.add_argument("--state", type=Path, default=Path(".ingestion/state.db"))
    ingest.add_argument("--stream")
    ingest.add_argument("--id-field", default="id")
    ingest.add_argument("--operation-field", default="_operation")
    ingest.add_argument("--batch-size", type=int, default=500)
    ingest.add_argument("--pipeline", default="jsonl-query")

    query = subparsers.add_parser("query", help="Search or inspect a local SQLite collection")
    query.add_argument("text", nargs="?")
    query.add_argument("--collection", type=Path, default=Path(".ingestion/query.db"))
    query.add_argument("--source")
    query.add_argument("--stream")
    query.add_argument("--limit", type=int, default=10)
    query.add_argument("--offset", type=int, default=0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "plugins":
            result = {kind: sorted(discover_plugins(kind)) for kind in PLUGIN_GROUPS}
            if args.as_json:
                print(json.dumps(result, sort_keys=True))
            else:
                for kind, names in result.items():
                    print(f"{kind}: {', '.join(names) if names else '(none)'}")
            return 0
        if args.command == "validate":
            return _validate_spec(args)
        if args.command == "plan":
            return _plan_spec(args)
        if args.command == "run":
            return asyncio.run(_run_spec(args))
        if args.command == "discord-sync":
            return asyncio.run(_discord_sync(args))
        if args.command == "ingest-jsonl":
            return asyncio.run(_ingest_jsonl(args))
        if args.command == "query":
            return asyncio.run(_query(args))
    except (IngestionError, OSError, ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    build_parser().print_help()
    return 0


def _validate_spec(args: argparse.Namespace) -> int:
    from ingestion_graph.runtime import plan_ingestion_pipeline

    plan = plan_ingestion_pipeline(args.spec)
    result: dict[str, Any] = {
        "valid": True,
        "name": plan.name,
        "spec_fingerprint": plan.spec_fingerprint,
        "processing_fingerprint": plan.processing_fingerprint,
        "materialization_fingerprint": plan.materialization_fingerprint,
        "state_namespace": plan.state_namespace,
        "warnings": list(plan.warnings),
    }
    if args.as_json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"valid: {plan.name}")
        print(f"spec fingerprint: {plan.spec_fingerprint}")
        print(f"state namespace: {plan.state_namespace}")
        for warning in plan.warnings:
            print(f"warning: {warning}")
    return 0


def _plan_spec(args: argparse.Namespace) -> int:
    from ingestion_graph.runtime import plan_ingestion_pipeline

    plan = plan_ingestion_pipeline(args.spec)
    if args.as_json:
        print(json.dumps(plan.to_dict(), sort_keys=True))
        return 0
    print(f"pipeline: {plan.name}")
    print(f"source: {plan.source.plugin} ({plan.source.version or 'constructor-only'})")
    for index, transform in enumerate(plan.transforms, start=1):
        print(f"transform {index}: {transform.plugin} ({transform.version or 'constructor-only'})")
    print(
        f"destination: {plan.destination.plugin} ({plan.destination.version or 'constructor-only'})"
    )
    print(f"state store: {plan.state_store.plugin}")
    print(f"state namespace: {plan.state_namespace}")
    print(f"processing fingerprint: {plan.processing_fingerprint}")
    print(f"materialization fingerprint: {plan.materialization_fingerprint}")
    for extension in plan.extensions:
        location = "built-in" if extension.built_in else "entry point"
        print(f"{extension.field}: {extension.plugin} [{location}]")
    for warning in plan.warnings:
        print(f"warning: {warning}")
    return 0


async def _run_spec(args: argparse.Namespace) -> int:
    from ingestion_graph.runtime import plan_ingestion_pipeline, run_ingestion_pipeline
    from ingestion_graph.spec import PipelineSpec

    spec = PipelineSpec.load(args.spec)
    plan = plan_ingestion_pipeline(spec)
    result = await run_ingestion_pipeline(spec, plan=plan)
    print(
        json.dumps(
            {
                "pipeline": result.pipeline,
                "streams_processed": result.streams_processed,
                "records_written": result.records_written,
                "checkpoints_committed": result.checkpoints_committed,
                "spec_fingerprint": plan.spec_fingerprint,
                "processing_fingerprint": plan.processing_fingerprint,
                "materialization_fingerprint": plan.materialization_fingerprint,
                "state_namespace": plan.state_namespace,
            },
            sort_keys=True,
        )
    )
    return 0


async def _discord_sync(args: argparse.Namespace) -> int:
    from ingestion_graph.destinations import JsonlDestination
    from ingestion_graph.pipeline import Pipeline
    from ingestion_graph.secrets import SecretRef
    from ingestion_graph.sources import DiscordSource
    from ingestion_graph.state import SQLiteStateStore

    result = await Pipeline(
        args.pipeline,
        DiscordSource(args.channels, SecretRef(args.token_env)),
        JsonlDestination(args.output),
        state_store=SQLiteStateStore(args.state),
    ).run()
    print(
        json.dumps(
            {
                "pipeline": result.pipeline,
                "streams_processed": result.streams_processed,
                "records_written": result.records_written,
                "checkpoints_committed": result.checkpoints_committed,
                "output": str(args.output),
                "state": str(args.state),
            },
            sort_keys=True,
        )
    )
    return 0


async def _ingest_jsonl(args: argparse.Namespace) -> int:
    from ingestion_graph.destinations import SQLiteCollection
    from ingestion_graph.pipeline import Pipeline
    from ingestion_graph.sources import JsonlSource
    from ingestion_graph.state import SQLiteStateStore

    result = await Pipeline(
        args.pipeline,
        JsonlSource(
            args.input,
            stream=args.stream,
            id_field=args.id_field,
            operation_field=args.operation_field,
            batch_size=args.batch_size,
        ),
        SQLiteCollection(args.collection),
        state_store=SQLiteStateStore(args.state),
    ).run()
    print(
        json.dumps(
            {
                "pipeline": result.pipeline,
                "streams_processed": result.streams_processed,
                "records_written": result.records_written,
                "checkpoints_committed": result.checkpoints_committed,
                "collection": str(args.collection),
                "state": str(args.state),
            },
            sort_keys=True,
        )
    )
    return 0


async def _query(args: argparse.Namespace) -> int:
    from ingestion_graph.destinations import SQLiteCollection
    from ingestion_graph.query import QueryRequest

    result = await SQLiteCollection(args.collection).query(
        QueryRequest(
            text=args.text,
            source=args.source,
            stream=args.stream,
            limit=args.limit,
            offset=args.offset,
        )
    )
    print(
        json.dumps(
            {
                "total": result.total,
                "hits": [
                    {"score": hit.score, "envelope": hit.envelope.to_dict()} for hit in result
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
