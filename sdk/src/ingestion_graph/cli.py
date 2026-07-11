"""Minimal CLI for validating the SDK installation and inspecting plugins."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from ingestion_graph import __version__
from ingestion_graph.plugins import PLUGIN_GROUPS, discover_plugins


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingestion-graph")
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command")
    plugins = subparsers.add_parser("plugins", help="List installed connector plugins")
    plugins.add_argument("--json", action="store_true", dest="as_json")
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
    if args.command == "plugins":
        result = {kind: sorted(discover_plugins(kind)) for kind in PLUGIN_GROUPS}
        if args.as_json:
            print(json.dumps(result, sort_keys=True))
        else:
            for kind, names in result.items():
                print(f"{kind}: {', '.join(names) if names else '(none)'}")
        return 0
    if args.command == "discord-sync":
        return asyncio.run(_discord_sync(args))
    if args.command == "ingest-jsonl":
        return asyncio.run(_ingest_jsonl(args))
    if args.command == "query":
        return asyncio.run(_query(args))
    build_parser().print_help()
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
