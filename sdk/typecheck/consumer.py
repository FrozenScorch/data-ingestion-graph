"""External-consumer typing smoke test, run after installing the built wheel."""

from collections.abc import Sequence
from pathlib import Path

from ingestion_graph import (
    PIPELINE_SPEC_VERSION,
    ComponentRef,
    ConformanceReport,
    ConnectorPlan,
    ConnectorSpec,
    Envelope,
    ExtensionPlan,
    Pipeline,
    PipelinePlan,
    PipelineRegistry,
    PipelineResult,
    PipelineSpec,
    ProviderSpec,
    QueryRequest,
    QueryResult,
    RecordPayload,
    StateStoreRef,
    Transform,
    create_ingestion_pipeline,
    inspect_manifest,
    load_connector_manifest,
    load_pipeline_spec,
    plan_ingestion_pipeline,
    run_ingestion_pipeline,
    stable_record_id,
)

manifest: ConnectorSpec = load_connector_manifest("sources", "discord")
destination_manifest: ConnectorSpec = load_connector_manifest("destinations", "sqlite")
conformance: ConformanceReport = inspect_manifest(manifest, expected_name="discord")

record: Envelope = Envelope(
    id=stable_record_id("example", "items", "1"),
    source="example",
    stream="items",
    payload=RecordPayload({"value": 1}),
)
serialized: dict[str, object] = record.to_dict()
request: QueryRequest = QueryRequest("example", stream="items")


class KeepAll(Transform):
    async def apply(self, records: Sequence[Envelope]) -> Sequence[Envelope]:
        return records


transform: Transform = KeepAll()

source_ref: ComponentRef = ComponentRef("jsonl", {"path": "data/items.jsonl"})
destination_ref: ComponentRef = ComponentRef(
    "sqlite", {"path": ".ingestion/items.db"}
)
state_ref: StateStoreRef = StateStoreRef(
    "sqlite", {"path": ".ingestion/state.db"}
)
provider_spec: ProviderSpec = ProviderSpec(
    "example",
    "1",
    {"type": "object", "properties": {}, "additionalProperties": False},
)
pipeline_spec_version: str = PIPELINE_SPEC_VERSION
pipeline_spec: PipelineSpec = PipelineSpec(
    name="external-consumer",
    source=source_ref,
    destination=destination_ref,
    state_store=state_ref,
)
registry: PipelineRegistry = PipelineRegistry()
pipeline_plan: PipelinePlan = plan_ingestion_pipeline(
    pipeline_spec, registry=registry
)
source_plan: ConnectorPlan = pipeline_plan.source
extension_plans: tuple[ExtensionPlan, ...] = pipeline_plan.extensions
pipeline: Pipeline = create_ingestion_pipeline(
    pipeline_spec, registry=registry, plan=pipeline_plan
)


def first_match(result: QueryResult) -> Envelope:
    return result[0].envelope


def load_spec(path: Path) -> PipelineSpec:
    return load_pipeline_spec(path)


async def run_spec() -> PipelineResult:
    return await run_ingestion_pipeline(
        pipeline_spec, registry=registry, plan=pipeline_plan
    )
