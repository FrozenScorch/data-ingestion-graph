# Portable pipeline specifications

> `PipelineSpec` is an unreleased SDK 0.8 development API. The verified SDK 0.7.0
> dependency pin, `1ffcd738f0699ddd518bd35029dc550c22a1f487`, does not contain it.

`PipelineSpec` is the SDK-owned executable configuration for embedded
applications and command-line runs. Version 1 matches one durable checkpoint
boundary:

```text
source -> ordered transforms -> idempotent destination
   \---------- checkpoint state store ----------/
```

It is intentionally smaller than the Studio DAG. A future Studio importer should
preserve this contract's execution and checkpoint semantics rather than create a
second portable format.

## Run the checked-in example

The JSONL example includes its input data and needs only the base SDK:

```shell
cd sdk
python -m pip install -e .
ingestion-graph validate examples/jsonl-to-sqlite.toml
ingestion-graph plan examples/jsonl-to-sqlite.toml
ingestion-graph run examples/jsonl-to-sqlite.toml
ingestion-graph query Ada --collection examples/.ingestion/items.db
```

`examples/local-documents.toml` is also dependency-free. It ingests the checked-in
text fixture and uses the built-in identity document splitter.

## Minimal TOML

```toml
schema_version = "1"
name = "jsonl-query"

[source]
plugin = "jsonl"
[source.config]
path = "./data/items.jsonl"

[destination]
plugin = "sqlite"
[destination.config]
path = "./.ingestion/items.db"

[state_store]
plugin = "sqlite"
[state_store.config]
path = "./.ingestion/state.db"
```

The same boundary is available in Python:

```python
from ingestion_graph import plan_ingestion_pipeline, run_ingestion_pipeline

plan = plan_ingestion_pipeline("pipeline.toml")
print(plan.state_namespace, plan.warnings)
result = await run_ingestion_pipeline("pipeline.toml", plan=plan)
```

Passing the plan back to `run_ingestion_pipeline` keeps planning and construction
on the same resolved plugin metadata.

## What validate and plan guarantee

```shell
ingestion-graph validate pipeline.toml
ingestion-graph plan pipeline.toml
ingestion-graph plan pipeline.toml --json
```

These commands load installed entry points and constructor-free manifests, apply
declared defaults, validate configuration, resolve schema-declared local paths,
and calculate checkpoint identities. They do not construct components, resolve
secrets, call connector `check()` methods, open configured source/destination
paths, or contact remote services. Plugin modules and manifest methods must
therefore be import-safe and side-effect-free.

Execution constructs the resolved components. Source and destination `check()`
methods run when the pipeline itself runs.

## Declarative plugin boundary

Configuration contains names, never arbitrary Python import strings. Names
resolve through standard entry points or an explicit in-process
`PipelineRegistry`.

Sources and destinations selected by a `PipelineSpec` must expose
`manifest() -> ConnectorSpec`. The manifest supplies a stable name and version,
capabilities, and an object-shaped JSON configuration schema. A
constructor-only source or destination can still be passed directly to
`Pipeline`, but it is not declaratively usable.

Transforms, state stores, and document providers use a callable factory. A
`manifest() -> ProviderSpec` is optional, but supplies an explicit stable version
and configuration schema. Without it, the planner can still validate constructor
arguments and infer an implementation identity, but it cannot infer path,
secret-reference, enum, or other schema semantics. The runtime checks each
factory's required type or structural methods before construction. Built-in state
stores and document providers ship equivalent provider metadata.

The entry-point groups are:

```text
ingestion_graph.sources
ingestion_graph.destinations
ingestion_graph.transforms
ingestion_graph.state_stores
ingestion_graph.document_ai.ocr_engines
ingestion_graph.document_ai.page_renderers
ingestion_graph.document_ai.table_extractors
ingestion_graph.document_ai.document_splitters
ingestion_graph.document_ai.extraction_caches
ingestion_graph.document_ai.vision_extractors
ingestion_graph.document_ai.external_processing_policies
```

The built-in state-store names are `memory` and `sqlite`. A state-store entry
point must construct a concrete `StateStore` subclass; its async `load` and
`save` methods are the checkpoint contract.

## Paths, defaults, and secrets

When a `.toml` or `.json` file is loaded, schema fields marked
`format = "local-path"` or `format = "local-paths"` resolve relative to the spec
file. Their resolved values and effective defaults participate in planning and
fingerprints. A field is not treated as a filesystem path merely because it is
named `path`: REST object paths, URL paths, and database identifiers must remain
ordinary strings.

Custom connectors and providers must therefore publish a manifest schema to get
spec-relative path behavior. For example:

```python
from ingestion_graph import ProviderSpec

class MyProvider:
    @classmethod
    def manifest(cls) -> ProviderSpec:
        return ProviderSpec(
            name="my-provider",
            version="2",
            config_schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "model_path": {
                        "type": "string",
                        "format": "local-path",
                    }
                },
                "required": ["model_path"],
            },
        )
```

Without that schema marker, a custom path string cannot be made spec-relative
and remains relative to the process working directory. Programmatically created
specs have no file origin, so their relative paths are captured against the
process working directory during planning. A prepared plan is rejected if that
resolution context changes; use absolute paths when working-directory changes
are possible.

Manifest fields with `format = "secret-ref"` require a reference such as:

```toml
password = "env:POSTGRES_PASSWORD"
```

The serialized spec and plan contain the provider and key, not the resolved
secret. Raw strings without a provider prefix fail validation.

## Checkpoint identity and rebuilds

A plan exposes three identities:

- `spec_fingerprint` covers the serialized definition, excluding its file
  location.
- `processing_fingerprint` covers the effective source, ordered transforms,
  selected streams, document providers, plugin/implementation versions,
  defaults, and resolved local paths.
- `materialization_fingerprint` adds the effective destination.

Unless `state_namespace` is explicitly set, the SDK derives the checkpoint
namespace from the pipeline name and full materialization fingerprint. A declared
source, parser/provider, transform, stream, or destination change therefore gets
a new source replay. State-store location and descriptive metadata do not change
the materialization identity.

An explicit `state_namespace` disables that automatic isolation. Treat it as a
manual epoch: reuse it only when the old checkpoints are compatible, and change
it whenever existing records must be replayed. `validate` and `plan` warn when a
manual namespace is present.

Fingerprints describe configuration and installed implementation identity; they
cannot observe external physical state. If a destination is deleted, truncated,
or recreated at the same configured location while its state store survives, the
configuration is unchanged and no replay is automatic. Coordinate that rebuild
with a new namespace (or an intentional reset of the matching checkpoints).

## Replaceable document components

`local_documents` accepts nested references for OCR, rendering, table extraction,
splitting, extraction caching, vision extraction, and external-processing policy:

```toml
[source]
plugin = "local_documents"
[source.config]
paths = ["./documents"]
ocr_mode = "auto"

[source.config.ocr_engine]
plugin = "tesseract"

[source.config.page_renderer]
plugin = "pdfium"

[source.config.extraction_cache]
plugin = "sqlite"
[source.config.extraction_cache.config]
path = "./.ingestion/extraction-cache.db"
```

Executing this OCR configuration requires the checkout's `ocr` extra, a local
Tesseract executable, and the configured local model/runtime dependencies.
Planning remains constructor-free.

Built-in provider names are:

| Provider kind | Built-ins |
|---|---|
| OCR engine | `tesseract` |
| Page renderer | `pdfium` |
| Document splitter | `identity` |
| Extraction cache | `memory`, `sqlite` |

There are no built-in table, vision, or external-processing-policy providers.
Declarative `table_mode = "local"` or `"auto"` requires an explicit table
extractor. Vision processing requires both a vision extractor and an explicit
external-processing policy. The SDK does not guess model paths or download model
artifacts.

Provider packages register a class, or a factory with a concrete return type, in
the corresponding group. A side-effect-free `ProviderSpec` manifest is required
for schema-aware defaults, paths, and secret references. The provider type must
implement that group's structural contract, such as
`check`/`recognize`/`close` for OCR or `render`/`close` for page rendering.

## Current v1 boundary

Version 1 is a linear, checkpoint-bounded graph. It does not serialize arbitrary
Studio DAG nodes, schedules, retries, resources, or deployment policies. Those
features can be layered on the same versioned core without changing its durable
checkpoint boundary.
