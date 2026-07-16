# Local-first document intelligence

`LocalDocumentsSource` keeps its legacy parser path by default. OCR, image
ingestion, table recovery, and custom splitters are opt-in and run against the
immutable snapshot already used for checkpointing.

Install only the capabilities you need:

```shell
python -m pip install -e "ingestion-graph[documents,ocr]"
```

The `ocr` extra provides PDF/image rendering libraries. The CPU OCR adapter
uses the Tesseract executable and language data installed by the operating
system. `document-ai` is an optional, heavyweight Docling adapter; it requires
an explicitly configured offline converter and never downloads model artifacts
from SDK import or source construction.

```python
from ingestion_graph import LocalDocumentsSource
from ingestion_graph.document_ai import SQLiteExtractionCache, TesseractOcrEngine

source = LocalDocumentsSource(
    "documents/",
    ocr_mode="auto",
    ocr_engine=TesseractOcrEngine(),
    table_mode="off",
    extraction_cache=SQLiteExtractionCache(".ingestion/extraction-cache.db"),
    failure_mode="best_effort",
)
```

The legacy defaults remain unchanged: image extensions are not discovered,
PDFs use `pypdf.extract_text()`, and no extraction metadata is added. Set
`ocr_mode="auto"` or `"always"` and explicitly include image extensions when
ingesting PNG, JPEG, WebP, or TIFF files.

Tables are normalized through the provider-neutral `TableArtifact` contract and
then emitted as the existing `TableBatch` payload. A user-supplied
`TableExtractor`, `DocumentSplitter`, or `VisionExtractor` can be injected
without importing Studio or a model provider. Narrative splitters run after
semantic extraction; `TableBatch` is never sent through a text splitter.

Vision is a bounded, region-level fallback. It is disabled unless explicitly
configured, and nondeterministic/external components require a persistent
`ExtractionCache` so interrupted reads cannot regenerate a different element
sequence. The SDK validates cached values and structured table results using
JSON-safe contracts; raw prompts, images, and provider payloads are not logged.

Docling normalization accepts an application-supplied offline converter factory:

```python
from ingestion_graph.document_ai import DoclingTableExtractor

extractor = DoclingTableExtractor(converter_factory=make_preconfigured_converter)
```

Cross-page stitching, handwriting, forms, formulas, charts, and live provider
adapters are intentionally deferred.
