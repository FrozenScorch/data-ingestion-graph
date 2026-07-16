"""Optional, provider-neutral document intelligence contracts."""

from ingestion_graph.document_ai.cache import (
    ExtractionCache,
    MemoryExtractionCache,
    SQLiteExtractionCache,
)
from ingestion_graph.document_ai.docling_adapter import DoclingTableExtractor
from ingestion_graph.document_ai.models import (
    BoundingBox,
    ComponentDescriptor,
    EngineUsage,
    ExtractionWarning,
    OcrResult,
    OcrToken,
    SplitChunk,
    TableArtifact,
    TableCell,
    canonical_fingerprint,
)
from ingestion_graph.document_ai.protocols import (
    DocumentSplitter,
    ExternalProcessingPolicy,
    LayoutAnalyzer,
    OcrEngine,
    PageRenderer,
    TableExtractor,
    VisionExtractor,
)
from ingestion_graph.document_ai.quality import TextQuality, evaluate_text_quality
from ingestion_graph.document_ai.rendering import PdfiumPageRenderer
from ingestion_graph.document_ai.splitters import IdentitySplitter
from ingestion_graph.document_ai.tables import table_artifact_to_batches
from ingestion_graph.document_ai.tesseract import TesseractOcrEngine

__all__ = [
    "BoundingBox",
    "ComponentDescriptor",
    "DocumentSplitter",
    "DoclingTableExtractor",
    "EngineUsage",
    "ExtractionCache",
    "ExtractionWarning",
    "ExternalProcessingPolicy",
    "IdentitySplitter",
    "LayoutAnalyzer",
    "MemoryExtractionCache",
    "OcrEngine",
    "OcrResult",
    "OcrToken",
    "PageRenderer",
    "SQLiteExtractionCache",
    "SplitChunk",
    "TableArtifact",
    "TableCell",
    "TableExtractor",
    "TextQuality",
    "VisionExtractor",
    "canonical_fingerprint",
    "evaluate_text_quality",
    "table_artifact_to_batches",
    "PdfiumPageRenderer",
    "TesseractOcrEngine",
]
