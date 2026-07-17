"""Isolated PDFium worker used to make rendering deadlines cancellable."""

from __future__ import annotations

import sys

from ingestion_graph.document_ai.rendering import PdfiumPageRenderer


def main() -> int:
    if len(sys.argv) != 5:
        return 2
    try:
        page_number, dpi, max_pixels, max_output_bytes = map(int, sys.argv[1:])
        source = sys.stdin.buffer.read()
        renderer = PdfiumPageRenderer(
            max_pixels=max_pixels,
            max_output_bytes=max_output_bytes,
            max_source_bytes=max(len(source), 1),
            use_subprocess=False,
        )
        output = renderer._render_sync(source, page_number, dpi)
        sys.stdout.buffer.write(output)
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
