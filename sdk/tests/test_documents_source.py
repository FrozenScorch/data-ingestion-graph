from __future__ import annotations

from email.message import EmailMessage
from pathlib import Path

import pytest

from ingestion_graph import ConnectorSpec
from ingestion_graph import LocalDocumentsSource as PublicLocalDocumentsSource
from ingestion_graph.destinations import JsonlDestination
from ingestion_graph.messages import RecordMessage, StateMessage
from ingestion_graph.models import DocumentElement, TableBatch
from ingestion_graph.pipeline import Pipeline
from ingestion_graph.sources import LocalDocumentsSource, documents
from ingestion_graph.state import MemoryStateStore


async def collect(source: LocalDocumentsSource, stream, state=None):
    return [message async for message in source.read(stream, state)]


@pytest.mark.asyncio
async def test_discovers_supported_files_and_ignores_hidden_and_unknown(tmp_path: Path):
    (tmp_path / "notes.txt").write_text("hello", encoding="utf-8")
    (tmp_path / "data.csv").write_text("id,name\n1,Ada\n", encoding="utf-8")
    (tmp_path / "ignore.bin").write_bytes(b"binary")
    (tmp_path / ".hidden.md").write_text("secret", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "readme.md").write_text("nested", encoding="utf-8")

    source = LocalDocumentsSource(tmp_path)
    assert PublicLocalDocumentsSource is LocalDocumentsSource
    assert isinstance(source.spec(), ConnectorSpec)
    assert (await source.check()).ok
    streams = await source.discover()

    assert [stream.name for stream in streams] == ["data.csv", "nested/readme.md", "notes.txt"]
    assert all(stream.namespace == "local.documents" for stream in streams)
    assert source.spec().capabilities.incremental is True


@pytest.mark.asyncio
async def test_text_resume_and_content_change_are_checkpoint_safe(tmp_path: Path):
    path = tmp_path / "long.txt"
    path.write_text(("first section " * 30) + "\n" + ("second section " * 30), encoding="utf-8")
    source = LocalDocumentsSource(path, checkpoint_interval=1, text_chunk_chars=256)
    stream = (await source.discover())[0]

    first = await collect(source, stream)
    records = [message for message in first if isinstance(message, RecordMessage)]
    states = [message for message in first if isinstance(message, StateMessage)]
    assert len(records) >= 2
    assert states[0].state["complete"] is False
    assert states[-1].state["complete"] is True

    resumed = await collect(source, stream, states[0].state)
    resumed_records = [message for message in resumed if isinstance(message, RecordMessage)]
    assert [item.envelope.id for item in resumed_records] == [
        item.envelope.id for item in records[1:]
    ]

    complete = await collect(source, stream, states[-1].state)
    assert not any(isinstance(message, RecordMessage) for message in complete)

    path.write_text("replacement content", encoding="utf-8")
    changed = await collect(source, stream, states[-1].state)
    changed_records = [message for message in changed if isinstance(message, RecordMessage)]
    assert len(changed_records) == 1
    assert changed_records[0].envelope.checksum != records[0].envelope.checksum


@pytest.mark.asyncio
async def test_csv_and_excel_emit_table_batches(tmp_path: Path):
    csv_path = tmp_path / "people.csv"
    csv_path.write_text("id,name\n1,Ada\n2,Grace\n", encoding="utf-8")

    if documents.load_workbook is None:
        pytest.skip("openpyxl is not installed")
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Orders"
    sheet.append(["id", "total"])
    sheet.append([1, 10.5])
    sheet.append([2, 20.0])
    xlsx_path = tmp_path / "orders.xlsx"
    workbook.save(xlsx_path)

    source = LocalDocumentsSource(tmp_path, table_batch_rows=1)
    streams = {stream.name: stream for stream in await source.discover()}
    csv_messages = await collect(source, streams["people.csv"])
    xlsx_messages = await collect(source, streams["orders.xlsx"])
    csv_payloads = [
        message.envelope.payload for message in csv_messages if isinstance(message, RecordMessage)
    ]
    xlsx_payloads = [
        message.envelope.payload for message in xlsx_messages if isinstance(message, RecordMessage)
    ]

    assert all(isinstance(payload, TableBatch) for payload in csv_payloads + xlsx_payloads)
    assert [payload.rows[0]["name"] for payload in csv_payloads] == ["Ada", "Grace"]
    assert [payload.rows[0]["total"] for payload in xlsx_payloads] == [10.5, 20]


@pytest.mark.asyncio
async def test_word_email_html_and_pdf_emit_document_elements(tmp_path: Path, monkeypatch):
    if documents.WordDocument is None:
        pytest.skip("python-docx is not installed")
    from docx import Document

    word = Document()
    word.add_heading("Quarterly report")
    word.add_paragraph("Revenue increased.")
    table = word.add_table(rows=2, cols=2)
    table.rows[0].cells[0].text = "metric"
    table.rows[0].cells[1].text = "value"
    table.rows[1].cells[0].text = "revenue"
    table.rows[1].cells[1].text = "42"
    word.save(tmp_path / "report.docx")

    message = EmailMessage()
    message["Subject"] = "Status"
    message["From"] = "sender@example.com"
    message["To"] = "team@example.com"
    message.set_content("Everything is green.")
    (tmp_path / "status.eml").write_bytes(message.as_bytes())
    (tmp_path / "page.html").write_text("<h1>Hello</h1><p>from HTML</p>", encoding="utf-8")

    class FakePage:
        def extract_text(self):
            return "PDF page text"

    class FakeReader:
        def __init__(self, _path):
            self.pages = [FakePage()]

    monkeypatch.setattr(documents, "PdfReader", FakeReader)
    (tmp_path / "paper.pdf").write_bytes(b"fake-pdf")

    source = LocalDocumentsSource(tmp_path)
    streams = {stream.name: stream for stream in await source.discover()}
    payloads = {}
    for name in ("report.docx", "status.eml", "page.html", "paper.pdf"):
        messages = await collect(source, streams[name])
        payloads[name] = [
            message.envelope.payload for message in messages if isinstance(message, RecordMessage)
        ]

    assert any(isinstance(item, DocumentElement) for item in payloads["report.docx"])
    assert any(isinstance(item, TableBatch) for item in payloads["report.docx"])
    assert payloads["status.eml"][0].text == "Everything is green."
    assert payloads["page.html"][0].text == "Hello\nfrom HTML"
    assert payloads["paper.pdf"][0].page_number == 1


@pytest.mark.asyncio
async def test_missing_optional_parser_is_reported_by_check(tmp_path: Path, monkeypatch):
    (tmp_path / "paper.pdf").write_bytes(b"pdf")
    monkeypatch.setattr(documents, "PdfReader", None)

    result = await LocalDocumentsSource(tmp_path).check()

    assert result.ok is False
    assert "ingestion-graph[documents]" in result.message


@pytest.mark.asyncio
async def test_document_source_runs_in_embedded_pipeline(tmp_path: Path):
    source_path = tmp_path / "source"
    source_path.mkdir()
    (source_path / "notes.md").write_text("Reusable SDK ingestion", encoding="utf-8")
    destination_path = tmp_path / "records.jsonl"
    state = MemoryStateStore()

    result = await Pipeline(
        "documents",
        LocalDocumentsSource(source_path),
        JsonlDestination(destination_path),
        state_store=state,
    ).run()

    assert result.records_written == 1
    assert result.checkpoints_committed == 1
    assert "Reusable SDK ingestion" in destination_path.read_text(encoding="utf-8")
