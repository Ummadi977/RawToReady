"""
FastAPI backend for DataFlow Agents.
- /api/scrape          : LLM scraper   (SSE)
- /api/extract-direct  : Direct PDF/Excel→CSV extraction, no LLM  (SSE)
- /api/extract         : LLM extractor for complex layouts  (SSE)
- /api/clean           : LLM cleaner   (SSE)
- /api/clean-chat      : Chat-style LLM transformation  (SSE)
- /api/upload          : Upload raw files
- /api/files           : List output files
- /api/preview         : Preview CSV/Excel
- /api/download        : ZIP download
"""
from __future__ import annotations

import asyncio
import io
import json
import shutil
import threading
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from dataflow_agents.runner import (
    StepResult,
    stream_chat_cleaner,
    stream_cleaner,
    stream_extractor,
    stream_scraper,
)
from dataflow_agents.tools import _extract_pdf_with_llm

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="DataFlow Agents API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _abs(rel: str) -> Path:
    p = Path(rel)
    return p if p.is_absolute() else PROJECT_ROOT / p


# ── Serialisation ──────────────────────────────────────────────────────────────

def _ser(event_type: str, content: Any) -> dict:
    if event_type == "result":
        r: StepResult = content
        return {"type": "result", "success": r.success,
                "files": r.files, "previews": r.previews, "error": r.error}
    if event_type == "script_path":
        return {"type": "script_path", "path": str(content)}
    return {"type": event_type, "content": str(content)}


def _sse(gen_fn, stop_event: threading.Event, *args, **kwargs):
    """Wrap a sync generator in an async SSE generator."""
    async def _inner():
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def _run():
            try:
                for item in gen_fn(*args, stop_event=stop_event, **kwargs):
                    loop.call_soon_threadsafe(q.put_nowait, item)
            except Exception as exc:
                loop.call_soon_threadsafe(q.put_nowait, ("error", str(exc)))
            finally:
                loop.call_soon_threadsafe(q.put_nowait, None)

        threading.Thread(target=_run, daemon=True).start()
        try:
            while True:
                item = await q.get()
                if item is None:
                    break
                yield {"data": json.dumps(_ser(*item))}
        except asyncio.CancelledError:
            stop_event.set()
            raise
        finally:
            stop_event.set()

    return _inner()


# ── Direct extraction helpers (no LLM) ────────────────────────────────────────

def _extract_excel_file(src: Path, dest_dir: Path) -> int:
    """Extract every non-empty sheet as a CSV. Returns sheet count."""
    xl = pd.ExcelFile(src)
    saved = 0
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        if df.empty:
            continue
        safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in str(sheet)).strip()
        out = dest_dir / f"{src.stem}_{safe}.csv"
        df.to_csv(out, index=False)
        saved += 1
    return saved


def _extract_pdf_file(src: Path, dest_dir: Path) -> int:
    """Extract tables from a PDF using LLM vision. Returns table count."""
    results = _extract_pdf_with_llm(src, dest_dir)
    return sum(1 for r in results if "✓" in r)


def _direct_extract_gen(output_dir: str, save_as: str = ""):
    """
    Sync generator: directly extract tables from PDFs / Excel files, no LLM.
    Yields (event_type, content) for SSE.
    If save_as is provided, all extracted CSVs are combined into one file with that name.
    """
    raw_dir = PROJECT_ROOT / "data" / "raw" / output_dir
    interim_dir = PROJECT_ROOT / "data" / "interim" / output_dir

    if not raw_dir.exists():
        yield "error", f"No files found at data/raw/{output_dir}/ — please upload files first."
        yield "result", StepResult(success=False, error=f"data/raw/{output_dir}/ does not exist")
        return

    all_files = [f for f in sorted(raw_dir.rglob("*")) if f.is_file()]
    if not all_files:
        yield "error", "No files in the raw directory. Please upload PDFs or Excel files first."
        yield "result", StepResult(success=False, error="No input files")
        return

    # Clear interim and recreate
    if interim_dir.exists():
        shutil.rmtree(interim_dir)
    interim_dir.mkdir(parents=True)

    yield "thought", f"📂 {len(all_files)} file(s) found in data/raw/{output_dir}/"

    total = 0
    errors: list[str] = []

    for fp in all_files:
        ext = fp.suffix.lower()
        yield "tool_call", f"⚡ {fp.name}"
        try:
            if ext in (".xlsx", ".xls"):
                n = _extract_excel_file(fp, interim_dir)
                total += n
                yield "tool_result", f"✓ {fp.name} → {n} sheet(s) saved"

            elif ext == ".pdf":
                n = _extract_pdf_file(fp, interim_dir)
                total += n
                if n > 0:
                    yield "tool_result", f"✓ {fp.name} → {n} table(s) saved"
                else:
                    msg = f"⚠ {fp.name} — no tables detected"
                    yield "thought", msg
                    errors.append(msg)

            elif ext == ".csv":
                shutil.copy2(fp, interim_dir / fp.name)
                total += 1
                yield "tool_result", f"✓ {fp.name} → copied as-is"

            else:
                yield "thought", f"⏭ Skipping {fp.name} (unsupported: {ext})"

        except Exception as exc:
            msg = f"✗ {fp.name}: {exc}"
            yield "error", msg
            errors.append(msg)

    # ── Combine into one file if save_as was specified ──────────────────────
    if save_as and total > 0:
        csvs = sorted(interim_dir.rglob("*.csv"))
        if csvs:
            try:
                combined = pd.concat(
                    [pd.read_csv(c, header=None, dtype=str) for c in csvs],
                    ignore_index=True,
                )
                fname = save_as if save_as.endswith(".csv") else f"{save_as}.csv"
                out_path = interim_dir / fname
                combined.to_csv(out_path, index=False, header=False)
                for c in csvs:
                    if c != out_path:
                        c.unlink()
                yield "tool_result", f"✓ Combined {len(csvs)} table(s) → {fname}  ({combined.shape[0]} rows)"
            except Exception as exc:
                yield "error", f"✗ Could not combine tables: {exc}"

    # Collect file list
    out_files = [
        f"{f.relative_to(interim_dir)}  ({f.stat().st_size:,} bytes)"
        for f in sorted(interim_dir.rglob("*")) if f.is_file()
    ]

    success = total > 0
    if success:
        yield "thought", f"✅ Done — {total} table(s) extracted to data/interim/{output_dir}/"
    else:
        yield "thought", "⚠️ No tables extracted. Upload PDFs or Excel files and try again."

    yield "result", StepResult(
        success=success,
        files=out_files,
        previews={},
        error="\n".join(errors) if not success else "",
    )


# ── Request models ─────────────────────────────────────────────────────────────

class ScrapeBody(BaseModel):
    url: str
    output_dir: str
    description: str = ""
    feedback: str = ""
    html_elements: str = ""


class ExtractBody(BaseModel):
    output_dir: str
    description: str = ""
    feedback: str = ""
    save_as: str = ""   # optional: instruct agent to save combined output with this name


class DirectExtractBody(BaseModel):
    output_dir: str
    save_as: str = ""   # optional: combine all tables into one CSV with this name


class CleanBody(BaseModel):
    output_dir: str
    description: str = ""
    feedback: str = ""
    single_file_only: bool = True


class ChatCleanBody(BaseModel):
    output_dir: str
    message: str          # natural-language transformation request


# ── SSE endpoints ──────────────────────────────────────────────────────────────

@app.post("/api/scrape")
async def scrape(body: ScrapeBody):
    stop = threading.Event()
    return EventSourceResponse(_sse(
        stream_scraper, stop,
        url=body.url, output_dir=body.output_dir,
        description=body.description, html_elements=body.html_elements,
        feedback=body.feedback,
    ))


@app.post("/api/extract-direct")
async def extract_direct(body: DirectExtractBody):
    """Direct PDF/Excel extraction — no LLM involved."""
    stop = threading.Event()

    async def gen():
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def _run():
            try:
                for item in _direct_extract_gen(body.output_dir, save_as=body.save_as):
                    loop.call_soon_threadsafe(q.put_nowait, item)
            except Exception as exc:
                loop.call_soon_threadsafe(q.put_nowait, ("error", str(exc)))
            finally:
                loop.call_soon_threadsafe(q.put_nowait, None)

        threading.Thread(target=_run, daemon=True).start()
        try:
            while True:
                item = await q.get()
                if item is None:
                    break
                yield {"data": json.dumps(_ser(*item))}
        except asyncio.CancelledError:
            stop.set()
            raise

    return EventSourceResponse(gen())


@app.post("/api/extract")
async def extract(body: ExtractBody):
    """LLM-powered extraction for complex layouts."""
    stop = threading.Event()
    return EventSourceResponse(_sse(
        stream_extractor, stop,
        output_dir=body.output_dir, feedback=body.feedback,
        description=(body.description + (f"\nSave all extracted tables combined into one file named: {body.save_as}" if body.save_as else "")),
    ))


@app.post("/api/clean")
async def clean(body: CleanBody):
    stop = threading.Event()
    return EventSourceResponse(_sse(
        stream_cleaner, stop,
        output_dir=body.output_dir, description=body.description,
        feedback=body.feedback, single_file_only=body.single_file_only,
    ))


@app.post("/api/clean-chat")
async def clean_chat(body: ChatCleanBody):
    """Apply a single natural-language transformation to the processed data."""
    stop = threading.Event()

    async def gen():
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def _run():
            try:
                for item in stream_chat_cleaner(
                    output_dir=body.output_dir,
                    user_message=body.message,
                ):
                    loop.call_soon_threadsafe(q.put_nowait, item)
            except Exception as exc:
                loop.call_soon_threadsafe(q.put_nowait, ("error", str(exc)))
            finally:
                loop.call_soon_threadsafe(q.put_nowait, None)

        threading.Thread(target=_run, daemon=True).start()
        try:
            while True:
                item = await q.get()
                if item is None:
                    break
                yield {"data": json.dumps(_ser(*item))}
        except asyncio.CancelledError:
            stop.set()
            raise

    return EventSourceResponse(gen())


# ── File upload ────────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload_files(output_dir: str, files: list[UploadFile]):
    dest = _abs(f"data/raw/{output_dir}")
    dest.mkdir(parents=True, exist_ok=True)
    saved = []
    for f in files:
        if not f.filename:
            continue
        target = dest / f.filename
        target.write_bytes(await f.read())
        saved.append(f.filename)
    return {"saved": saved, "directory": str(dest.relative_to(PROJECT_ROOT))}


# ── File listing ───────────────────────────────────────────────────────────────

@app.get("/api/files")
async def list_files(output_dir: str, stage: str = "raw"):
    dir_path = _abs(f"data/{stage}/{output_dir}")
    if not dir_path.exists():
        return {"files": []}
    files = []
    for f in sorted(dir_path.rglob("*")):
        if f.is_file():
            files.append({
                "name": str(f.relative_to(dir_path)),
                "size": f.stat().st_size,
                "path": str(f.relative_to(PROJECT_ROOT)),
                "ext": f.suffix.lower(),
            })
    return {"files": files}


# ── Preview ────────────────────────────────────────────────────────────────────

@app.get("/api/preview")
async def preview_file(path: str):
    fp = _abs(path)
    if not fp.exists():
        raise HTTPException(404, "File not found")
    ext = fp.suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(fp, nrows=100, on_bad_lines="skip")
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(fp, nrows=100)
        else:
            return {"type": "text", "content": fp.read_text(errors="replace")[:3000]}

        rows = [
            [None if (isinstance(v, float) and v != v) else v for v in row]
            for row in df.values.tolist()
        ]
        return {
            "type": "csv",
            "columns": [str(c) for c in df.columns],
            "rows": rows,
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "dtypes": {str(c): str(t) for c, t in df.dtypes.items()},
        }
    except Exception as exc:
        return {"type": "error", "message": str(exc)}


# ── ZIP download ───────────────────────────────────────────────────────────────

@app.get("/api/download")
async def download_zip(output_dir: str, stage: str = "processed"):
    dir_path = _abs(f"data/{stage}/{output_dir}")
    if not dir_path.exists():
        raise HTTPException(404, "Directory not found")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(dir_path.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(dir_path))
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={output_dir}_{stage}.zip"},
    )


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok"}
