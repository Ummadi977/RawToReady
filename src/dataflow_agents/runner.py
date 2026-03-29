"""
Pure agent runner functions — no LangGraph interrupts.

Used by the Streamlit UI and any other caller that manages
the human-in-the-loop flow itself.
"""

from __future__ import annotations

import asyncio
import json as _json
import queue
import threading
import uuid
from pathlib import Path
from dataclasses import dataclass, field

# Project root is three levels up from this file:
#   src/dataflow_agents/runner.py → src/dataflow_agents/ → src/ → project/
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _abs(path: str) -> Path:
    """Resolve a possibly-relative path against the project root."""
    p = Path(path)
    return p if p.is_absolute() else _PROJECT_ROOT / p


def _random_script(output_dir: str, stem: str) -> str:
    """Return a unique script path when the user did not provide one."""
    tag = uuid.uuid4().hex[:8]
    return f"src/pipelines/{output_dir}/{stem}_{tag}.py"

from langgraph.prebuilt import create_react_agent

from .llm import get_llm
from .tools import ALL_TOOLS, read_file
from .prompts import SCRAPER_PROMPT, EXTRACTOR_PROMPT, CLEANER_PROMPT, CHAT_CLEANER_PROMPT, VALIDATOR_PROMPT

# Playwright MCP server config (stdio transport)
_MCP_CONFIG = {
    "playwright": {
        "command": "npx",
        "args": ["@playwright/mcp@latest"],
        "transport": "stdio",
    }
}


# ── Playwright page inspector (no LLM — pure programmatic) ────────────────────

async def _inspect_page_async(url: str, event_queue: "queue.Queue") -> None:
    """
    Programmatically inspect a page using Playwright MCP tools (no LLM).
    Navigates, expands accordions via JS, collects all file URLs.
    Puts (event_type, content) events on the queue and ends with ('page_info', dict).
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient

    try:
        event_queue.put(("thought", "🔍 Opening page in browser…"))
        client = MultiServerMCPClient(_MCP_CONFIG)
        tools = await client.get_tools()
        tool_map = {t.name: t for t in tools}

        # Navigate
        await tool_map["browser_navigate"].ainvoke({"url": url})
        event_queue.put(("tool_call", f"🔧 browser_navigate(url='{url}')"))
        await asyncio.sleep(2)  # let JS render

        # Initial snapshot for page description
        snap_raw = await tool_map["browser_snapshot"].ainvoke({})
        snapshot = str(snap_raw)
        event_queue.put(("tool_call", "🔧 browser_snapshot()"))

        # Expand all accordion/collapsible sections via JavaScript
        expand_js = """() => {
            const clicked = [];
            document.querySelectorAll('button, [role="button"], .accordion-toggle').forEach(btn => {
                const txt = (btn.textContent || '').trim();
                const expanded = btn.getAttribute('aria-expanded');
                if (txt === '+' || txt === '▶' || txt === '▸' || expanded === 'false') {
                    try { btn.click(); clicked.push(txt || 'btn'); } catch(e) {}
                }
            });
            return clicked.length;
        }"""
        expand_result = await tool_map["browser_evaluate"].ainvoke({"function": expand_js})
        n_clicked = str(expand_result)
        event_queue.put(("tool_result", f"✓ Expanded {n_clicked} accordion sections"))
        await asyncio.sleep(1.5)  # wait for expansion animations

        # Collect all downloadable file URLs
        collect_js = """() => {
            return Array.from(document.querySelectorAll('a[href]'))
                .map(a => ({ url: a.href, text: (a.textContent || a.title || '').trim() }))
                .filter(l => /\\.(pdf|xlsx?|xls|csv|zip)$/i.test(l.url));
        }"""
        file_links_raw = await tool_map["browser_evaluate"].ainvoke({"function": collect_js})
        try:
            file_links = _json.loads(str(file_links_raw)) if isinstance(file_links_raw, str) else file_links_raw
            if not isinstance(file_links, list):
                file_links = []
        except Exception:
            file_links = []

        event_queue.put(("tool_result", f"✓ Found {len(file_links)} downloadable files"))
        if file_links:
            sample = ", ".join(l.get("text") or l.get("url", "")[:40] for l in file_links[:5])
            event_queue.put(("thought", f"📎 Files: {sample}{'…' if len(file_links) > 5 else ''}"))

        # Also grab page title for context
        title_raw = await tool_map["browser_evaluate"].ainvoke({"function": "() => document.title"})
        page_title = str(title_raw)

        event_queue.put(("page_info", {
            "url": url,
            "title": page_title,
            "snapshot": snapshot[:2000],
            "file_links": file_links,
        }))

    except Exception as e:
        event_queue.put(("error", f"Page inspection error: {e}"))
        event_queue.put(("page_info", {
            "url": url, "title": "", "snapshot": "", "file_links": [], "error": str(e)
        }))
    finally:
        event_queue.put(None)


def _inspect_page(url: str):
    """
    Sync generator: yields (event_type, content) during Playwright inspection.
    Final event is ('page_info', dict).
    """
    event_queue: queue.Queue = queue.Queue()

    def _run() -> None:
        asyncio.run(_inspect_page_async(url, event_queue))

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    while True:
        event = event_queue.get()
        if event is None:
            break
        yield event
    thread.join()


@dataclass
class ValidationResult:
    success: bool                  # True if all checks passed
    checks: list                   # [{name, passed, detail}]
    agent_log: str
    error: str = ""


@dataclass
class StepResult:
    success: bool
    files: list[str] = field(default_factory=list)
    previews: dict[str, str] = field(default_factory=dict)  # filename → preview text
    agent_log: str = ""
    error: str = ""


def _run_agent(system_prompt: str, task: str) -> tuple[bool, str]:
    """Run a ReAct agent and return (success, full_log)."""
    llm = get_llm()
    agent = create_react_agent(llm, ALL_TOOLS, prompt=system_prompt)
    try:
        result = agent.invoke({"messages": [("user", task)]})
        log_lines = []
        for msg in result.get("messages", []):
            content = getattr(msg, "content", "")
            if content:
                log_lines.append(str(content))
        return True, "\n\n".join(log_lines)
    except Exception as e:
        return False, f"Agent error: {e}"


def _emit_chunk(chunk: dict, log: list[str], event_queue: "queue.Queue[tuple | None]") -> None:
    """Parse a LangGraph stream chunk and put events onto the queue."""
    for _node_name, updates in chunk.items():
        for msg in updates.get("messages", []):
            tool_calls = getattr(msg, "tool_calls", [])
            tool_name = getattr(msg, "name", None)
            content = str(getattr(msg, "content", "") or "")

            if tool_calls:
                for tc in tool_calls:
                    args = tc.get("args", {})
                    args_str = ", ".join(f"{k}={repr(v)[:80]}" for k, v in args.items())
                    text = f"🔧 {tc['name']}({args_str})"
                    log.append(text)
                    event_queue.put(("tool_call", text))

            elif tool_name:
                short = content[:200] + ("…" if len(content) > 200 else "")
                text = f"✓ {tool_name}: {short}"
                log.append(text)
                event_queue.put(("tool_result", text))

            elif content:
                log.append(content)
                event_queue.put(("thought", content))


async def _stream_agent_async(
    system_prompt: str,
    task: str,
    event_queue: "queue.Queue[tuple | None]",
    stop_event: "threading.Event | None" = None,
) -> None:
    """Async producer: runs the ReAct agent and puts events onto event_queue."""
    llm = get_llm()
    log: list[str] = []
    try:
        agent = create_react_agent(llm, ALL_TOOLS, prompt=system_prompt)
        async for chunk in agent.astream({"messages": [("user", task)]}):
            if stop_event and stop_event.is_set():
                event_queue.put(("stopped", "⏹ Stopped by user."))
                log.append("Stopped by user.")
                break
            _emit_chunk(chunk, log, event_queue)
        event_queue.put(("done", "\n\n".join(log)))
    except Exception as e:
        msg = f"Agent error: {e}"
        log.append(msg)
        event_queue.put(("error", msg))
        event_queue.put(("done", "\n\n".join(log)))
    finally:
        event_queue.put(None)


def _stream_agent(
    system_prompt: str,
    task: str,
    stop_event: "threading.Event | None" = None,
):
    """
    Sync generator streaming (event_type, content) tuples from a ReAct agent.
    event_types: 'tool_call', 'tool_result', 'thought', 'error', 'stopped', 'done'
    """
    event_queue: queue.Queue[tuple | None] = queue.Queue()

    def _run() -> None:
        asyncio.run(_stream_agent_async(system_prompt, task, event_queue, stop_event))

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    while True:
        event = event_queue.get()
        if event is None:
            break
        yield event
    thread.join()


def _collect_files(directory: str) -> list[str]:
    p = _abs(directory)
    if not p.exists():
        return []
    return [
        f"{f.relative_to(p)}  ({f.stat().st_size:,} bytes)"
        for f in sorted(p.rglob("*")) if f.is_file()
    ]


def _collect_previews(directory: str, suffix: str = ".csv", max_files: int = 3) -> dict[str, str]:
    p = _abs(directory)
    if not p.exists():
        return {}
    previews = {}
    for f in sorted(p.rglob(f"*{suffix}"))[:max_files]:
        previews[str(f.relative_to(p))] = read_file.invoke({"path": str(f)})
    return previews


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_error(log: str) -> str:
    """Pull the most relevant error lines from an agent log."""
    lines = log.splitlines()
    error_lines = [
        l for l in lines
        if any(k in l for k in (
            "Error", "error", "STDERR", "Traceback", "Exception",
            "failed", "EXIT CODE", "404", "403", "ConnectionError",
        ))
    ]
    return "\n".join(error_lines[:10]) if error_lines else log[-500:]


# ── Scraper ────────────────────────────────────────────────────────────────────

def stream_scraper(
    url: str,
    output_dir: str,
    description: str = "",
    html_elements: str = "",
    script_path: str = "",
    feedback: str = "",
    max_retries: int = 3,
    stop_event: "threading.Event | None" = None,
):
    """
    Stream scraper events then yield a final ('result', StepResult).

    The user provides `html_elements` — CSS selectors, HTML snippets, or a
    natural-language description of the page structure — so the agent knows
    exactly what to target without any browser inspection.

    Auto-retries up to max_retries on failure, feeding error context back.
    """
    raw_dir = str(_abs(f"data/raw/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "scrape")))
    full_log: list[str] = []
    last_error = feedback

    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            yield "thought", f"🔄 Auto-retry {attempt}/{max_retries} — analysing error and fixing script…"

        task = (
            f"URL: {url}\n"
            f"Output directory: {raw_dir}\n"
            f"Script path: {script_path}\n\n"
            f"## What the user wants\n{description}\n\n"
            f"Use the description above to determine:\n"
            f"  - Which files/data to download\n"
            f"  - The output folder structure (e.g. year/month.pdf if mentioned)\n"
        )
        if html_elements.strip():
            task += (
                f"\n## Page elements provided by user\n"
                f"Parse these to find the file URLs and URL pattern:\n"
                f"{html_elements}\n"
                f"\nExtract all href URLs, identify the date/naming pattern, "
                f"and generate the COMPLETE list of files to download.\n"
            )
        else:
            task += "\nNo page elements provided — use fetch_url to examine the page and determine the best approach.\n"

        if last_error:
            task += (
                f"\n\nPREVIOUS ATTEMPT FAILED. Error details:\n{last_error}\n\n"
                "Analyse the error, fix the script, and try again."
            )

        if stop_event and stop_event.is_set():
            yield "stopped", "⏹ Stopped by user."
            return

        log = ""
        for event_type, content in _stream_agent(SCRAPER_PROMPT, task, stop_event=stop_event):
            if event_type == "done":
                log = content
                full_log.append(log)
            elif event_type == "stopped":
                yield event_type, content
                yield "result", StepResult(
                    success=False, files=[], previews={},
                    agent_log="\n\n--- retry ---\n\n".join(full_log),
                    error="Stopped by user.",
                )
                return
            else:
                yield event_type, content

        files = _collect_files(raw_dir)
        _dir_p = _abs(raw_dir)
        yield "thought", (
            f"📂 Checking `{_dir_p}` — "
            f"exists={_dir_p.exists()}, files_found={len(files)}"
        )
        if files:
            previews = _collect_previews(raw_dir)
            yield "result", StepResult(
                success=True,
                files=files,
                previews=previews,
                agent_log="\n\n--- retry ---\n\n".join(full_log),
            )
            return

        last_error = _extract_error(log)
        if attempt < max_retries:
            yield "thought", f"⚠️ Attempt {attempt} produced no output files. Error: {last_error[:300]}"

    yield "result", StepResult(
        success=False,
        files=[],
        previews={},
        agent_log="\n\n--- retry ---\n\n".join(full_log),
        error=f"Failed after {max_retries} attempts.\n\nLast error:\n{last_error}",
    )


def run_scraper(
    url: str,
    output_dir: str,
    description: str = "",
    script_path: str = "",
    feedback: str = "",
) -> StepResult:
    raw_dir = str(_abs(f"data/raw/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "scrape")))

    task = f"""Scrape data from: {url}
Description: {description}
Save output to: {raw_dir}
Write the script to: {script_path}
"""
    if feedback:
        task += f"\nPrevious feedback: {feedback}\nFix the issues described above."

    success, log = _run_agent(SCRAPER_PROMPT, task)
    files = _collect_files(raw_dir)
    previews = _collect_previews(raw_dir)

    return StepResult(
        success=success and bool(files),
        files=files,
        previews=previews,
        agent_log=log,
        error="" if success else log,
    )


# ── Extractor ──────────────────────────────────────────────────────────────────

def stream_extractor(
    output_dir: str,
    description: str = "",
    script_path: str = "",
    feedback: str = "",
    stop_event: "threading.Event | None" = None,
):
    """Stream extractor events then yield a final ('result', StepResult)."""
    raw_dir = str(_abs(f"data/raw/{output_dir}"))
    interim_dir = str(_abs(f"data/interim/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "extract")))
    task = f"Extract tables from raw files in: {raw_dir}\nDescription: {description}\nSave extracted tables to: {interim_dir}\nWrite the extraction script to: {script_path}\n"
    if feedback:
        task += f"\nPrevious feedback: {feedback}\nFix the issues described above."

    # Always clear interim before (re-)extraction — we re-extract from raw each time
    interim_path = _abs(interim_dir)
    if interim_path.exists():
        import shutil
        shutil.rmtree(interim_path)
    interim_path.mkdir(parents=True, exist_ok=True)

    log = ""
    for event_type, content in _stream_agent(EXTRACTOR_PROMPT, task, stop_event=stop_event):
        if event_type == "done":
            log = content
        elif event_type == "stopped":
            yield event_type, content
            yield "result", StepResult(
                success=False, files=[], previews={}, agent_log=log, error="Stopped by user."
            )
            return
        else:
            yield event_type, content

    files = _collect_files(interim_dir)
    previews = _collect_previews(interim_dir)
    yield "result", StepResult(
        success=bool(files),
        files=files, previews=previews, agent_log=log,
        error="" if files else "No files found after extraction.",
    )


def run_extractor(
    output_dir: str,
    description: str = "",
    script_path: str = "",
    feedback: str = "",
) -> StepResult:
    raw_dir = str(_abs(f"data/raw/{output_dir}"))
    interim_dir = str(_abs(f"data/interim/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "extract")))

    task = f"""Extract tables from raw files in: {raw_dir}
Description: {description}
Save extracted tables to: {interim_dir}
Write the extraction script to: {script_path}
"""
    if feedback:
        task += f"\nPrevious feedback: {feedback}\nFix the issues described above."

    success, log = _run_agent(EXTRACTOR_PROMPT, task)
    files = _collect_files(interim_dir)
    previews = _collect_previews(interim_dir)

    return StepResult(
        success=success and bool(files),
        files=files,
        previews=previews,
        agent_log=log,
        error="" if success else log,
    )


# ── Cleaner ────────────────────────────────────────────────────────────────────

def stream_cleaner(
    output_dir: str,
    description: str = "",
    script_path: str = "",
    feedback: str = "",
    single_file_only: bool = True,
    stop_event: "threading.Event | None" = None,
):
    """Stream cleaner events then yield a final ('result', StepResult)."""
    interim_dir = str(_abs(f"data/interim/{output_dir}"))
    processed_dir = str(_abs(f"data/processed/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "clean")))
    if feedback:
        # Re-run with feedback: work from the already-cleaned processed files, not interim
        task = (
            f"Current processed CSVs directory (already cleaned once): {processed_dir}\n"
            f"Output (processed) directory: {processed_dir}\n"
            f"Script path: {script_path}\n"
            f"Description: {description}\n"
            f"\nUser feedback on the previous result: {feedback}\n"
            "Read the existing processed CSV(s), apply the corrections described in the feedback, "
            "and overwrite them in place. Do NOT go back to the interim directory."
        )
    else:
        task = (
            f"Interim CSVs directory: {interim_dir}\n"
            f"Output (processed) directory: {processed_dir}\n"
            f"Script path: {script_path}\n"
            f"Description: {description}\n"
        )
        if single_file_only:
            task += (
                "\nIMPORTANT: Process ONE file only as a test:\n"
                "1. Pick the first CSV file in the interim directory.\n"
                "2. Write the cleaning script with write_file.\n"
                "3. Run it with run_script — the script MUST write the cleaned CSV to the output directory.\n"
                "   Always use df.to_csv(..., mode='w', index=False) — NEVER append mode.\n"
                "4. Verify the saved file exists using read_file.\n"
                "5. Stop — do NOT process remaining files.\n"
                "The cleaned file MUST be physically written to disk in the output directory."
            )
        else:
            task += (
                "\nProcess ALL files, concatenate, and save to the output directory.\n"
                "Always use df.to_csv(..., mode='w', index=False) — NEVER append mode."
            )

    # Only wipe processed dir on a fresh run — preserve it when re-running with feedback
    processed_path = _abs(processed_dir)
    if not feedback:
        if processed_path.exists():
            import shutil
            shutil.rmtree(processed_path)
    processed_path.mkdir(parents=True, exist_ok=True)

    log = ""
    for event_type, content in _stream_agent(CLEANER_PROMPT, task, stop_event=stop_event):
        if event_type == "done":
            log = content
        elif event_type == "stopped":
            yield event_type, content
            yield "result", StepResult(
                success=False, files=[], previews={}, agent_log=log, error="Stopped by user."
            )
            return
        else:
            yield event_type, content

    files = _collect_files(processed_dir)
    if not files:
        yield "thought", (
            f"⚠️ No files found in `{processed_dir}`. "
            f"Interim dir has {len(_collect_files(interim_dir))} file(s). "
            "The agent may have used a wrong path or the script failed silently."
        )
    previews = _collect_previews(processed_dir)
    yield "result", StepResult(
        success=bool(files),
        files=files, previews=previews, agent_log=log,
        error="" if files else f"No files found after cleaning in {processed_dir}.",
    )


def stream_chat_cleaner(
    output_dir: str,
    user_message: str,
    script_path: str = "",
):
    """Stream chat-based transformation events then yield a final ('result', StepResult)."""
    processed_dir = str(_abs(f"data/processed/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "transform")))
    task = (
        f"Processed data directory: {processed_dir}\n"
        f"Script path: {script_path}\n\n"
        f"User instruction: {user_message}\n\n"
        f"Apply the user's transformation to the CSV files in {processed_dir}."
    )

    log = ""
    for event_type, content in _stream_agent(CHAT_CLEANER_PROMPT, task):
        if event_type == "done":
            log = content
        else:
            yield event_type, content

    files = _collect_files(processed_dir)
    previews = _collect_previews(processed_dir)
    yield "result", StepResult(
        success=bool(files),
        files=files, previews=previews, agent_log=log,
        error="" if files else "No files found after transformation.",
    )


def run_cleaner(
    output_dir: str,
    description: str = "",
    script_path: str = "",
    feedback: str = "",
    single_file_only: bool = True,
) -> StepResult:
    interim_dir = str(_abs(f"data/interim/{output_dir}"))
    processed_dir = str(_abs(f"data/processed/{output_dir}"))
    script_path = str(_abs(script_path or _random_script(output_dir, "clean")))

    task = f"""Clean interim CSVs from: {interim_dir}
Description: {description}
Save cleaned output to: {processed_dir}
Write the cleaning script to: {script_path}
"""
    if single_file_only:
        task += "\nIMPORTANT: Run on a SINGLE file only. Stop after showing results — do not process all files."
    if feedback:
        task += f"\nPrevious feedback: {feedback}\nFix the issues described above."

    success, log = _run_agent(CLEANER_PROMPT, task)
    files = _collect_files(processed_dir)
    previews = _collect_previews(processed_dir)

    return StepResult(
        success=success and bool(files),
        files=files,
        previews=previews,
        agent_log=log,
        error="" if success else log,
    )


# ── Validator ───────────────────────────────────────────────────────────────────

def stream_validator(
    output_dir: str,
    description: str = "",
    script_path: str = "",
    feedback: str = "",
    stop_event: "threading.Event | None" = None,
):
    """Stream validator events then yield a final ('result', ValidationResult)."""
    import json as _json_mod

    processed_dir = str(_abs(f"data/processed/{output_dir}"))
    script_path_abs = _abs(script_path or _random_script(output_dir, "validate"))
    report_path = str(script_path_abs).replace(".py", "_report.json")

    task = (
        f"Processed CSVs directory: {processed_dir}\n"
        f"Script path: {str(script_path_abs)}\n"
        f"Validation report output: {report_path}\n"
        f"Description: {description}\n"
        "\nValidate the cleaned data following the steps in your instructions.\n"
        "The JSON report MUST be saved to the exact 'Validation report output:' path above."
    )
    if feedback:
        task += f"\nPrevious feedback: {feedback}\nAdjust the validation rules accordingly."

    log = ""
    for event_type, content in _stream_agent(VALIDATOR_PROMPT, task, stop_event=stop_event):
        if event_type == "done":
            log = content
        elif event_type == "stopped":
            yield event_type, content
            yield "result", ValidationResult(
                success=False, checks=[], agent_log=log, error="Stopped by user."
            )
            return
        else:
            yield event_type, content

    # Parse the JSON report written by the validation script
    checks = []
    try:
        rp = Path(report_path)
        if rp.exists():
            checks = _json_mod.loads(rp.read_text())
    except Exception:
        pass

    all_passed = bool(checks) and all(c.get("passed", False) for c in checks)
    yield "result", ValidationResult(
        success=all_passed,
        checks=checks,
        agent_log=log,
        error="" if checks else "No validation report found. Agent may have failed to write it.",
    )
