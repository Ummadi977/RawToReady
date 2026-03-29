"""Shared UI utilities used by all step render functions."""
from io import StringIO
from pathlib import Path

import pandas as pd
import streamlit as st

# Project root: ui/helpers.py → ui/ → src/ → <project_root>
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# @st.fragment compatibility shim (requires Streamlit >= 1.33)
try:
    _fragment = st.fragment
except AttributeError:
    def _fragment(fn):  # no-op fallback for older Streamlit
        return fn


# ── Navigation ────────────────────────────────────────────────────────────────

def _app_rerun():
    """Full-app rerun compatible with Streamlit <1.37 and >=1.37."""
    try:
        st.rerun(scope="app")
    except TypeError:
        st.rerun()


def step_badge(n: int, label: str) -> str:
    current = st.session_state.step
    if n < current:
        return f"✅ {label}"
    elif n == current:
        return f"▶ {label}"
    else:
        return f"○ {label}"


# ── Background thread / queue ─────────────────────────────────────────────────

def _start_bg(step: str, runner_fn, **runner_kwargs):
    """Start runner_fn in a background thread and wire queue/stop/events into session state."""
    import queue as _q
    import threading as _t

    q    = _q.Queue()
    stop = _t.Event()
    st.session_state[f"{step}_queue"]   = q
    st.session_state[f"{step}_stop"]    = stop
    st.session_state[f"{step}_running"] = True
    st.session_state[f"{step}_result"]  = None
    st.session_state[f"{step}_events"]  = []

    def _bg():
        try:
            for ev in runner_fn(**runner_kwargs, stop_event=stop):
                q.put(ev)
        except Exception as exc:
            q.put(("error", f"Unexpected runner error: {exc}"))
        finally:
            q.put(None)  # always unblock the UI, even on crash

    _t.Thread(target=_bg, daemon=True).start()


def _drain_and_render(
    step: str,
    running_label: str,
    done_label: str,
    fail_label: str,
    on_complete=None,
):
    """Drain the step queue, render live events in st.status, and poll while running."""
    import time

    q = st.session_state.get(f"{step}_queue")
    if q:
        while True:
            try:
                ev = q.get_nowait()
            except Exception:
                break
            if ev is None:
                st.session_state[f"{step}_running"] = False
                if on_complete:
                    on_complete()
                break
            et, content = ev
            if et == "result":
                st.session_state[f"{step}_result"] = content
            st.session_state[f"{step}_events"].append((et, content))

    result  = st.session_state.get(f"{step}_result")
    running = st.session_state[f"{step}_running"]
    ok      = bool(result and result.success)
    label   = running_label if running else (done_label if ok else fail_label)
    state   = "running"     if running else ("complete"  if ok else "error")

    events = st.session_state[f"{step}_events"]
    thoughts = [(et, c) for et, c in events if et in ("thought", "stopped", "error")]
    tool_events = [(et, c) for et, c in events if et in ("tool_call", "tool_result")]

    with st.status(label, expanded=not ok, state=state):
        for et, content in thoughts:
            if et == "error":
                st.error(content)
            else:
                st.markdown(content)

        if tool_events:
            with st.expander("🔧 Tool calls", expanded=False):
                for et, content in tool_events:
                    if et == "tool_call":
                        st.markdown(f"`{content}`")
                    else:
                        st.caption(content)

    if running:
        time.sleep(0.4)
        st.rerun()


# ── Result display ────────────────────────────────────────────────────────────

def show_result(result, dir_label: str):
    """Display files list + CSV previews for a StepResult."""
    if result is None:
        return

    if not result.success:
        st.error(f"Step failed.\n\n{result.error}")
        return

    st.success(f"Complete — {len(result.files)} file(s) in `{dir_label}`")

    with st.expander("📁 Files", expanded=True):
        if result.files:
            st.code("\n".join(result.files))
        else:
            st.warning("No files found in output directory.")

    if result.previews:
        st.markdown("**Preview**")
        tabs = st.tabs(list(result.previews.keys())[:5])
        for tab, (name, text) in zip(tabs, list(result.previews.items())[:5]):
            with tab:
                try:
                    df = pd.read_csv(StringIO(text.split("\n\n", 1)[-1]))
                    st.dataframe(df, use_container_width=True)
                except Exception:
                    st.text(text)

    with st.expander("🤖 Agent log", expanded=False):
        st.text(result.agent_log[:3000] + ("..." if len(result.agent_log) > 3000 else ""))


def _download_section(abs_dir: Path, zip_name: str):
    """Render a ZIP download button (+ individual buttons for ≤10 files)."""
    import io as _io
    import zipfile

    files = sorted(f for f in abs_dir.rglob("*") if f.is_file()) if abs_dir.exists() else []
    if not files:
        return

    buf = _io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, f.relative_to(abs_dir))

    st.markdown("**Save files**")
    _dc, _ = st.columns([2, 3])
    with _dc:
        st.download_button(
            label=f"⬇ Download all as ZIP  ({len(files)} file{'s' if len(files) != 1 else ''})",
            data=buf.getvalue(),
            file_name=f"{zip_name}.zip",
            mime="application/zip",
            use_container_width=True,
            key=f"dlzip_{zip_name}",
        )

    if 1 < len(files) <= 10:
        with st.expander("Individual file downloads"):
            for i, f in enumerate(files):
                _mime = "text/csv" if f.suffix.lower() == ".csv" else "application/octet-stream"
                st.download_button(
                    label=f"⬇ {f.relative_to(abs_dir)}  ({f.stat().st_size:,} bytes)",
                    data=f.read_bytes(),
                    file_name=f.name,
                    mime=_mime,
                    key=f"dlind_{zip_name}_{i}",
                )
