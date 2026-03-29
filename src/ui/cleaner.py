"""Step 3 — Clean render function."""
import shutil

import pandas as pd
import streamlit as st

from ui.helpers import (
    PROJECT_ROOT,
    _app_rerun,
    _download_section,
    _drain_and_render,
    _fragment,
    _start_bg,
)
from ui.state import clean


def _clean_preview(clean_out: str):
    """Enhanced data preview for processed CSV(s)."""
    _pdir = PROJECT_ROOT / "data" / "processed" / clean_out
    _csvs = sorted(_pdir.rglob("*.csv")) if _pdir.exists() else []
    if not _csvs:
        st.warning("No CSV files found in processed directory yet.")
        return
    _tabs = st.tabs([f.name for f in _csvs[:5]])
    for _tab, _csv in zip(_tabs, _csvs[:5]):
        with _tab:
            try:
                _df = pd.read_csv(_csv)
                m1, m2, m3 = st.columns(3)
                m1.metric("Rows",       f"{len(_df):,}")
                m2.metric("Columns",    len(_df.columns))
                m3.metric("Null cells", int(_df.isnull().sum().sum()))
                with st.expander("Column info", expanded=False):
                    _info = pd.DataFrame({
                        "dtype":  _df.dtypes.astype(str),
                        "nulls":  _df.isnull().sum(),
                        "unique": _df.nunique(),
                    })
                    st.dataframe(_info, use_container_width=True)
                st.dataframe(_df.head(20), use_container_width=True)
            except Exception as _e:
                st.error(f"Could not read {_csv.name}: {_e}")


def _start_clean_bg(output_dir, description, script_path, feedback, single_file_only, mode):
    from dataflow_agents.runner import stream_cleaner as _sc
    clean.mode = mode
    _start_bg("clean", _sc,
        output_dir=output_dir, description=description,
        script_path=script_path, feedback=feedback,
        single_file_only=single_file_only,
    )


@_fragment
def render_clean():
    st.subheader("Step 3 — Clean")
    st.caption(
        "Normalize interim CSVs — fix types, strip whitespace, deduplicate. "
        "Runs on a **single file first** for review before processing all."
    )

    c1, c2 = st.columns([1, 1])
    with c2:
        clean_out = st.text_input(
            "Dataset path  *(name or subpath)*",
            value=st.session_state.output_dir,
            placeholder="e.g.  my_dataset  or  annual_pass/2026/Feb",
            key="_clean_out",
            help=(
                "Simple name → data/interim/<name>/\n"
                "Subpath → data/interim/<subpath>/  e.g. annual_pass/2026/Feb"
            ),
        )
    with c1:
        st.text_input(
            "Input directory  *(interim)*",
            value=f"data/interim/{clean_out or '…'}",
            disabled=True,
        )

    with st.expander("📥 Upload files or folder  *(skips Step 2)*"):
        uploaded_interim = st.file_uploader(
            "Upload individual CSV files",
            type=["csv"],
            accept_multiple_files=True,
            key="_upload_interim",
        )
        if uploaded_interim and clean_out:
            _interim_dir = PROJECT_ROOT / "data" / "interim" / clean_out
            _interim_dir.mkdir(parents=True, exist_ok=True)
            for _uf in uploaded_interim:
                (_interim_dir / _uf.name).write_bytes(_uf.getvalue())
            st.success(f"Saved {len(uploaded_interim)} file(s) to `data/interim/{clean_out}/`")

        st.markdown("**Or specify a local folder path:**")
        _ifp_col, _ibtn_col = st.columns([3, 1])
        with _ifp_col:
            _interim_folder = st.text_input(
                "Folder path",
                key="_interim_folder_path",
                placeholder="/path/to/your/csv/folder",
                label_visibility="collapsed",
            )
        with _ibtn_col:
            _use_interim_folder = st.button(
                "📂 Use folder", key="_use_interim_folder", use_container_width=True
            )
        if _use_interim_folder:
            if not _interim_folder:
                st.error("Enter a folder path.")
            else:
                from pathlib import Path as _Path
                _isrc = _Path(_interim_folder)
                if not _isrc.exists():
                    st.error(f"Path does not exist: `{_interim_folder}`")
                elif _isrc.is_file():
                    _iname = clean_out or _isrc.stem
                    if not clean_out:
                        st.session_state.output_dir = _iname
                    _interim_dir = PROJECT_ROOT / "data" / "interim" / _iname
                    _interim_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(_isrc, _interim_dir / _isrc.name)
                    st.success(f"Copied `{_isrc.name}` → `data/interim/{_iname}/`")
                else:
                    _iname = clean_out or _isrc.name
                    if not clean_out:
                        st.session_state.output_dir = _iname
                    _interim_dir = PROJECT_ROOT / "data" / "interim" / _iname
                    _interim_dir.mkdir(parents=True, exist_ok=True)
                    _icount = 0
                    for _f in _isrc.rglob("*.csv"):
                        if _f.is_file():
                            _rel = _f.relative_to(_isrc)
                            (_interim_dir / _rel).parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(_f, _interim_dir / _rel)
                            _icount += 1
                    st.success(
                        f"Copied {_icount} CSV file(s) from `{_interim_folder}` "
                        f"→ `data/interim/{_iname}/`"
                    )

    with st.expander("⚙️ Advanced — script path"):
        clean_script = st.text_input(
            "Cleaner script path  *(leave blank for auto-generated name)*",
            value=st.session_state.clean_script or "",
            placeholder="auto-generated, e.g. src/pipelines/my_dataset/clean_a1b2c3d4.py",
            key="_clean_script",
        )

    run_col, stop_col, _ = st.columns([1, 1, 2])
    with run_col:
        run_test = st.button(
            "▶ Run test  *(single file)*",
            type="primary",
            use_container_width=True,
            help="Clean one file and show a preview for review",
            disabled=clean.running or False,
        )
    with stop_col:
        stop_clean = st.button(
            "⏹ Stop", use_container_width=True,
            disabled=not (clean.running or False),
            key="stop_clean_btn",
        )

    if stop_clean and clean.stop:
        clean.stop.set()
        clean.running = False
        st.rerun()

    # Trigger: approval button was clicked → process all files
    if st.session_state.get("clean_approved") and not (clean.running or False):
        st.session_state["clean_approved"] = False
        st.session_state["clean_all_done"] = False
        _start_clean_bg(
            output_dir=clean_out,
            description=st.session_state.description,
            script_path=st.session_state.clean_script or "",
            feedback="",
            single_file_only=False,
            mode="all",
        )
        st.rerun()

    if run_test:
        if not clean_out:
            st.error("Dataset name is required.")
        else:
            _fb = clean.feedback or ""
            st.session_state["clean_script"]   = clean_script
            st.session_state["clean_all_done"] = False
            clean.feedback = ""
            _start_clean_bg(
                output_dir=clean_out,
                description=st.session_state.description,
                script_path=clean_script,
                feedback=_fb,
                single_file_only=True,
                mode="test",
            )
            st.rerun()

    if clean.running or clean.events:
        _mode        = clean.mode or "test"
        _running_lbl = "Cleaning (single-file test)…" if _mode == "test" else "Processing all files…"
        _done_lbl    = "Test complete ✓"              if _mode == "test" else "All files cleaned ✓"

        def _on_complete():
            if clean.mode == "all":
                st.session_state["clean_all_done"] = True

        _drain_and_render("clean", _running_lbl, _done_lbl, "Cleaning stopped / failed",
                          on_complete=_on_complete)

    if clean.result is not None:
        if not clean.result.success:
            st.error(f"Cleaning failed.\n\n{clean.result.error}")
            with st.expander("🤖 Agent log", expanded=False):
                log = clean.result.agent_log
                st.text(log[:3000] + ("..." if len(log) > 3000 else ""))
        else:
            st.markdown("### Preview")
            _clean_preview(clean_out)

            with st.expander("📁 Files  |  🤖 Agent log", expanded=False):
                st.code("\n".join(clean.result.files))
                st.divider()
                log = clean.result.agent_log
                st.text(log[:3000] + ("..." if len(log) > 3000 else ""))

            _processed_abs = PROJECT_ROOT / "data" / "processed" / clean_out
            _download_section(_processed_abs, f"{clean_out}_processed")
            st.markdown("---")

            if st.session_state.get("clean_all_done"):
                st.success("🎉 All files cleaned and saved to `data/processed/`")
                if st.button("▶ Proceed to Validate →", type="primary"):
                    st.session_state.step = 4
                    _app_rerun()
            else:
                st.markdown("**Does this look right?**")
                approve_col, feedback_col = st.columns([1, 2])
                with approve_col:
                    if st.button(
                        "✓ Approve — process all files",
                        type="primary",
                        use_container_width=True,
                    ):
                        st.session_state["clean_approved"] = True
                        st.rerun()
                with feedback_col:
                    _fb_key = f"clean_fb_input_{st.session_state.get('clean_fb_counter', 0)}"
                    _fb = st.text_input(
                        "Describe what needs fixing:",
                        key=_fb_key,
                        placeholder="e.g. 'numeric column still has commas'",
                    )
                    if st.button("↺ Re-run with feedback", use_container_width=True) and _fb:
                        st.session_state["clean_all_done"] = False
                        st.session_state["clean_fb_counter"] = (
                            st.session_state.get("clean_fb_counter", 0) + 1
                        )
                        _start_clean_bg(
                            output_dir=clean_out,
                            description=st.session_state.description,
                            script_path=st.session_state.clean_script or "",
                            feedback=_fb,
                            single_file_only=True,
                            mode="test",
                        )
                        st.rerun()
