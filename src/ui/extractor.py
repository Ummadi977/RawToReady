"""Step 2 — Extract render function."""
import shutil

import streamlit as st

from ui.helpers import (
    PROJECT_ROOT,
    _app_rerun,
    _download_section,
    _drain_and_render,
    _fragment,
    _start_bg,
    show_result,
)
from ui.state import extract


@_fragment
def render_extract():
    st.subheader("Step 2 — Extract")
    st.caption("Extract structured tables from raw files and save to `data/interim/`")

    c1, c2 = st.columns([1, 1])
    with c2:
        extract_out = st.text_input(
            "Dataset path  *(name or subpath)*",
            value=st.session_state.output_dir,
            placeholder="e.g.  my_dataset  or  annual_pass/2026/Feb",
            key="_extract_out",
            help=(
                "Simple name → data/raw/<name>/\n"
                "Subpath → data/raw/<subpath>/  e.g. annual_pass/2026/Feb"
            ),
        )
    with c1:
        st.text_input(
            "Input directory  *(raw files)*",
            value=f"data/raw/{extract_out or '…'}",
            disabled=True,
        )

    with st.expander("📥 Upload files or folder  *(skips Step 1)*"):
        uploaded_raw = st.file_uploader(
            "Upload individual files  *(PDF, Excel, CSV)*",
            type=["pdf", "xlsx", "xls", "csv"],
            accept_multiple_files=True,
            key="_upload_raw",
        )
        if uploaded_raw and extract_out:
            _raw_dir = PROJECT_ROOT / "data" / "raw" / extract_out
            _raw_dir.mkdir(parents=True, exist_ok=True)
            for _uf in uploaded_raw:
                (_raw_dir / _uf.name).write_bytes(_uf.getvalue())
            st.success(f"Saved {len(uploaded_raw)} file(s) to `data/raw/{extract_out}/`")

        st.markdown("**Or specify a local file/folder path:**")
        _fp_col, _btn_col = st.columns([3, 1])
        with _fp_col:
            _raw_folder = st.text_input(
                "File/folder path",
                key="_raw_folder_path",
                placeholder="/path/to/your/folder",
                label_visibility="collapsed",
            )
        with _btn_col:
            _use_raw_folder = st.button("📂 Use file/folder", key="_use_raw_folder", use_container_width=True)
        if _use_raw_folder:
            if not _raw_folder:
                st.error("Enter a file/folder path.")
            else:
                from pathlib import Path as _Path
                _src = _Path(_raw_folder)
                if not _src.exists():
                    st.error(f"Path does not exist: `{_raw_folder}`")
                elif _src.is_file():
                    _name = extract_out or _src.stem
                    if not extract_out:
                        st.session_state.output_dir = _name
                    _raw_dir = PROJECT_ROOT / "data" / "raw" / _name
                    _raw_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(_src, _raw_dir / _src.name)
                    st.success(f"Copied `{_src.name}` → `data/raw/{_name}/`")
                else:
                    _name = extract_out or _src.name
                    if not extract_out:
                        st.session_state.output_dir = _name
                    _raw_dir = PROJECT_ROOT / "data" / "raw" / _name
                    _raw_dir.mkdir(parents=True, exist_ok=True)
                    _count = 0
                    for _f in _src.rglob("*"):
                        if _f.is_file():
                            _rel = _f.relative_to(_src)
                            (_raw_dir / _rel).parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(_f, _raw_dir / _rel)
                            _count += 1
                    st.success(f"Copied {_count} file(s) from `{_raw_folder}` → `data/raw/{_name}/`")

    with st.expander("📄 PDF extraction options  *(optional)*", expanded=False):
        st.caption("Leave all fields blank to let the agent auto-detect. Fill in only what you know.")
        _py_col, _pt_col, _pm_col = st.columns([1, 2, 1])
        with _py_col:
            _pdf_year = st.text_input(
                "Year filter",
                value=st.session_state.pdf_year,
                placeholder="all years",
                key="_pdf_year",
                help="Leave blank to process all years / all files.",
            )
        with _pt_col:
            _pdf_tables = st.text_input(
                "Table number(s) in PDF",
                value=st.session_state.pdf_tables,
                placeholder="e.g. 2.2  or  1.4 for 2016-2023, 2.2 for 2003-2015",
                key="_pdf_tables",
                help=(
                    "Table number to locate inside the PDF. "
                    "Use 'X for YEAR1-YEAR2, Y for YEAR3-YEAR4' for year-specific tables."
                ),
            )
        with _pm_col:
            _pdf_method = st.selectbox(
                "Extraction method",
                options=["auto", "lattice", "stream"],
                index=["auto", "lattice", "stream"].index(
                    st.session_state.pdf_method
                    if st.session_state.pdf_method in ("auto", "lattice", "stream")
                    else "auto"
                ),
                key="_pdf_method",
                help="lattice: bordered tables  |  stream: borderless  |  auto: try both",
            )
        st.session_state.pdf_year   = _pdf_year
        st.session_state.pdf_tables = _pdf_tables
        st.session_state.pdf_method = _pdf_method

    _extract_save_desc = st.text_area(
        "How should files be saved?  *(optional)*",
        value=st.session_state.extract_save_desc,
        placeholder=(
            "Describe the output structure, e.g.:\n"
            "• Save each year in a separate folder: year/output.csv\n"
            "• Multiple tables per file → table_1.csv, table_2.csv\n"
            "• Merge all years into a single output.csv\n"
            "• Name files after the sheet name"
        ),
        height=100,
        key="_extract_save_desc",
        help="The agent will follow this structure when saving interim CSV files.",
    )
    st.session_state.extract_save_desc = _extract_save_desc

    with st.expander("⚙️ Advanced — script path"):
        extract_script = st.text_input(
            "Extractor script path  *(leave blank for auto-generated name)*",
            value=st.session_state.extract_script,
            placeholder="auto-generated, e.g. src/pipelines/my_dataset/extract_a1b2c3d4.py",
            key="_extract_script",
        )

    run_col, stop_col, _ = st.columns([1, 1, 2])
    with run_col:
        run_extract = st.button(
            "▶ Run Extractor", type="primary", use_container_width=True,
            disabled=extract.running or False,
        )
    with stop_col:
        stop_extract = st.button(
            "⏹ Stop", use_container_width=True,
            disabled=not (extract.running or False),
            key="stop_extract_btn",
        )

    if stop_extract and extract.stop:
        extract.stop.set()
        extract.running = False
        st.rerun()

    if run_extract:
        if not extract_out:
            st.error("Dataset name is required.")
        else:
            from dataflow_agents.runner import stream_extractor as _stream_extractor

            _desc = st.session_state.description
            if st.session_state.extract_save_desc:
                _desc = (
                    (_desc + "\n\n" if _desc else "")
                    + "Output file structure instructions:\n"
                    + st.session_state.extract_save_desc
                )
            _pdf_parts = []
            if _pdf_year:
                _pdf_parts.append(f"Year filter: {_pdf_year}")
            if _pdf_tables:
                _pdf_parts.append(f"Table number(s): {_pdf_tables}")
            if _pdf_method and _pdf_method != "auto":
                _pdf_parts.append(f"Extraction method: {_pdf_method}")
            elif _pdf_method == "auto":
                _pdf_parts.append("Extraction method: auto (try lattice first, then stream)")
            if _pdf_parts:
                _desc = (_desc + "\n\n" if _desc else "") + "PDF options:\n" + "\n".join(
                    f"- {p}" for p in _pdf_parts
                )
            st.session_state.extract_script = extract_script
            _start_bg("extract", _stream_extractor,
                output_dir=extract_out, description=_desc,
                script_path=extract_script, feedback=extract.feedback or "",
            )
            extract.feedback = ""
            st.rerun()

    if extract.running or extract.events:
        _drain_and_render(
            "extract", "Extracting tables…", "Extraction complete ✓", "Extraction stopped / failed"
        )

    if extract.result is not None:
        show_result(extract.result, dir_label=f"data/interim/{extract_out}")

        if extract.result.success:
            _interim_abs = PROJECT_ROOT / "data" / "interim" / extract_out
            _download_section(_interim_abs, f"{extract_out}_interim")
            st.markdown("---")
            fb_col, btn_col = st.columns([3, 1])
            with fb_col:
                feedback = st.text_input(
                    "What needs fixing?",
                    key="extract_fb_input",
                    placeholder="e.g. 'table headers were not detected correctly'",
                )
            with btn_col:
                st.write("")
                st.write("")
                if st.button("↺ Re-extract", use_container_width=True) and feedback:
                    extract.feedback = feedback
                    extract.result   = None
                    st.rerun()

            if st.session_state.step == 2:
                if st.button("✓ Looks good — continue to Clean →", type="primary"):
                    st.session_state.step = 3
                    _app_rerun()
