"""Step 1 — Scrape render function."""
import streamlit as st

from ui.helpers import _app_rerun, _drain_and_render, _fragment, _start_bg, show_result
from ui.state import scrape


@_fragment
def render_scrape():
    st.subheader("Step 1 — Scrape")
    st.caption("Fetch data from a public URL and save raw files to `data/raw/`")

    c1, c2 = st.columns([2, 1])
    with c1:
        url = st.text_input(
            "URL",
            value=st.session_state.url,
            placeholder="https://example.com/data",
            key="_url",
        )
    with c2:
        output_dir = st.text_input(
            "Output directory name",
            value=st.session_state.output_dir,
            placeholder="my_dataset",
            key="_output_dir",
        )

    description = st.text_input(
        "Description  *(what the data represents)*",
        value=st.session_state.description,
        placeholder="Annual GDP statistics by country",
        key="_description",
    )

    html_elements = st.text_area(
        "Page elements to scrape  *(paste CSS selectors, HTML snippets, or file URLs)*",
        value=st.session_state.html_elements,
        placeholder=(
            "Examples:\n"
            "• CSS selector:  table.data-table tr\n"
            "• File links:    a[href$='.pdf']\n"
            "• HTML snippet:  <a href='/reports/jan-2025.pdf'>Jan 2025</a>\n"
            "• Direct URLs:   https://example.com/data.xlsx\n"
            "Leave blank to let the agent figure it out from the page."
        ),
        height=140,
        key="_html_elements",
        help="Paste anything that identifies what you want — selectors, HTML, or URLs.",
    )

    with st.expander("⚙️ Advanced — script path"):
        scrape_script = st.text_input(
            "Scraper script path  *(leave blank for auto-generated name)*",
            value=st.session_state.scrape_script,
            placeholder="auto-generated, e.g. src/pipelines/my_dataset/scrape_a1b2c3d4.py",
            key="_scrape_script",
        )

    run_col, stop_col, _ = st.columns([1, 1, 2])
    with run_col:
        run_scrape = st.button(
            "▶ Run Scraper", type="primary", use_container_width=True,
            disabled=scrape.running or False,
        )
    with stop_col:
        stop_scrape = st.button(
            "⏹ Stop", use_container_width=True,
            disabled=not (scrape.running or False),
            key="stop_scrape_btn",
        )

    if stop_scrape and scrape.stop:
        scrape.stop.set()
        scrape.running = False
        st.rerun()

    if run_scrape:
        if not url or not output_dir:
            st.error("URL and output directory are required.")
        else:
            from dataflow_agents.runner import stream_scraper as _stream_scraper

            _script = scrape_script or f"src/pipelines/{output_dir}/scrape.py"
            st.session_state.url          = url
            st.session_state.output_dir   = output_dir
            st.session_state.description  = description
            st.session_state.html_elements = html_elements
            st.session_state.scrape_script = _script
            _start_bg("scrape", _stream_scraper,
                url=url, output_dir=output_dir, description=description,
                html_elements=html_elements, script_path=_script,
                feedback=scrape.feedback or "",
            )
            scrape.feedback = ""
            st.rerun()

    if scrape.running or scrape.events:
        _drain_and_render("scrape", "Scraping…", "Scrape complete ✓", "Scrape stopped / failed")

    if scrape.result is not None:
        show_result(scrape.result, dir_label=f"data/raw/{st.session_state.output_dir}")

        if scrape.result.success:
            st.markdown("---")
            fb_col, btn_col = st.columns([3, 1])
            with fb_col:
                feedback = st.text_input(
                    "What needs fixing? *(leave blank if it looks good)*",
                    key="scrape_fb_input",
                    placeholder="e.g. 'pagination was not followed'",
                )
            with btn_col:
                st.write("")
                st.write("")
                if st.button("↺ Re-scrape", use_container_width=True) and feedback:
                    scrape.feedback = feedback
                    scrape.result   = None
                    st.rerun()

            if st.session_state.step == 1:
                if st.button("✓ Looks good — continue to Extract →", type="primary"):
                    st.session_state.step = 2
                    _app_rerun()
