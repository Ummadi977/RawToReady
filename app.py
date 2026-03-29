"""
dataflow-agents — Streamlit UI

Run with:
    streamlit run app.py
"""
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="dataflow-agents",
    page_icon="⚙️",
    layout="wide",
)

# ── Session state defaults ─────────────────────────────────────────────────────

DEFAULTS = {
    "step": 1,
    # Step 1 — Scrape
    "url": "",
    "output_dir": "",
    "description": "",
    "html_elements": "",
    "scrape_script": "",
    "scrape_result": None,
    "scrape_feedback": "",
    "scrape_running": False,
    "scrape_events": [],
    "scrape_queue": None,
    "scrape_stop": None,
    # Step 2 — Extract
    "extract_script": "",
    "extract_result": None,
    "extract_feedback": "",
    "extract_running": False,
    "extract_events": [],
    "extract_queue": None,
    "extract_stop": None,
    "pdf_year": "",
    "pdf_tables": "",
    "pdf_method": "auto",
    "extract_save_desc": "",
    # Step 3 — Clean
    "clean_script": "",
    "clean_result": None,
    "clean_feedback": "",
    "clean_all_done": False,
    "clean_approved": False,
    "clean_running": False,
    "clean_events": [],
    "clean_queue": None,
    "clean_stop": None,
    "clean_mode": "",
    "clean_chat_history": [],
    # Step 4 — Validate
    "validate_script": "",
    "validate_result": None,
    "validate_running": False,
    "validate_events": [],
    "validate_queue": None,
    "validate_stop": None,
    "validate_fb_counter": 0,
}

for key, val in DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ── Step modules ───────────────────────────────────────────────────────────────

from ui.scraper   import render_scrape
from ui.extractor import render_extract
from ui.cleaner   import render_clean
from ui.validator import render_validate

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("⚙️ dataflow-agents")
st.caption("Scrape public data → Extract tables → Clean & normalize → Validate")

# ── Step progress indicator ────────────────────────────────────────────────────

STEPS = ["Scrape", "Extract", "Clean", "Validate"]
current = st.session_state.step

cols = st.columns(len(STEPS))
for i, (col, label) in enumerate(zip(cols, STEPS), start=1):
    if i < current:
        if col.button(f"✅ {i}. {label}", key=f"nav_{i}", use_container_width=True):
            st.session_state.step = i
            st.rerun()
    elif i == current:
        col.button(f"▶ {i}. {label}", key=f"nav_{i}", use_container_width=True,
                   type="primary", disabled=True)
    else:
        col.button(f"○ {i}. {label}", key=f"nav_{i}", use_container_width=True,
                   disabled=True)

st.divider()

# ── Active step ────────────────────────────────────────────────────────────────

if current == 1:
    render_scrape()
elif current == 2:
    render_extract()
elif current == 3:
    render_clean()
elif current == 4:
    render_validate()

# ── Further transformations (chat — shown from step 3 onwards) ─────────────────

if current >= 3:
    st.divider()
    st.subheader("Further transformations")
    st.caption(
        "Describe any change to the cleaned data — "
        "e.g. *melt year columns into rows*, *rename X to Y*, *pivot by category*, *drop column Z*"
    )

    _chat_dir = st.session_state.get("_clean_out", "") or st.session_state.output_dir

    if not _chat_dir:
        _chat_dir = st.text_input("Dataset name", placeholder="e.g. annual", key="_chat_dir_input")

    if _chat_dir:
        for _msg in st.session_state.clean_chat_history:
            with st.chat_message(_msg["role"]):
                st.markdown(_msg["content"])

        if _user_chat := st.chat_input("What transformation do you want to apply?"):
            st.session_state.clean_chat_history.append({"role": "user", "content": _user_chat})

            from dataflow_agents.runner import stream_chat_cleaner
            _chat_result = None
            with st.status("Applying transformation…", expanded=True) as _chat_status:
                for _et, _ct in stream_chat_cleaner(output_dir=_chat_dir, user_message=_user_chat):
                    if _et == "tool_call":
                        st.markdown(f"&nbsp;&nbsp;`{_ct}`")
                    elif _et == "tool_result":
                        st.caption(f"&nbsp;&nbsp;{_ct}")
                    elif _et == "thought":
                        st.markdown(_ct)
                    elif _et == "error":
                        st.error(_ct)
                    elif _et == "result":
                        _chat_result = _ct
                        st.session_state.clean_result = _ct

                _ok = _chat_result and _chat_result.success
                _chat_status.update(
                    label="Transformation applied ✓" if _ok else "Transformation failed",
                    state="complete" if _ok else "error",
                    expanded=not _ok,
                )

            _reply = (
                f"Applied. {len(_chat_result.files)} file(s) updated in `data/processed/{_chat_dir}/`."
                if _ok else "Transformation failed — see agent log above."
            )
            st.session_state.clean_chat_history.append({"role": "assistant", "content": _reply})
            st.rerun()

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## Pipeline config")
    st.markdown(f"**Output dir:** `{st.session_state.output_dir or '—'}`")
    st.markdown(f"**Step:** {current} / {len(STEPS)}")

    st.divider()
    st.markdown("### Paths")
    _d = st.session_state.output_dir or "<dir>"
    st.markdown(f"Raw → `data/raw/{_d}/`")
    st.markdown(f"Interim → `data/interim/{_d}/`")
    st.markdown(f"Processed → `data/processed/{_d}/`")

    st.divider()
    if st.button("↺ Reset pipeline", use_container_width=True):
        for key in DEFAULTS:
            st.session_state[key] = DEFAULTS[key]
        st.rerun()

    st.divider()
    st.markdown("### LLM")
    from dataflow_agents.config import settings
    provider = (
        "Gemini"    if settings.google_api_key    else
        "OpenAI"    if settings.openai_api_key     else
        "Anthropic" if settings.anthropic_api_key  else
        "❌ No key found"
    )
    st.markdown(f"**Provider:** {provider}")
    st.markdown(f"**Model:** `{settings.dataflow_model}`")

    st.divider()
    if st.button("🗑 Clear chat history", use_container_width=True):
        st.session_state.clean_chat_history = []
        st.rerun()
