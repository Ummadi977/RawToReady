"""Typed proxies over st.session_state — one per pipeline step.

Instead of raw string keys like st.session_state["scrape_running"],
use  scrape.running  with IDE autocomplete and AttributeError on typos.
"""
import streamlit as st


class _StepState:
    """Proxy that maps attribute access to st.session_state[<prefix>_<name>].

    Created once at module level; reads/writes happen only at access time
    so it is safe to import before the Streamlit session is active.
    """

    def __init__(self, prefix: str):
        object.__setattr__(self, "_prefix", prefix)

    def __getattr__(self, name: str):
        key = f"{object.__getattribute__(self, '_prefix')}_{name}"
        return st.session_state.get(key)

    def __setattr__(self, name: str, value):
        key = f"{object.__getattribute__(self, '_prefix')}_{name}"
        st.session_state[key] = value


# Module-level singletons — import whichever you need in a step file
scrape   = _StepState("scrape")
extract  = _StepState("extract")
clean    = _StepState("clean")
validate = _StepState("validate")
