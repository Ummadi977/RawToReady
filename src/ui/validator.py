"""Step 4 — Validate render function."""
import streamlit as st

from ui.helpers import _fragment, _start_bg
from ui.state import validate


def _start_validate_bg(output_dir, description, script_path, feedback):
    from dataflow_agents.runner import stream_validator as _sv
    _start_bg("validate", _sv,
        output_dir=output_dir, description=description,
        script_path=script_path, feedback=feedback,
    )


@_fragment
def render_validate():
    st.subheader("Step 4 — Validate")
    st.caption("Run quality checks on the processed dataset and get a pass/fail report")

    _validate_out = st.session_state.output_dir
    if not _validate_out:
        st.info("Complete Steps 1–3 first, or set a dataset name.")
        return

    _vrun_col, _vstop_col, _ = st.columns([1, 1, 2])
    with _vrun_col:
        _vrun = st.button(
            "▶ Run validation", type="primary", use_container_width=True,
            disabled=validate.running or False,
        )
    with _vstop_col:
        _vstop = st.button(
            "⏹ Stop", use_container_width=True,
            disabled=not (validate.running or False),
            key="stop_validate_btn",
        )

    if _vstop and validate.stop:
        validate.stop.set()
        validate.running = False
        st.rerun()

    if _vrun:
        _start_validate_bg(
            output_dir=_validate_out,
            description=st.session_state.description,
            script_path=st.session_state.validate_script or "",
            feedback="",
        )
        st.rerun()

    # Drain queue + render — kept inline: three-way label logic (passed / issues found / failed)
    if validate.running or validate.events:
        import time

        vq = validate.queue
        if vq:
            while True:
                try:
                    ev = vq.get_nowait()
                except Exception:
                    break
                if ev is None:
                    validate.running = False
                    break
                et, content = ev
                if et == "result":
                    validate.result = content
                st.session_state["validate_events"].append((et, content))

        _vresult  = validate.result
        _vrunning = validate.running or False
        _vok      = _vresult and _vresult.success

        if _vrunning:
            _vlabel = "Validating…"
        else:
            _vlabel = "Validation passed ✓" if _vok else (
                "Validation complete — issues found"
                if (_vresult and _vresult.checks)
                else "Validation stopped / failed"
            )
        _vstate = "running" if _vrunning else ("complete" if _vok else "error")

        with st.status(_vlabel, expanded=not _vok, state=_vstate):
            for et, content in (validate.events or []):
                if et == "tool_call":
                    st.markdown(f"&nbsp;&nbsp;`{content}`")
                elif et == "tool_result":
                    st.caption(f"&nbsp;&nbsp;{content}")
                elif et in ("thought", "stopped"):
                    st.markdown(content)
                elif et == "error":
                    st.error(content)

        if _vrunning:
            time.sleep(0.4)
            st.rerun()

    _vr = validate.result
    if _vr is not None:
        if _vr.error and not _vr.checks:
            st.error(f"Validation failed.\n\n{_vr.error}")
        else:
            st.markdown("### Quality Report")
            _passed = sum(1 for c in _vr.checks if c.get("passed"))
            _failed = len(_vr.checks) - _passed

            _summary_col, _ = st.columns([2, 3])
            with _summary_col:
                if _failed == 0:
                    st.success(f"All {_passed} checks passed")
                else:
                    st.warning(f"{_passed} passed · {_failed} failed")

            for chk in _vr.checks:
                icon   = "✅" if chk.get("passed") else "❌"
                name   = chk.get("name", "check").replace("_", " ").title()
                detail = chk.get("detail", "")
                st.markdown(f"{icon} **{name}** — {detail}")

            with st.expander("🤖 Agent log", expanded=False):
                st.text(_vr.agent_log[:3000] + ("..." if len(_vr.agent_log) > 3000 else ""))

            st.markdown("---")
            _vfb_key = f"validate_fb_{st.session_state.get('validate_fb_counter', 0)}"
            _vfb = st.text_input(
                "Adjust validation rules:",
                key=_vfb_key,
                placeholder="e.g. 'allow nulls in district column' or 'add check that year >= 2010'",
            )
            if st.button("↺ Re-run with adjusted rules", use_container_width=True) and _vfb:
                st.session_state["validate_fb_counter"] = (
                    st.session_state.get("validate_fb_counter", 0) + 1
                )
                _start_validate_bg(
                    output_dir=_validate_out,
                    description=st.session_state.description,
                    script_path=st.session_state.validate_script or "",
                    feedback=_vfb,
                )
                st.rerun()

            if _failed == 0:
                st.success("🎉 Pipeline complete — data is validated and ready to use!")
