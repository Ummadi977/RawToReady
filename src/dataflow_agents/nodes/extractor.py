from pathlib import Path

from langgraph.prebuilt import create_react_agent
from langgraph.types import interrupt

from ..llm import get_llm
from ..tools import ALL_TOOLS
from ..prompts import EXTRACTOR_PROMPT
from ..state import PipelineState


def extractor_node(state: PipelineState) -> dict:
    output_dir = state["output_dir"]
    description = state.get("description", "")
    feedback = state.get("feedback", "")

    raw_dir = f"data/raw/{output_dir}"
    interim_dir = f"data/interim/{output_dir}"
    script_path = f"src/pipelines/{output_dir}/extract.py"

    task = f"""Extract tables from raw files in: {raw_dir}
Description: {description}
Save extracted tables to: {interim_dir}
Write the extraction script to: {script_path}
"""
    if feedback:
        task += f"\nPrevious attempt feedback: {feedback}\nPlease fix the issues described above."

    llm = get_llm()
    agent = create_react_agent(llm, ALL_TOOLS, prompt=EXTRACTOR_PROMPT)
    result = agent.invoke({"messages": [("user", task)]})

    from ..tools import list_files, read_file
    files_output = list_files.invoke({"directory": interim_dir})

    interim_path = Path(interim_dir)
    preview = ""
    if interim_path.exists():
        csvs = sorted(interim_path.rglob("*.csv"))
        if csvs:
            preview = read_file.invoke({"path": str(csvs[0])})

    decision = interrupt(
        f"--- Step 2: Extract complete ---\n\n"
        f"Tables extracted to {interim_dir}:\n{files_output}\n\n"
        f"Preview ({csvs[0].name if csvs else 'no CSV found'}):\n{preview}\n\n"
        f"Do the extracted tables look correct?\n"
        f"  Type 'yes' to continue to cleaning\n"
        f"  Or describe what needs fixing to re-extract"
    )

    approved = decision.strip().lower() in ("yes", "y", "ok")
    return {
        "extract_files": files_output.splitlines(),
        "extract_approved": approved,
        "feedback": "" if approved else decision,
        "messages": result["messages"],
    }
