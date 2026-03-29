from pathlib import Path

from langgraph.prebuilt import create_react_agent
from langgraph.types import interrupt

from ..llm import get_llm
from ..tools import ALL_TOOLS
from ..prompts import CLEANER_PROMPT
from ..state import PipelineState


def cleaner_node(state: PipelineState) -> dict:
    output_dir = state["output_dir"]
    description = state.get("description", "")
    feedback = state.get("feedback", "")

    interim_dir = f"data/interim/{output_dir}"
    processed_dir = f"data/processed/{output_dir}"
    script_path = f"src/pipelines/{output_dir}/clean.py"

    task = f"""Clean interim CSVs from: {interim_dir}
Description: {description}
Save cleaned output to: {processed_dir}
Write the cleaning script to: {script_path}

IMPORTANT: Run on a single file first. Stop and report results — do not process all files yet.
"""
    if feedback:
        task += f"\nPrevious attempt feedback: {feedback}\nPlease fix the issues described above."

    llm = get_llm()
    agent = create_react_agent(llm, ALL_TOOLS, prompt=CLEANER_PROMPT)
    result = agent.invoke({"messages": [("user", task)]})

    from ..tools import list_files, read_file
    files_output = list_files.invoke({"directory": processed_dir})

    processed_path = Path(processed_dir)
    preview = ""
    if processed_path.exists():
        csvs = sorted(processed_path.rglob("*.csv"))
        if csvs:
            preview = read_file.invoke({"path": str(csvs[0])})

    decision = interrupt(
        f"--- Step 3: Clean (single-file test) complete ---\n\n"
        f"Output in {processed_dir}:\n{files_output}\n\n"
        f"Cleaned data preview:\n{preview}\n\n"
        f"Does the cleaned output look correct?\n"
        f"  Type 'yes' to process all remaining files\n"
        f"  Or describe what needs fixing to update the script and rerun"
    )

    approved = decision.strip().lower() in ("yes", "y", "ok")
    return {
        "clean_files": files_output.splitlines(),
        "clean_approved": approved,
        "feedback": "" if approved else decision,
        "messages": result["messages"],
    }
