from pathlib import Path

from langgraph.prebuilt import create_react_agent
from langgraph.types import interrupt

from ..llm import get_llm
from ..tools import ALL_TOOLS
from ..prompts import SCRAPER_PROMPT
from ..state import PipelineState


def scraper_node(state: PipelineState) -> dict:
    url = state["url"]
    output_dir = state["output_dir"]
    description = state.get("description", "")
    feedback = state.get("feedback", "")

    script_path = f"src/pipelines/{output_dir}/scrape.py"
    raw_dir = f"data/raw/{output_dir}"

    task = f"""Scrape data from: {url}
Description: {description}
Save output to: {raw_dir}
Write the spider/script to: {script_path}
"""
    if feedback:
        task += f"\nPrevious attempt feedback: {feedback}\nPlease fix the issues described above."

    llm = get_llm()
    agent = create_react_agent(llm, ALL_TOOLS, prompt=SCRAPER_PROMPT)
    result = agent.invoke({"messages": [("user", task)]})

    # Build a preview of what was saved
    from ..tools import list_files, read_file
    files_output = list_files.invoke({"directory": raw_dir})

    # Find first CSV for preview
    raw_path = Path(raw_dir)
    preview = ""
    if raw_path.exists():
        csvs = sorted(raw_path.rglob("*.csv"))
        if csvs:
            preview = read_file.invoke({"path": str(csvs[0])})

    # Interrupt for human review
    decision = interrupt(
        f"--- Step 1: Scrape complete ---\n\n"
        f"Files saved to {raw_dir}:\n{files_output}\n\n"
        f"Preview ({csvs[0].name if csvs else 'no CSV found'}):\n{preview}\n\n"
        f"Does the scraped data look correct?\n"
        f"  Type 'yes' to continue to extraction\n"
        f"  Or describe what needs fixing to re-scrape"
    )

    approved = decision.strip().lower() in ("yes", "y", "ok")
    return {
        "scrape_files": files_output.splitlines(),
        "scrape_approved": approved,
        "feedback": "" if approved else decision,
        "messages": result["messages"],
    }
