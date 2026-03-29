# NOTE: This is an experimental LangGraph-based implementation with human-in-the-loop
# interrupts. The Streamlit app uses runner.py (manual threading + queues) instead.
# Kept here as a reference — not called by any production code path.

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from .state import PipelineState
from .nodes import scraper_node, extractor_node, cleaner_node


# --- Routing functions ---

def route_after_scraper(state: PipelineState) -> str:
    return "extractor" if state.get("scrape_approved") else "scraper"


def route_after_extractor(state: PipelineState) -> str:
    return "cleaner" if state.get("extract_approved") else "extractor"


def route_after_cleaner(state: PipelineState) -> str:
    return END if state.get("clean_approved") else "cleaner"


# --- Graph builder ---

def build_graph():
    workflow = StateGraph(PipelineState)

    workflow.add_node("scraper", scraper_node)
    workflow.add_node("extractor", extractor_node)
    workflow.add_node("cleaner", cleaner_node)

    workflow.set_entry_point("scraper")

    workflow.add_conditional_edges("scraper", route_after_scraper, {
        "extractor": "extractor",
        "scraper": "scraper",
    })
    workflow.add_conditional_edges("extractor", route_after_extractor, {
        "cleaner": "cleaner",
        "extractor": "extractor",
    })
    workflow.add_conditional_edges("cleaner", route_after_cleaner, {
        END: END,
        "cleaner": "cleaner",
    })

    memory = MemorySaver()
    return workflow.compile(checkpointer=memory)


# --- High-level entry point ---

def run_pipeline(url: str, output_dir: str, description: str = "") -> PipelineState:
    """
    Run the full pipeline interactively.

    Pauses at each step for human review. Type 'yes' to continue or
    describe the issue to have the step rerun with corrections.

    Args:
        url:         Public URL to scrape data from.
        output_dir:  Base folder name — data is staged under data/raw/,
                     data/interim/, and data/processed/.
        description: What the data represents (helps guide each agent).

    Returns:
        Final PipelineState after all steps complete.
    """
    from uuid import uuid4
    from langgraph.types import Command

    graph = build_graph()
    config = {"configurable": {"thread_id": str(uuid4())}}

    inputs: dict = {
        "url": url,
        "output_dir": output_dir,
        "description": description,
        "scrape_approved": False,
        "extract_approved": False,
        "clean_approved": False,
        "feedback": "",
        "scrape_files": [],
        "extract_files": [],
        "clean_files": [],
        "messages": [],
    }

    while True:
        # Run (or resume) the graph
        for _ in graph.stream(inputs, config, stream_mode="updates"):
            pass  # events are handled via interrupts below

        # Check graph state
        current = graph.get_state(config)

        if not current.next:
            # All steps approved — pipeline is done
            print("\nPipeline complete!")
            return current.values

        # There's a pending interrupt — show it and get user input
        for task in current.tasks:
            for intr in task.interrupts:
                print(f"\n{intr.value}\n")
                user_input = input("> ").strip()
                inputs = Command(resume=user_input)
                break
        else:
            # No interrupt found but graph still has next nodes — shouldn't happen
            break
