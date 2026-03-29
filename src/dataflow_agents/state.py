from typing import Annotated
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages


class PipelineState(TypedDict):
    # Inputs
    url: str
    output_dir: str
    description: str

    # Step results
    scrape_files: list[str]
    extract_files: list[str]
    clean_files: list[str]

    # Human-in-the-loop
    scrape_approved: bool
    extract_approved: bool
    clean_approved: bool
    feedback: str

    # Agent message history
    messages: Annotated[list, add_messages]
