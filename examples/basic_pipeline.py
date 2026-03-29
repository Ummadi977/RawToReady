"""
Basic example: run the full pipeline on a public page with tables.

Usage:
    python examples/basic_pipeline.py

The pipeline pauses after each step and asks for your approval before
continuing. Type 'yes' to proceed, or describe what needs fixing to
have that step rerun with corrections.
"""

from dataflow_agents.graph import run_pipeline


if __name__ == "__main__":
    run_pipeline(
        url="https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)",
        output_dir="gdp_by_country",
        description="Table of countries ranked by nominal GDP in USD billions",
    )
