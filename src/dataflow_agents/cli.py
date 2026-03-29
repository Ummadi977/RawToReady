import typer
from rich.console import Console
from rich.panel import Panel

app = typer.Typer(help="dataflow-agents: scrape → extract → clean with human-in-the-loop verification")
console = Console()


@app.command()
def run(
    url: str = typer.Argument(..., help="Public URL to scrape data from"),
    output_dir: str = typer.Argument(..., help="Output directory name (used across all pipeline steps)"),
    description: str = typer.Option("", "--description", "-d", help="What the data represents"),
):
    """Run the full pipeline: scrape → extract → clean."""
    console.print(Panel.fit(
        f"[bold]dataflow-agents[/bold]\n\n"
        f"URL        : {url}\n"
        f"Output dir : {output_dir}\n"
        f"Description: {description or '(none)'}",
        title="Pipeline starting",
        border_style="blue",
    ))
    console.print("\nEach step will pause for your review before continuing.\n")

    from .graph import run_pipeline
    run_pipeline(url=url, output_dir=output_dir, description=description)


@app.command()
def scrape(
    url: str = typer.Argument(..., help="URL to scrape"),
    output_dir: str = typer.Argument(..., help="Output directory name"),
    description: str = typer.Option("", "--description", "-d"),
):
    """Run only the scraper step."""
    from .nodes.scraper import scraper_node
    from .state import PipelineState
    from langgraph.types import interrupt

    console.print(f"[blue]Scraping[/blue] {url} → data/raw/{output_dir}/\n")
    state: PipelineState = {
        "url": url, "output_dir": output_dir, "description": description,
        "scrape_approved": False, "extract_approved": False, "clean_approved": False,
        "feedback": "", "scrape_files": [], "extract_files": [], "clean_files": [],
        "messages": [],
    }
    scraper_node(state)


@app.command()
def extract(
    output_dir: str = typer.Argument(..., help="Output directory name"),
    description: str = typer.Option("", "--description", "-d"),
):
    """Run only the extractor step."""
    from .nodes.extractor import extractor_node
    from .state import PipelineState

    console.print(f"[blue]Extracting[/blue] data/raw/{output_dir}/ → data/interim/{output_dir}/\n")
    state: PipelineState = {
        "url": "", "output_dir": output_dir, "description": description,
        "scrape_approved": True, "extract_approved": False, "clean_approved": False,
        "feedback": "", "scrape_files": [], "extract_files": [], "clean_files": [],
        "messages": [],
    }
    extractor_node(state)


@app.command()
def clean(
    output_dir: str = typer.Argument(..., help="Output directory name"),
    description: str = typer.Option("", "--description", "-d"),
):
    """Run only the cleaner step."""
    from .nodes.cleaner import cleaner_node
    from .state import PipelineState

    console.print(f"[blue]Cleaning[/blue] data/interim/{output_dir}/ → data/processed/{output_dir}/\n")
    state: PipelineState = {
        "url": "", "output_dir": output_dir, "description": description,
        "scrape_approved": True, "extract_approved": True, "clean_approved": False,
        "feedback": "", "scrape_files": [], "extract_files": [], "clean_files": [],
        "messages": [],
    }
    cleaner_node(state)


def main():
    app()
