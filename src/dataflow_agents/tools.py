import sys
import subprocess
from pathlib import Path

from langchain_core.tools import tool


@tool
def fetch_url(url: str) -> str:
    """Fetch a public URL and return the first 5000 characters of HTML for analysis."""
    import httpx
    try:
        response = httpx.get(url, follow_redirects=True, timeout=30)
        response.raise_for_status()
        return response.text[:5000]
    except Exception as e:
        return f"ERROR fetching {url}: {e}"


@tool
def write_file(path: str, content: str) -> str:
    """Write content to a file, creating parent directories as needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"Written: {path} ({len(content)} chars)"


@tool
def run_script(path: str) -> str:
    """Execute a Python script and return stdout + stderr (max 3000 chars)."""
    p = Path(path)
    if not p.exists():
        return f"ERROR: {path} does not exist"
    try:
        result = subprocess.run(
            [sys.executable, str(p)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = result.stdout
        if result.stderr:
            output += f"\nSTDERR:\n{result.stderr}"
        if result.returncode != 0:
            output += f"\nEXIT CODE: {result.returncode} (FAILED)"
        return (output or "(no output)")[:3000]
    except subprocess.TimeoutExpired:
        return "ERROR: Script timed out after 300 seconds"
    except Exception as e:
        return f"ERROR running script: {e}"


@tool
def list_files(directory: str) -> str:
    """List all files in a directory recursively with sizes."""
    p = Path(directory)
    if not p.exists():
        return f"Directory does not exist: {directory}"
    files = sorted(p.rglob("*"))
    lines = [
        f"  {f.relative_to(p)}  ({f.stat().st_size:,} bytes)"
        for f in files if f.is_file()
    ]
    return "\n".join(lines) if lines else f"(empty: {directory})"


@tool
def read_file(path: str) -> str:
    """Read a file and return first 100 lines. For CSVs, returns a formatted preview."""
    p = Path(path)
    if not p.exists():
        return f"File does not exist: {path}"

    if p.suffix.lower() == ".csv":
        try:
            import pandas as pd
            df = pd.read_csv(p, nrows=10)
            return f"Shape (preview): {df.shape}\n\n{df.to_string(index=False)}"
        except Exception as e:
            return f"Could not parse CSV: {e}"

    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    preview = "\n".join(lines[:100])
    if len(lines) > 100:
        preview += f"\n... ({len(lines) - 100} more lines)"
    return preview


# Expose as a list for easy import
ALL_TOOLS = [fetch_url, write_file, run_script, list_files, read_file]
