import json
import re
import shutil
from pathlib import Path

from langchain_core.tools import tool


# ── Inspect ────────────────────────────────────────────────────────────────────

@tool
def fetch_url(url: str) -> str:
    """Fetch a public URL and return up to 6000 characters of the HTML/text response.
    Use this to inspect a page before deciding which links to download."""
    import httpx
    try:
        r = httpx.get(url, follow_redirects=True, timeout=30,
                      headers={"User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)"})
        r.raise_for_status()
        return r.text[:6000]
    except Exception as e:
        return f"ERROR fetching {url}: {e}"


@tool
def fetch_page_links(url: str, extensions: str = "pdf,xlsx,xls,csv") -> str:
    """Fetch a webpage and return all links to downloadable files found on it.
    extensions: comma-separated list of file extensions to look for (default: pdf,xlsx,xls,csv).
    Returns one line per link: LINK_TEXT | FULL_URL
    Use this to discover what files are available before calling download_file."""
    import httpx
    from urllib.parse import urljoin, urlparse

    exts = [e.strip().lower().lstrip(".") for e in extensions.split(",")]
    pattern = r'href=["\']([^"\'#?\s]+\.(?:' + "|".join(re.escape(e) for e in exts) + r'))["\']'

    try:
        r = httpx.get(url, follow_redirects=True, timeout=30,
                      headers={"User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)"})
        r.raise_for_status()
        html = r.text
    except Exception as e:
        return f"ERROR fetching {url}: {e}"

    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    raw_links = re.findall(pattern, html, re.IGNORECASE)

    # Also try to grab link text from anchor tags
    anchor_pattern = r'<a[^>]+href=["\']([^"\'#?\s]+\.(?:' + "|".join(re.escape(e) for e in exts) + r'))["\'][^>]*>(.*?)</a>'
    text_map: dict[str, str] = {}
    for href, text in re.findall(anchor_pattern, html, re.IGNORECASE | re.DOTALL):
        clean_text = re.sub(r'<[^>]+>', '', text).strip()
        text_map[href] = clean_text

    seen: set[str] = set()
    lines: list[str] = []
    for href in raw_links:
        full_url = href if href.startswith("http") else urljoin(base + "/", href.lstrip("/"))
        if full_url not in seen:
            seen.add(full_url)
            label = text_map.get(href, "").strip() or Path(href).name
            lines.append(f"{label} | {full_url}")

    if not lines:
        return f"No {extensions} links found on {url}"
    return f"Found {len(lines)} link(s):\n" + "\n".join(lines)


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
    """Read a file and return its contents.
    For CSVs: returns column names, dtypes, shape, and a sample of up to 20 rows.
    For other files: returns the first 120 lines."""
    p = Path(path)
    if not p.exists():
        return f"File does not exist: {path}"

    if p.suffix.lower() == ".csv":
        try:
            import pandas as pd
            import numpy as np
            df = pd.read_csv(p, header=None, dtype=str, nrows=200)
            sample = pd.read_csv(p, header=None, dtype=str, nrows=20)
            out = [
                f"Shape (up to 200 rows read): {df.shape}",
                f"\nFirst 20 rows (raw, no header):",
                sample.to_string(index=True),
            ]
            return "\n".join(out)
        except Exception as e:
            return f"Could not parse CSV: {e}"

    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    preview = "\n".join(lines[:120])
    if len(lines) > 120:
        preview += f"\n... ({len(lines) - 120} more lines)"
    return preview


# ── Download ───────────────────────────────────────────────────────────────────

@tool
def download_file(url: str, save_path: str) -> str:
    """Download a single file from a URL and save it to save_path (absolute path).
    Creates parent directories automatically.
    Use this for every PDF, Excel, or CSV file you want to save.
    Returns file size on success, or an error message."""
    import httpx

    dest = Path(save_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.Client(follow_redirects=True, timeout=120,
                          headers={"User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)"},
                          verify=False) as client:
            r = client.get(url)
            r.raise_for_status()
            dest.write_bytes(r.content)
        return f"✓ Saved: {save_path}  ({len(r.content):,} bytes)"
    except Exception as e:
        return f"✗ Failed to download {url}: {e}"


@tool
def scrape_html_table(url: str, save_path: str, table_index: int = 0) -> str:
    """Extract an HTML table from a webpage and save it as a CSV file.
    table_index: which table on the page to extract (0 = first, -1 = largest by row count).
    Returns the shape and column names of the saved table.
    Use this when the data you need is in a <table> on the page (not a downloadable file)."""
    import httpx
    import pandas as pd

    dest = Path(save_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = httpx.get(url, follow_redirects=True, timeout=60,
                      headers={"User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)"},
                      verify=False)
        r.raise_for_status()
        tables = pd.read_html(r.text)
        if not tables:
            return f"No HTML tables found on {url}"
        if table_index == -1:
            df = max(tables, key=len)
        else:
            if table_index >= len(tables):
                return f"Only {len(tables)} table(s) on page; table_index {table_index} out of range"
            df = tables[table_index]
        df.to_csv(dest, index=False)
        return (f"✓ Saved table {table_index} → {save_path}\n"
                f"  Shape: {df.shape}\n"
                f"  Columns: {list(df.columns)}\n"
                f"  First 3 rows:\n{df.head(3).to_string(index=False)}")
    except Exception as e:
        return f"✗ Failed to scrape table from {url}: {e}"


# ── Extract ────────────────────────────────────────────────────────────────────

_LLM_EXTRACT_PROMPT = (
    "You are a table extraction assistant. Look at this PDF page image.\n"
    "Extract ALL tables visible on this page.\n\n"
    "For each table:\n"
    "1. Output exactly: === TABLE START ===\n"
    "2. Output the table as CSV (comma-separated; quote cells that contain commas or newlines)\n"
    "3. Output exactly: === TABLE END ===\n\n"
    "If no tables exist on this page output exactly: NO_TABLES\n\n"
    "Rules:\n"
    "- Include the header row as the first CSV row\n"
    "- Include every data row completely\n"
    "- Output raw CSV only between the markers — no extra commentary"
)


_RATE_LIMIT_KEYWORDS = ("429", "quota", "rate limit", "resource exhausted", "too many")


def _extract_pdf_with_llm(src: Path, out: Path) -> list[str]:
    """Render each PDF page as an image and ask the LLM to extract tables (sequential)."""
    import base64
    import re
    import time
    import random

    try:
        import fitz  # pymupdf
    except ImportError:
        return ["  ✗ pymupdf not installed — run: uv add pymupdf"]

    from langchain_core.messages import HumanMessage
    from .llm import get_llm

    llm = get_llm()
    doc = fitz.open(str(src))
    results: list[str] = []
    mat = fitz.Matrix(100 / 72, 100 / 72)  # 100 DPI — compact and still readable

    for pg_i, page in enumerate(doc, 1):
        pix = page.get_pixmap(matrix=mat)
        b64 = base64.b64encode(pix.tobytes("png")).decode()

        msg = HumanMessage(content=[
            {"type": "text", "text": _LLM_EXTRACT_PROMPT},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
        ])

        text = ""
        for attempt in range(5):
            try:
                response = llm.invoke([msg])
                text = response.content if hasattr(response, "content") else str(response)
                break
            except Exception as e:
                err = str(e).lower()
                if any(k in err for k in _RATE_LIMIT_KEYWORDS):
                    wait = (2 ** attempt) * 3 + random.uniform(0, 1)
                    time.sleep(wait)
                else:
                    results.append(f"  ⚠ LLM failed on page {pg_i}: {e}")
                    break
        else:
            results.append(f"  ⚠ Page {pg_i} skipped — rate limited after 5 retries")
            continue

        if not text or "NO_TABLES" in text:
            continue

        blocks = re.findall(
            r"=== TABLE START ===\s*(.*?)\s*=== TABLE END ===", text, re.DOTALL
        )
        for block_i, csv_text in enumerate(blocks, 1):
            csv_text = csv_text.strip()
            if not csv_text:
                continue
            suffix = f"page_{pg_i}" if len(blocks) == 1 else f"page_{pg_i}_table_{block_i}"
            dest = out / f"{suffix}.csv"
            dest.write_text(csv_text, encoding="utf-8")
            row_count = len(csv_text.splitlines())
            results.append(f"  ✓ [llm] page {pg_i} table {block_i} → {dest}  ({row_count} rows)")

    doc.close()
    if not results:
        results.append("  ⚠ LLM: no tables found on any page")
    return results


@tool
def extract_tables(file_path: str, output_dir: str, method: str = "auto") -> str:
    """Extract all tables from a PDF or Excel file and save them as CSVs.
    file_path: absolute path to the source file (.pdf, .xlsx, .xls, .csv)
    output_dir: directory where extracted CSVs will be saved
    method: 'auto' or 'llm' — converts each PDF page to an image and uses the LLM to
            extract tables (default); 'stream', 'lattice', 'pdfplumber' are legacy
            camelot/pdfplumber options only used when explicitly passed.

    For CSV files: copies the file directly.
    Returns a summary of every CSV saved (path, row count)."""
    src = Path(file_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ext = src.suffix.lower()

    if not src.exists():
        return f"ERROR: File not found: {file_path}"

    results: list[str] = []

    # ── CSV: copy as-is ────────────────────────────────────────────────────
    if ext == ".csv":
        dest = out / src.name
        shutil.copy2(src, dest)
        return f"✓ Copied CSV as-is → {dest}"

    # ── Excel ──────────────────────────────────────────────────────────────
    if ext in (".xlsx", ".xls"):
        try:
            import pandas as pd
            sheets = pd.read_excel(src, sheet_name=None, header=None)
            for sheet_name, df in sheets.items():
                if df.dropna(how="all").empty:
                    continue
                df = df.astype(str).replace("nan", "")
                df = df.apply(lambda c: c.str.replace(r"\n", " ", regex=True)
                              if c.dtype == object else c)
                dest = out / f"{sheet_name}.csv"
                df.to_csv(dest, index=False, header=False)
                results.append(f"  ✓ Sheet '{sheet_name}' → {dest}  {df.shape}")
            return (f"Extracted {len(results)} sheet(s) from {src.name}:\n"
                    + "\n".join(results)) if results else f"No non-empty sheets found in {src.name}"
        except Exception as e:
            return f"ERROR extracting Excel {src.name}: {e}"

    # ── PDF — LLM vision (default) ─────────────────────────────────────────
    if ext == ".pdf":
        if method in ("auto", "llm"):
            llm_results = _extract_pdf_with_llm(src, out)
            count = sum(1 for r in llm_results if "✓" in r)
            return (
                f"Extracted {count} table(s) from {src.name} using LLM vision:\n"
                + "\n".join(llm_results)
            )

        # Legacy explicit methods ──────────────────────────────────────────
        if method in ("stream", "lattice"):
            try:
                import camelot
                tables = camelot.read_pdf(str(src), pages="all", flavor=method)
                if tables.n > 0:
                    for i, tbl in enumerate(tables, 1):
                        name = "output" if tables.n == 1 else f"table_{i}"
                        dest = out / f"{name}.csv"
                        (tbl.df
                         .astype(str)
                         .apply(lambda c: c.str.replace(r"\n", " ", regex=True)
                                if c.dtype == object else c)
                         .to_csv(dest, index=False, header=False))
                        results.append(f"  ✓ [{method}] table {i} → {dest}  {tbl.df.shape}")
                    return (f"Extracted {len(results)} table(s) from {src.name} using camelot/{method}:\n"
                            + "\n".join(results))
                results.append(f"  ⚠ camelot/{method}: no tables found")
            except Exception as e:
                results.append(f"  ⚠ camelot/{method} failed: {e}")

        if method == "pdfplumber":
            try:
                import pdfplumber
                import pandas as pd
                found = 0
                with pdfplumber.open(str(src)) as pdoc:
                    for pg_i, page in enumerate(pdoc.pages, 1):
                        tbl = page.extract_table()
                        if tbl:
                            df = (pd.DataFrame(tbl)
                                  .astype(str)
                                  .replace("None", "")
                                  .apply(lambda c: c.str.replace(r"\n", " ", regex=True)
                                         if c.dtype == object else c))
                            dest = out / f"page_{pg_i}.csv"
                            df.to_csv(dest, index=False, header=False)
                            results.append(f"  ✓ [pdfplumber] page {pg_i} → {dest}  {df.shape}")
                            found += 1
                if found:
                    return (f"Extracted {found} table(s) from {src.name} using pdfplumber:\n"
                            + "\n".join(results))
                results.append("  ⚠ pdfplumber: no tables found on any page")
            except Exception as e:
                results.append(f"  ⚠ pdfplumber failed: {e}")

        return f"Could not extract tables from {src.name}:\n" + "\n".join(results)

    return f"Unsupported file type: {ext}"


# ── Clean / Transform ──────────────────────────────────────────────────────────

@tool
def transform_csv(input_path: str, output_path: str, operations: str) -> str:
    """Apply a sequence of cleaning or transformation operations to a CSV file.
    Reads input_path, applies operations in order, saves to output_path.

    operations: a JSON array string. Each element is an object with an "op" field.

    Available operations:
      {"op": "set_header", "row": 0}
          Use row N (0-indexed) as column headers; drop rows 0..N.

      {"op": "combine_headers", "rows": [0, 1]}
          Combine multiple rows into one header by joining non-empty values with a space.

      {"op": "drop_empty_rows"}
          Drop rows where all values are NaN or empty string.

      {"op": "drop_empty_cols"}
          Drop columns where all values are NaN or empty string.

      {"op": "strip_whitespace"}
          Strip leading/trailing whitespace and collapse internal spaces in all string cells.
          Also removes embedded newlines (\\n) and non-breaking spaces.

      {"op": "replace_missing"}
          Replace common missing-value strings ("-", "—", "N.A.", "NA", "n.a.", "Nil",
          "nil", "N/A", "") with NaN.

      {"op": "drop_rows", "column": "col_name", "values": ["Total", "S.No", ""]}
          Drop rows where the named column's value (case-insensitive strip) matches any
          of the given values. Use column index as string if headers not yet set ("0").

      {"op": "to_numeric", "columns": ["col1", "col2"]}
          Convert columns to numeric: removes commas, converts "(56)" to -56, coerces
          non-numeric to NaN.

      {"op": "title_case", "columns": ["State", "District"]}
          Apply str.title() to the listed columns.

      {"op": "rename_columns", "mapping": {"old_name": "new_name"}}
          Rename columns by the given mapping.

      {"op": "add_column", "name": "year", "value": "2024"}
          Add a new column with a constant value (inserted at position 0).

      {"op": "melt", "id_vars": ["State"], "var_name": "year", "value_name": "value"}
          Melt wide-format table to long format.

      {"op": "pivot", "index": "State", "columns": "year", "values": "value"}
          Pivot long-format table to wide format.

      {"op": "sort", "by": "year", "ascending": false}
          Sort by a column. ascending defaults to true.

      {"op": "dedup"}
          Drop exact duplicate rows.

      {"op": "filter_rows", "column": "col", "operator": ">", "value": 0}
          Keep only rows where column op value is true.
          Operators: ">", ">=", "<", "<=", "==", "!="

    Example operations JSON:
    [
      {"op": "set_header", "row": 0},
      {"op": "drop_empty_rows"},
      {"op": "drop_empty_cols"},
      {"op": "strip_whitespace"},
      {"op": "replace_missing"},
      {"op": "drop_rows", "column": "State", "values": ["Total", "Grand Total"]},
      {"op": "to_numeric", "columns": ["Value", "Count"]},
      {"op": "title_case", "columns": ["State"]},
      {"op": "add_column", "name": "year", "value": "2024"},
      {"op": "dedup"}
    ]

    Returns a before/after summary and a preview of the result."""
    import pandas as pd
    import numpy as np

    src = Path(input_path)
    dest = Path(output_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        return f"ERROR: File not found: {input_path}"

    try:
        ops = json.loads(operations)
    except json.JSONDecodeError as e:
        return f"ERROR: Invalid JSON in operations: {e}"

    try:
        df = pd.read_csv(src, header=None, dtype=str)
    except Exception as e:
        return f"ERROR reading {input_path}: {e}"

    before_shape = df.shape
    before_cols = list(df.columns)
    log: list[str] = [f"Loaded: {src.name}  shape={before_shape}"]

    MISSING_VALS = {"-", "—", "–", "N.A.", "NA", "n.a.", "Nil", "nil", "N/A", ""}

    for i, op in enumerate(ops):
        name = op.get("op", "")
        try:
            if name == "set_header":
                row = int(op.get("row", 0))
                df.columns = df.iloc[row].astype(str).str.strip()
                df = df.iloc[row + 1:].reset_index(drop=True)
                log.append(f"[{i}] set_header row={row} → columns: {list(df.columns[:6])}…")

            elif name == "combine_headers":
                rows = op.get("rows", [0, 1])
                header = []
                for col in df.columns:
                    parts = [str(df.iloc[r][col]).strip() for r in rows
                             if str(df.iloc[r][col]).strip() not in ("", "nan")]
                    header.append(" ".join(parts) if parts else str(col))
                df.columns = header
                df = df.iloc[max(rows) + 1:].reset_index(drop=True)
                log.append(f"[{i}] combine_headers rows={rows}")

            elif name == "drop_empty_rows":
                before = len(df)
                df.replace("", pd.NA, inplace=True)
                df.dropna(how="all", inplace=True)
                df.reset_index(drop=True, inplace=True)
                log.append(f"[{i}] drop_empty_rows: {before} → {len(df)} rows")

            elif name == "drop_empty_cols":
                before = len(df.columns)
                df.replace("", pd.NA, inplace=True)
                df.dropna(axis=1, how="all", inplace=True)
                log.append(f"[{i}] drop_empty_cols: {before} → {len(df.columns)} cols")

            elif name == "strip_whitespace":
                def _clean_str(s: str) -> str:
                    if not isinstance(s, str):
                        return s
                    return re.sub(r'\s+', ' ',
                                  s.replace('\n', ' ')
                                   .replace('\xa0', ' ')
                                   .replace('\t', ' ')
                                   .replace('*', '')).strip()
                df = df.applymap(_clean_str)  # type: ignore[attr-defined]
                log.append(f"[{i}] strip_whitespace")

            elif name == "replace_missing":
                df.replace(list(MISSING_VALS), np.nan, inplace=True)
                log.append(f"[{i}] replace_missing: replaced {list(MISSING_VALS)} → NaN")

            elif name == "drop_rows":
                col = str(op["column"])
                vals = {str(v).strip().lower() for v in op.get("values", [])}
                before = len(df)
                mask = df[col].astype(str).str.strip().str.lower().isin(vals)
                df = df[~mask].reset_index(drop=True)
                log.append(f"[{i}] drop_rows col='{col}' values={list(vals)}: "
                           f"{before} → {len(df)} rows")

            elif name == "to_numeric":
                cols = op.get("columns", [])
                for col in cols:
                    if col not in df.columns:
                        log.append(f"[{i}] to_numeric: column '{col}' not found, skipped")
                        continue
                    s = (df[col].astype(str)
                         .str.strip()
                         .str.replace(",", "", regex=False)
                         .str.replace(r"^\((.+)\)$", r"-\1", regex=True))
                    df[col] = pd.to_numeric(s, errors="coerce")
                log.append(f"[{i}] to_numeric cols={cols}")

            elif name == "title_case":
                cols = op.get("columns", [])
                for col in cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip().str.title()
                log.append(f"[{i}] title_case cols={cols}")

            elif name == "rename_columns":
                mapping = op.get("mapping", {})
                df.rename(columns=mapping, inplace=True)
                log.append(f"[{i}] rename_columns {mapping}")

            elif name == "add_column":
                col_name = op["name"]
                val = op["value"]
                df.insert(0, col_name, val)
                log.append(f"[{i}] add_column '{col_name}' = '{val}'")

            elif name == "melt":
                id_vars = op.get("id_vars", [])
                var_name = op.get("var_name", "variable")
                value_name = op.get("value_name", "value")
                df = pd.melt(df, id_vars=id_vars, var_name=var_name, value_name=value_name)
                log.append(f"[{i}] melt id_vars={id_vars} → shape={df.shape}")

            elif name == "pivot":
                idx = op["index"]
                cols = op["columns"]
                vals = op["values"]
                df = df.pivot(index=idx, columns=cols, values=vals).reset_index()
                df.columns.name = None
                log.append(f"[{i}] pivot index={idx} columns={cols} → shape={df.shape}")

            elif name == "sort":
                by = op["by"]
                asc = op.get("ascending", True)
                df.sort_values(by, ascending=asc, inplace=True)
                df.reset_index(drop=True, inplace=True)
                log.append(f"[{i}] sort by='{by}' ascending={asc}")

            elif name == "dedup":
                before = len(df)
                df.drop_duplicates(inplace=True)
                df.reset_index(drop=True, inplace=True)
                log.append(f"[{i}] dedup: {before} → {len(df)} rows")

            elif name == "filter_rows":
                col = op["column"]
                operator = op["operator"]
                val = op["value"]
                ops_map = {
                    ">": lambda a, b: a > b, ">=": lambda a, b: a >= b,
                    "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
                    "==": lambda a, b: a == b, "!=": lambda a, b: a != b,
                }
                before = len(df)
                mask = ops_map[operator](pd.to_numeric(df[col], errors="coerce"), val)
                df = df[mask].reset_index(drop=True)
                log.append(f"[{i}] filter_rows '{col}' {operator} {val}: "
                           f"{before} → {len(df)} rows")

            else:
                log.append(f"[{i}] UNKNOWN op '{name}' — skipped")

        except Exception as e:
            log.append(f"[{i}] ERROR in op '{name}': {e}")

    df.to_csv(dest, index=False, mode="w")
    after_shape = df.shape

    summary = "\n".join(log)
    summary += (
        f"\n\nBefore: {before_shape}  cols: {before_cols[:8]}"
        f"\nAfter:  {after_shape}  cols: {list(df.columns[:8])}"
        f"\n\nPreview (first 5 rows):\n{df.head(5).to_string(index=False)}"
        f"\n\nSaved → {output_path}"
    )
    return summary


# ── All tools exposed to the LLM agents ───────────────────────────────────────

ALL_TOOLS = [
    fetch_url,
    fetch_page_links,
    download_file,
    scrape_html_table,
    list_files,
    read_file,
    extract_tables,
    transform_csv,
]
