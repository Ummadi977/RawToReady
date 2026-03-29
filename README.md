# dataflow-agents

A multi-agent pipeline for scraping, extracting, and cleaning structured data — powered by [LangGraph](https://github.com/langchain-ai/langgraph) and your choice of LLM (Gemini, OpenAI, or Anthropic).

```
Input URL
    ↓
[Scraper]       → Downloads HTML tables, PDFs, Excel, CSV  →  data/raw/
    ↓  (user verifies)
[Extractor]     → Extracts tables from raw files           →  data/interim/
    ↓  (user verifies)
[Cleaner]       → Normalizes, deduplicates, standardizes   →  data/processed/
```

Each step pauses for user verification before proceeding. Available as a **Streamlit UI**, **CLI**, or **Claude Code commands**.

## Quickstart

### 1. Clone and install

```bash
git clone https://github.com/your-username/dataflow-agents
cd dataflow-agents
uv sync --extra gemini   # or --extra openai / --extra anthropic
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env and add your API key (GOOGLE_API_KEY, OPENAI_API_KEY, or ANTHROPIC_API_KEY)
```

### 3. Run

```bash
# Streamlit UI (recommended)
streamlit run app.py

# or CLI
dataflow run https://example.com/data my_dataset
```

## Usage

### Streamlit UI

```bash
streamlit run app.py
```

Step-by-step browser wizard:
1. **Scrape** — enter URL and output directory → run → review files + CSV preview → continue or re-run with feedback
2. **Extract** — paths pre-filled from Step 1 → run → review extracted tables → continue or re-run
3. **Clean** — single-file test first → review cleaned output → process all files when ready

### CLI

```bash
# Full pipeline (interactive — pauses at each step)
dataflow run <url> <output_dir> --description "what the data is"

# Individual steps
dataflow scrape  <url> <output_dir>
dataflow extract <output_dir>
dataflow clean   <output_dir>
```

### Claude Code commands *(optional, requires Claude Code)*

```
/workflow        <url> | <output_dir> | <description>
/scraper         <url> | <output_dir> | <script_path> | <what_to_extract>
/table_extractor <folder> | <years> | <table_number> | <method> | <output_folder> | <script_path>
/data_cleaning   <interim_folder> | <output_folder> | <script_path> | <cleaning_type>
```

## Directory Layout

```
data/
├── raw/          # Downloaded files (scraper output)
├── interim/      # Extracted tables (extractor output)
└── processed/    # Clean CSVs (cleaner output)

src/
└── pipelines/    # Generated Python scripts per dataset
    └── <output_dir>/
        ├── scrape.py
        ├── extract.py
        └── clean.py
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GOOGLE_API_KEY` | — | Gemini — get one at [ai.google.dev](https://ai.google.dev/) |
| `OPENAI_API_KEY` | — | OpenAI alternative |
| `ANTHROPIC_API_KEY` | — | Anthropic alternative |
| `DATAFLOW_MODEL` | `gemini-2.0-flash` | Model name to use |
| `DATAFLOW_LOG_LEVEL` | `INFO` | Log verbosity |
| `LANGSMITH_API_KEY` | — | Optional LangSmith tracing |

## Requirements

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- One LLM API key (Gemini, OpenAI, or Anthropic)

## Development

```bash
uv run pytest        # Run tests
uv run ruff check .  # Lint
```

## License

MIT
