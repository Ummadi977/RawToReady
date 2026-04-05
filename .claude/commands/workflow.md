---
description: Run the full dataflow pipeline one step at a time — Scrape → Extract → Clean. Waits for user verification after each step before proceeding.
argument-hint: url | output_dir | description_of_data
---

## Context

Parse $Arguments:

- **[url]**: Public URL to scrape data from
- **[output_dir]**: Base output directory name used across all steps
- **[description_of_data]**: What the data represents (tables, PDFs, listings, etc.)

---

## Step 1 — Scrape

Run the scraper command with the following arguments:

```
[url] | [output_dir] | [python file path: src/pipelines/[output_dir]/scrape.py] | [description_of_data] | pagination: no | format: csv
```

Follow all instructions in `scraper.md` exactly.

After the scraper completes, show the user:
- Files saved to `data/raw/[output_dir]/` with sizes
- Preview of the first few rows from each downloaded file
- Any errors or skipped pages

**Pause and ask the user:**

> "Step 1 complete. Does the scraped data look correct?
> - **yes** — continue to Step 2
> - **no** — describe what's wrong and the scraper will rerun with corrections"

- If **yes**: proceed to Step 2
- If **no**: collect feedback, fix the scraper, and rerun Step 1 from the beginning

---

## Step 2 — Extract

Run the table extractor command with the following arguments:

```
[output_dir] | all years | [table number if applicable] | auto | [output_dir] | src/pipelines/[output_dir]/extract.py
```

Follow all instructions in `table_extractor.md` exactly.

After extraction completes, show the user:
- Number of tables extracted per file/year
- Column names and first few rows of each extracted table
- Any files where extraction failed or produced empty output

**Pause and ask the user:**

> "Step 2 complete. Do the extracted tables look correct?
> - **yes** — continue to Step 3
> - **no** — describe what's wrong and extraction will rerun with corrections"

- If **yes**: proceed to Step 3
- If **no**: collect feedback, fix the extraction settings or script, and rerun Step 2 from the beginning

---

## Step 3 — Clean

Run the cleaning command with the following arguments:

```
[output_dir] | [output_dir] | src/pipelines/[output_dir]/clean.py | simple
```

Follow all instructions in `data_cleaning.md` exactly.

The cleaning script runs on a **single file first** — show the user results before processing all files.

After the single-file test, show the user:
- Column names and dtypes
- `head(10)` of the cleaned output
- Unique values for any categorical columns
- Null counts per column

**Pause and ask the user:**

> "Step 3 (single-file test) complete. Does the cleaned output look correct?
> - **yes** — process all remaining files
> - **no** — describe what needs fixing and the cleaning script will be updated and rerun"

- If **yes**: run the cleaning script on all files, then report final row count and output file paths
- If **no**: collect feedback, update the script, and rerun Step 3 from the beginning

---

## Directory Layout After All Steps

```
data/
├── raw/[output_dir]/        # Step 1 — downloaded files
├── interim/[output_dir]/    # Step 2 — extracted tables
└── processed/[output_dir]/  # Step 3 — cleaned CSVs

src/
└── pipelines/[output_dir]/
    ├── scrape.py            # Scrapy spider (Step 1)
    ├── extract.py           # Table extraction script (Step 2)
    └── clean.py             # Cleaning script (Step 3)
```

## Rules

- Never skip a verification gate
- Never advance to the next step without explicit user approval
- When a step is rejected, always ask for specific feedback before rerunning
- If a step fails with an unrecoverable error, stop and report clearly — do not loop indefinitely
