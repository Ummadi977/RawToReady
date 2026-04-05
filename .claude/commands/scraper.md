---

description : Extract structured data from public websites including HTML tables, paginated listings, PDFs, and downloadable files using Scrapy and Playwright MCP server.
argument-hint: url | output folder name | python file path | data to extract | pagination (yes/no) | file format (csv/json)

---

## context

Parse $Arguments to scrape structured data from the mentioned url and save it in the specified format.

- [url] : The public URL to scrape data from
- [output folder name] : Folder name where output files are stored
- [python file path] : Python file path to store the scraping code
- [data to extract] : Description of what data needs to be extracted (e.g. table, product listings, links, PDFs)
- [pagination] : Whether to handle pagination (yes/no, default: no)
- [file format] : Output file format (csv/json, default: csv)

## Task

Scrape structured data from [url] and save the extracted data to data/interim/[output folder name].

**The scraping script is ALWAYS written in Scrapy. Playwright MCP server is ONLY used to inspect the page before writing the Scrapy spider.**

### Step 1 — Inspect with Playwright MCP (always do this first)

Use Playwright MCP tools to explore the page and collect the information needed to write the Scrapy spider:

1. Navigate to [url] with `mcp__playwright__browser_navigate`.
2. Take a snapshot with `mcp__playwright__browser_snapshot` to see the page structure and identify CSS selectors / XPath for the target data.
3. If content is behind a click or scroll, use `mcp__playwright__browser_click` / `mcp__playwright__browser_scroll` to reveal it and snapshot again.
4. Identify:
   - CSS selectors or XPath for each field to extract.
   - Pagination pattern (next-button selector, URL query param, etc.).
   - Any request headers, cookies, or API endpoints the page uses (check `mcp__playwright__browser_network_requests`).
5. Once selectors and patterns are confirmed, close the browser and proceed to Step 2.

### Step 2 — Write the Scrapy spider

Using the selectors found in Step 1, create a Scrapy spider at [python file path]:

1. Use `Spider` for simple single-page or known-URL scraping; use `CrawlSpider` with `LinkExtractor` for multi-page crawls.
2. Use `ItemLoader` and `Item` classes to structure extracted fields.
3. For pagination — follow `next page` links or increment URL params in the `parse` method until exhausted.
4. For file downloads (PDF, Excel, CSV) — use Scrapy's `FilesPipeline`, save raw files to `data/raw/[output folder name]/`.
5. If the site requires specific headers or cookies identified in Step 1 — set them in `custom_settings`.
6. Export items using Scrapy's built-in feed exports (`FEEDS` setting) to `data/raw/[output folder name]/output.csv`.
7. Handle retries and HTTP errors via Scrapy's built-in middlewares.
8. Use `pandas` for any post-processing before saving the final CSV.

Create the python file at [python file path].
Save extracted data to data/raw/[output folder name].

## output

If the source contains **multiple sections or pages**, save each in a separate file:
`data/raw/[output folder name]/[section_name]/output.csv`

If the source contains a **single table or dataset**, save it as:
`data/raw/[output folder name]/output.csv`

For downloaded files (PDFs, Excel), save raw files in:
`data/raw/[output folder name]/`

## Review work

-- **Invoke data-scraper-engineer subagent** to review the extracted data and verify:
  - The data matches the source content on the website.
  - Output files are in the correct format and location.
  - Pagination was handled correctly (all pages captured).
  - No rows are missing or duplicated.
  - Correct tool was used (Scrapy vs Playwright MCP) for the page type.
-- Implement fixes if any issues are found.
-- Iterate on the review process when needed.
