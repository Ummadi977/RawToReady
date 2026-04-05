---

description : Clean interim datasets and create processed datasets. Reads interim CSVs, applies cleaning operations, standardizes values, and saves to data/processed/.
argument-hint: interim folder path | output folder path | python file path | cleaning type (simple/complex)
---

## context

Parse $Arguments to clean interim datasets and create processed datasets.

- [interim folder path] : Path to interim folder containing extracted CSVs (under data/interim/)
- [output folder path] : Path to processed output folder (under data/processed/)
- [python file path] : Python file path to store the cleaning script
- [cleaning type] : simple or complex — determines the level of cleaning needed

## Task

### Step 1: Examine interim data structure

- List all year folders under `data/interim/[interim folder path]`
- Read a sample CSV from the **earliest year** and **latest year** folder to understand:
  - Column count and layout
  - Where headers are (which rows)
  - Where data starts (first state/city row) and ends (total row)
  - Whether it has serial numbers, multi-row headers, hierarchical categories
  - Whether the table is state-level, city-level, or head-wise
- Print the first 10-15 rows of both samples so the structure is visible

### Step 2: Read and understand the matching reference script

Based on the [cleaning type], read the corresponding reference file:

**For complex cleaning (head-wise, multi-category with melting):**
`projects/ncrb/src/data/crime_in_india/cyber_crimes/state/court_disposals_head_wise/create_dataset.py`

**For city/metropolitan cities cleaning (melting + city-state mapping):**
`projects/ncrb/src/data/crime_in_india/cyber_crimes/city/court_disposal/create_dataset.py`

**For state cleaning:**
`projects/ncrb/src/data/crime_in_india/cyber_crimes/state/court_disposal/create_dataset.py`

**For basic text preprocessing:**
`projects/fcra-home-affairs/src/data/1_fcra_filed_annual_returns/cleaning_dataset.py`

### Step 3: Create initial cleaning script

Create the Python script at [python file path] with:
- Correct `parents[N]` based on script depth
- Correct interim and output folder paths
- A `crimes(df, year)` cleaning function based on the reference pattern, adapted to the actual interim data structure observed in Step 1
- The full processing loop, standardization, and save logic


### Cleaning Operations to Apply

Based on the project's established patterns, apply these cleaning steps as needed:

#### 1. Read Interim Data
```python
import os
from pathlib import Path
import pandas as pd

project_dir = Path(__file__).resolve().parents[N]  # adjust N based on script depth

parent_folder = project_dir / "data/interim/<interim_folder>"
result_dir = project_dir / "data/processed/<output_folder>"
result_dir.mkdir(parents=True, exist_ok=True)

for file in os.listdir(parent_folder):
    file_path = Path.joinpath(parent_folder, file)
    for page in file_path.glob("**/*.csv"):
        df = pd.read_csv(page)
```

#### 2. Remove Junk Rows and Columns
- Drop all-null rows/columns: `df.dropna(how='all', axis=0)` and `df.dropna(how='all', axis=1)`
- Find header start row using keyword search (e.g., "state", "table")
- Find data start row by searching for known values (e.g., "Andhra Pradesh", "1 Andhra Pradesh")
- Find data end row by searching for total rows (e.g., "TOTAL ALL INDIA", "TOTAL (CITIES)")
- Remove intermediate total rows (e.g., "TOTAL UT(S)", "TOTAL STATE(S)")


#### 4. Clean Text Values
- Remove newlines: `df.replace(r'\n', '', regex=True)`
- Remove `\xa0` (non-breaking spaces): `df.replace(r'\xa0', ' ', regex=True)`
- Strip whitespace: `df.applymap(lambda x: x.strip() if isinstance(x, str) else x)`
- Collapse multiple spaces: `re.sub(r'\s{2,}', ' ', x)`
- Remove special characters: `df['col'].str.replace(r'^\d+', '').str.strip()`
- Title case: `.str.title()`
- Remove asterisks, parenthetical annotations: `.replace('*', '')`, `re.sub(r'\([^)]*\)', '', x)`

#### 5. Clean Numeric Values
- Remove commas: `x.replace(',', '')`
- Replace dash/em-dash with NA: `df.replace(['-', '\u2014'], pd.NA)`
- Convert to numeric: `pd.to_numeric(df[col], errors='coerce')`
- Round floats: `x.apply(lambda v: round(v, 2) if not pd.isna(v) else v)`

#### 6. Standardize State Names
Use the shared `factly.standard_names.states` library:
```python
from factly.standard_names.states import state_std_names

df = state_std_names(
    dfObj=df,
    column_name="state",
    thresh=70,
    manual_changes={
        "D&N Haveli and Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu",
        "Orissa": "Odisha",
    },
    identifier="state_name_changes",
)
```


#### 8. Reshape Data (Wide to Long)
For tables with categories spread across columns:
```python
melted_df = pd.melt(
    df,
    id_vars=["year", "state"],
    value_vars=category_columns,
    var_name="category",
)
```

Or use stacking for multi-level headers:
```python
df.set_index(df.columns.to_list()[:index_cols], inplace=True)
stack_df = df.stack(level=list(range(df.columns.nlevels))).reset_index()
```

#### 9. Add Metadata Columns
- Add `year` column from folder name or filename
- Add `unit` column describing the measurement unit
- Add `note` column for any special annotations
- Reorder columns to standard order:
  ```python
  df = df.reindex(columns=['year', 'state', 'category', 'sub_category', 'value', 'unit', 'note'])
  ```

#### 10. Deduplication and Sorting
```python
df = df.drop_duplicates()
df = df.sort_values(by="year", ascending=False)
```

#### 11. Save Output
Follow the append-or-create pattern:
```python
csv_path = result_dir / "output.csv"
if csv_path.exists():
    old_df = pd.read_csv(csv_path)
    final_df = pd.concat([old_df, df])
    final_df = final_df.drop_duplicates()
    final_df = final_df.sort_values(by="year", ascending=False)
    final_df.to_csv(csv_path, index=False)
else:
    df.to_csv(csv_path, index=False)
```

For splitting All India vs state-level data into separate files:
```python
all_india_df = df[df['state'] == 'All India']
other_df = df[df['state'] != 'All India']
all_india_df.to_csv(result_dir / 'all_india' / 'output.csv', index=False)
other_df.to_csv(result_dir / 'other_states' / 'output.csv', index=False)
```

## output

output files should be in the following format:

- Single output: `data/processed/[output folder]/output.csv`
- Split by category: `data/processed/[output folder]/[category]/output.csv`
- Split by all_india vs states: `data/processed/[output folder]/all_india/output.csv` and `data/processed/[output folder]/other_states/output.csv`

## Review work

-- **Invoke data-cleaning subagent** to review the cleaned datasets and verify correctness. Check both the processed output CSVs and the Python script are stored in the correct format and location. Implement changes if required.
-- Iterate on the review process when needed.

### Review checklist

1. **File completeness**: Check that all interim CSVs have been processed and no year folders were skipped. Compare the list of year folders in `data/interim/[folder]` against the years present in the output CSV.
2. **State name standardization**: Verify `state_std_names()` was applied — check for duplicate state entries caused by inconsistent naming (e.g., "Orissa" vs "Odisha", "D&N Haveli" variants). Run `df['state'].unique()` and compare against the standard names list.
3. **City-state mapping** (for city-level data): Verify all cities were successfully merged with `data/external/city_state_names.csv` — check for NaN values in the `state` column after the merge.
4. **Numeric conversion**: Verify numeric columns contain no string values. Check that `-`, `@`, `*`, and empty strings were properly handled (converted to NaN or noted in `note` column).
5. **Category standardization**: Verify category/crime_head/crime_type values are standardized — no year-specific suffixes, no parenthetical annotations, no inconsistent naming across years.
6. **No duplicate rows**: Run `df.duplicated().sum()` and confirm it returns 0.
7. **Output structure**: Confirm files are saved in the correct location:
   - State-level: `data/processed/[folder]/all_india/output.csv` and `data/processed/[folder]/other_states/output.csv`
   - City-level: `data/processed/[folder]/output.csv`
   - Head-wise: `data/processed/[folder]/output.csv`
8. **Script location**: Confirm the Python script is saved at the specified `[python file path]` and follows the reference pattern (correct `parents[N]`, correct imports, correct folder paths).
