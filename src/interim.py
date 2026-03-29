import pandas as pd
from pathlib import Path

RAW_DIR    = "/Users/ummadi/Desktop/dataflow-agents/data/raw/disposal"
INTERIM_DIR = "/Users/ummadi/Desktop/dataflow-agents/data/interim/disposal"

for xls in Path(RAW_DIR).rglob("*.xlsx"):
    year = xls.stem.split('-')[-1] # Extract year from filename "Disposal of Persons Arrested for Crime and Atrocities against SCs (Crime Head-wise) -2022.xlsx"
    sheets = pd.read_excel(xls, sheet_name=None, header=[2,3,4])  # all sheets, with multi-index header
    for sheet_name, df in sheets.items():
        out = Path(INTERIM_DIR) / year / f"{sheet_name}.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        # Flatten multi-index header for cleaner CSV output
        df.columns = ['_'.join(col).strip() for col in df.columns.values]
        df = df.replace(r'\n', '', regex=True)
        df.to_csv(out, index=False)
        print(f"Saved {out} - {len(df)} rows")
