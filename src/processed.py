import pandas as pd
import numpy as np
import os

def clean_data(input_file_path, output_file_path):
    # Read the CSV file, assuming the first row is the header
    df = pd.read_csv(input_file_path, header=0)

    # Extract the year from the file path
    year = input_file_path.split('/')[-2]

    # Clean up column names
    new_columns = []
    for col in df.columns:
        cleaned_col = col.split('_[')[0].strip()
        # Also clean up 'Unnamed' from SL and Crime Head if present
        if 'SL_Unnamed' in cleaned_col:
            new_columns.append('SL')
        elif 'Crime Head_Unnamed' in cleaned_col:
            new_columns.append('Crime Head')
        else:
            new_columns.append(cleaned_col)

    df.columns = new_columns

    # Drop duplicate 'SL' and 'Crime Head' columns, keeping the first occurrence
    df = df.loc[:, ~df.columns.duplicated()]

    # Add the 'Year' column
    df['Year'] = year

    # Drop rows where 'Crime Head' is null (these are likely junk rows or empty separators)
    df.dropna(subset=['Crime Head'], inplace=True)

    # Clean 'Crime Head' column: strip whitespace, remove special characters
    df['Crime Head'] = df['Crime Head'].astype(str).str.strip().str.replace(r'[\n\xa0\*]', '', regex=True)

    # Identify numeric columns (all columns except 'SL', 'Crime Head', 'Year')
    numeric_cols = [col for col in df.columns if col not in ['SL', 'Crime Head', 'Year']]

    for col in numeric_cols:
        # Convert to numeric, handling commas and '-'
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.replace('-', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows that are entirely null in the numeric columns (after conversion)
    df.dropna(how='all', subset=numeric_cols, inplace=True)

    # Reset index after dropping rows
    df.reset_index(drop=True, inplace=True)

    # Save the cleaned data
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    df.to_csv(output_file_path, mode='w', index=False)
    print(f"Cleaned data saved to {output_file_path}")

if __name__ == "__main__":
    input_file = "/Users/ummadi/Desktop/dataflow-agents/data/interim/disposal/2022/CIIReport.csv"
    output_dir = "/Users/ummadi/Desktop/dataflow-agents/data/processed/disposal"
    output_file = f"{output_dir}/output.csv"
    clean_data(input_file, output_file)