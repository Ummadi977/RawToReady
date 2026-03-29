import pandas as pd
import os

def clean_disposal_data(input_filepath, output_filepath, year):
    # Define new column names based on debugging output
    column_names = [
        'SL',
        'Crime Head',
        'Persons Arrested - Male', 'Persons Arrested - Female', 'Persons Arrested - Transgender', 'Persons Arrested - Total',
        'Junk_6', # This is an empty separator column
        'Persons Chargesheeted - Male', 'Persons Chargesheeted - Female', 'Persons Chargesheeted - Transgender', 'Persons Chargesheeted - Total',
        'Persons Convicted - Male', 'Persons Convicted - Female', 'Persons Convicted - Transgender', 'Persons Convicted - Total',
        'Junk_15', # This is an empty separator column
        'Persons Discharged - Male', 'Persons Discharged - Female', 'Persons Discharged - Transgender', 'Persons Discharged - Total',
        'Persons Acquitted - Male', 'Persons Acquitted - Female', 'Persons Acquitted - Transgender', 'Persons Acquitted - Total',
        'Junk_24', # This is an empty column at the end
        'Junk_25' # Another empty column at the very end
    ]

    # Read the CSV, skipping the header rows and assigning the new column names
    df = pd.read_csv(input_filepath, skiprows=6, header=None, names=column_names, encoding='utf-8')

    # Drop the placeholder columns
    df.drop(columns=['Junk_6', 'Junk_15', 'Junk_24', 'Junk_25'], inplace=True)

    # Add the 'Year' column
    df['Year'] = year

    # Drop rows where 'Crime Head' is NaN or contains "Total" (these are usually summary rows)
    df.dropna(subset=['Crime Head'], inplace=True)
    df = df[~df['Crime Head'].str.contains('Total', na=False)]
    df = df[~df['Crime Head'].str.contains('Grand Total', na=False)]
    df = df[~df['SL'].str.contains('Total', na=False)] # Also check SL column for total rows

    # Clean numeric columns
    # Exclude 'SL', 'Crime Head', 'Year' from numeric cleaning
    numeric_cols = [col for col in df.columns if col not in ['SL', 'Crime Head', 'Year']]
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Clean text columns
    text_cols = ['SL', 'Crime Head']
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()
        # Apply title case to 'Crime Head'
        if col == 'Crime Head':
            df[col] = df[col].str.title()

    # Drop any all-null rows that might have been created
    df.dropna(how='all', inplace=True) # Keep this to drop all-NaN rows
    # df.dropna(axis=1, how='all', inplace=True) # Temporarily remove this to debug column dropping

    # Reorder columns to have Year at the beginning
    cols = ['Year'] + [col for col in df.columns if col != 'Year']
    df = df[cols]

    # Save the cleaned data
    df.to_csv(output_filepath, mode='w', index=False)
    print(f"Cleaned data saved to {output_filepath}")

if __name__ == '__main__':
    input_file = '/Users/ummadi/Desktop/dataflow-agents/data/interim/disposal/2022/CIIReport.csv'
    output_dir = '/Users/ummadi/Desktop/dataflow-agents/data/processed/disposal'
    output_file = os.path.join(output_dir, 'output.csv')
    year = 2022

    os.makedirs(output_dir, exist_ok=True)
    clean_disposal_data(input_file, output_file, year)
