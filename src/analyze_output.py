import pandas as pd

def analyze_cleaned_data(file_path):
    df = pd.read_csv(file_path)

    print("\n--- Column Names ---")
    print(df.columns.tolist())

    print("\n--- Column Dtypes ---")
    print(df.dtypes.to_string())

    print("\n--- Head (5) of Cleaned Output (selected columns) ---")
    display_cols = ['SL', 'Crime Head', 'Persons Arrested_Total', 'Persons Convicted_Total', 'Year']
    print(df[display_cols].head(5).to_string(index=False))

    print("\n--- Unique Values for Categorical Columns (if < 30 unique values) ---")
    # Only print for 'SL' and 'Year' as 'Crime Head' has many unique values
    if df['SL'].nunique() < 30:
        print(f"\nUnique values for 'SL':")
        print(df['SL'].unique().tolist())
    if df['Year'].nunique() < 30:
        print(f"\nUnique values for 'Year':")
        print(df['Year'].unique().tolist())


    print("\n--- Null Count per Column (only columns with nulls) ---")
    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0]
    if not null_counts.empty:
        print(null_counts.to_string())
    else:
        print("No null values found.")


if __name__ == "__main__":
    processed_file = "/Users/ummadi/Desktop/dataflow-agents/data/processed/disposal/output.csv"
    analyze_cleaned_data(processed_file)