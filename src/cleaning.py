
import pandas as pd
import io

# Define input and output paths
input_file = "/Users/ummadi/Desktop/dataflow-agents/data/interim/disposal/2022/CIIReport.csv"
output_file = "/Users/ummadi/Desktop/dataflow-agents/data/processed/disposal/output.csv"

# Load the CSV, skipping initial junk rows, and setting the header explicitly
df = pd.read_csv(input_file, skiprows=2, header=0)

# Drop the 'SL' column as it's a serial number
df = df.drop(columns=['SL'], errors='ignore')

# Drop rows that are not actual data (e.g., 'STATES:', 'TOTAL') using the original column name
df = df[~df['State/UT'].isin(['STATES:', 'TOTAL'])]

# Rename columns for clarity and consistency
df = df.rename(columns={
    'State/UT': 'State',
    'Actual Population of SCs (in Lakhs) (2011)': 'SC_Population_2011_Lakhs',
    'Rate of Total Crime against SCs (2022)': 'Crime_Rate_2022_Percentage',
    'Chargesheeting Rate (2022)': 'Chargesheeting_Rate_2022_Percentage'
})

# Clean 'State' column
df['State'] = df['State'].str.strip().str.title()

# Drop rows where 'State' is NaN after initial cleaning
df.dropna(subset=['State'], inplace=True)

# Convert numeric columns, handling '-' as NaN
numeric_cols = ['2020', '2021', '2022', 'SC_Population_2011_Lakhs', 'Crime_Rate_2022_Percentage', 'Chargesheeting_Rate_2022_Percentage']
for col in numeric_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace('-', 'NaN', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Add a 'units' column based on the column names
units_mapping = {
    '2020': 'Absolute Numbers',
    '2021': 'Absolute Numbers',
    '2022': 'Absolute Numbers',
    'SC_Population_2011_Lakhs': 'Lakhs',
    'Crime_Rate_2022_Percentage': 'Percentage',
    'Chargesheeting_Rate_2022_Percentage': 'Percentage'
}

# Create a 'Metric' and 'Unit' column by melting the data
df_melted = df.melt(id_vars=['State'],
                    value_vars=['2020', '2021', '2022', 'SC_Population_2011_Lakhs', 'Crime_Rate_2022_Percentage', 'Chargesheeting_Rate_2022_Percentage'],
                    var_name='Metric',
                    value_name='Value')

df_melted['Unit'] = df_melted['Metric'].map(units_mapping)

# Extract year from 'Metric' where applicable
def extract_year(metric):
    if metric in ['2020', '2021', '2022']:
        return int(metric)
    elif '2011' in metric:
        return 2011
    elif '2022' in metric and 'Rate' in metric:
        return 2022
    return None

df_melted['Year'] = df_melted['Metric'].apply(extract_year)

# Reorder columns
df_cleaned = df_melted[['State', 'Year', 'Metric', 'Value', 'Unit']]

# Drop rows where all values are null (after cleaning)
df_cleaned.dropna(how='all', inplace=True)

# Drop columns where all values are null
df_cleaned.dropna(axis=1, how='all', inplace=True)

# Sort by year descending if present
if 'Year' in df_cleaned.columns:
    df_cleaned = df_cleaned.sort_values(by='Year', ascending=False)

# Save the cleaned DataFrame to CSV
df_cleaned.to_csv(output_file, mode='w', index=False)

print(f"Cleaned data saved to {output_file}")

# --- Reporting section ---
# Read the cleaned file back for reporting
try:
    df_report = pd.read_csv(output_file)

    # Capture print output
    output_capture = io.StringIO()
    
    print("\n--- Cleaned Data Report ---", file=output_capture)
    print("\nColumn names and dtypes:", file=output_capture)
    df_report.info(buf=output_capture)
    
    print("\nHead (10) of cleaned output:", file=output_capture)
    print(df_report.head(10).to_string(), file=output_capture)
    
    print("\nUnique values for categorical columns:", file=output_capture)
    for col in ['State', 'Metric', 'Unit']:
        if col in df_report.columns and df_report[col].nunique() < 30:
            print(f"  {col}: {df_report[col].unique().tolist()}", file=output_capture)
        elif col in df_report.columns:
            print(f"  {col}: (More than 30 unique values, showing count: {df_report[col].nunique()})", file=output_capture)
            
    print("\nNull count per column:", file=output_capture)
    print(df_report.isnull().sum().to_string(), file=output_capture)

    print(output_capture.getvalue())

except FileNotFoundError:
    print(f"Error: Cleaned file not found at {output_file}")
