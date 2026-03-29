import pandas as pd

output_file = '/Users/ummadi/Desktop/dataflow-agents/data/processed/disposal/output.csv'
df = pd.read_csv(output_file)

print("### Column Names and Data Types ###")
print("\n" + df.info().__str__() + "\n")

print("### Head (10) of Cleaned Output ###")
print("\n" + df.head(10).to_string() + "\n")

print("### Unique Values for Categorical Columns (if < 30 unique values) ###")
categorical_cols = ['SL', 'Crime Head']
for col in categorical_cols:
    if col in df.columns:
        unique_values = df[col].nunique()
        if unique_values < 30:
            print(f"\nUnique values for {col} ({unique_values} unique values):\n{df[col].unique().tolist()}")
        else:
            print(f"\n{col} has {unique_values} unique values (too many to list).")

print("\n### Null Count Per Column ###")
print("\n" + df.isnull().sum().to_string() + "\n")
