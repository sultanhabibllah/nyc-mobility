import pandas as pd

# Path to your raw dataset
csv_path = "data/raw/train_unzipped/train.csv"

# Read just the first 5 rows
df = pd.read_csv(csv_path, nrows=5)

print("✅ Preview of my dataset:")
print(df.head())
print("\n🧾 Columns available:", list(df.columns))
print("\n🔢 Total rows in sample:", len(df))
