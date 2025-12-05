import pandas as pd

file_path = "Sponsored_Products_Search_term_report.xlsx"
df = pd.read_excel(file_path)

print("\n=== Column Info Snapshot ===")
print(df.dtypes)

print("\n=== First 5 Rows (head) ===")
print(df.head())

print("\n=== Any duplicate column names? ===")
print(df.columns[df.columns.duplicated()].tolist())