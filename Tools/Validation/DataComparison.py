import pandas as pd
import hashlib
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
 
# ======================
# CONFIGURATION
# ======================
file1 = "file1.xlsx"       # First file
file2 = "file2.xlsx"       # Second file
output_file = "comparison_output.xlsx"  # Output file
highlight_color = "FFFF0000"  # Red
 
# ======================
# FUNCTIONS
# ======================
def clean_dataframe(df):
    """Lowercase & strip all string cells, vectorized."""
    return df.apply(lambda col: col.astype(str).str.strip().str.lower())
 
def compute_row_hash(df):
    """Compute MD5 hash for each row."""
    return pd.util.hash_pandas_object(df, index=False).astype(str)
 
def highlight_cells(ws, df_orig, df_lower, df_cmp_lower):
    """Highlight cells that differ."""
    highlight = PatternFill(start_color=highlight_color, end_color=highlight_color, fill_type="solid")
 
    for i, row in enumerate(df_orig.itertuples(index=False), start=2):
        if row.Status == 'Not Matched':
            # Get matching rows in other file
            cmp_idx = df_cmp_lower[df_cmp_lower['row_hash'] == df_lower.loc[i-2, 'row_hash']].index
           
            if len(cmp_idx) == 0:
                # No match: highlight whole row except Status
                for j in range(1, len(row)):
                    ws.cell(row=i, column=j).fill = highlight
            else:
                # Partial match: highlight only mismatched cells
                cmp_row = df_cmp_lower.loc[cmp_idx[0], :-1].tolist()
                for j, val in enumerate(df_lower.loc[i-2, :-1], start=1):
                    if val != cmp_row[j-1]:
                        ws.cell(row=i, column=j).fill = highlight
 
# ======================
# READ & CLEAN FILES
# ======================
df1 = pd.read_excel(file1)
df2 = pd.read_excel(file2)
 
df1_lower = clean_dataframe(df1)
df2_lower = clean_dataframe(df2)
 
# ======================
# HASHING
# ======================
df1_lower['row_hash'] = compute_row_hash(df1_lower)
df2_lower['row_hash'] = compute_row_hash(df2_lower)
 
# ======================
# FAST MATCH CHECK
# ======================
df2_hashes = set(df2_lower['row_hash'])
df1['Status'] = df1_lower['row_hash'].apply(lambda x: 'Matched' if x in df2_hashes else 'Not Matched')
 
df1_hashes = set(df1_lower['row_hash'])
df2['Status'] = df2_lower['row_hash'].apply(lambda x: 'Matched' if x in df1_hashes else 'Not Matched')
 
# ======================
# SAVE INITIAL OUTPUT
# ======================
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df1.to_excel(writer, sheet_name='File1', index=False)
    df2.to_excel(writer, sheet_name='File2', index=False)
 
# ======================
# HIGHLIGHT DIFFERENCES
# ======================
wb = load_workbook(output_file)
 
highlight_cells(wb['File1'], df1, df1_lower, df2_lower)
highlight_cells(wb['File2'], df2, df2_lower, df1_lower)
 
wb.save(output_file)
 
print(f"Comparison completed successfully. Output saved to '{output_file}'")