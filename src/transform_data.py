import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import re
from datetime import date



# ---------- Load & quick EDA ----------
df = pd.read_csv("supplier-data-pipeline/data/supplier_feed.csv")
print(df.head())         # preview first rows
print(df.info())         # column data types
print(df.isna().sum())   # count missing values
print(df['cost_price'].describe())
print(df['stock_level'].describe())

# Handling Missing Values 
# Based on exploration:
# - ~13% stock_level missing
# - ~5% cost_price missing
# Strategy below:
#   * Map out-of-stock markers to 0
#   * For "Low Stock", compute mode within the lowest quartile of unique positive values
#   * Remaining NaNs -> median
#   * drop rows with missing cost_price 

# ---------- Unique non-integer markers (for debugging/awareness) ----------
df_temp = df.copy()
col = "stock_level"
numeric_try = pd.to_numeric(df_temp[col], errors="coerce")
mask_non_integer = numeric_try.isna() | (numeric_try % 1 != 0)
unique_non_integers = df_temp.loc[mask_non_integer, col].dropna().unique()
print("Unique non-integer values in", col, ":", unique_non_integers)

# ---------- Clean & visualize stock_level distribution (pre-imputation) ----------
# Make a numeric view with common markers turned to NaN for fair plotting
stock_numeric_for_plot = pd.to_numeric(
    df["stock_level"]
      .astype(str).str.strip(),  # normalize whitespace
    errors="coerce"
)

# Save histogram with integer bins if possible
if stock_numeric_for_plot.dropna().size > 0:
    s = stock_numeric_for_plot.dropna()
    vmin, vmax = int(np.floor(s.min())), int(np.ceil(s.max()))
    fig, ax = plt.subplots(figsize=(12, 6))
    if vmin < vmax:
        bins = range(vmin, vmax + 2)  # integer bin edges
        ax.hist(s, bins=bins, edgecolor="black")
        ax.xaxis.set_major_locator(MaxNLocator(nbins=15, integer=True))
    else:
        # fallback single-bin
        ax.hist(s, bins=10, edgecolor="black")
    ax.set_title("Distribution of Stock Level (raw numeric-coercible)")
    ax.set_xlabel("Stock Level (integer)")
    ax.set_ylabel("Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("stock_level_distribution.png", dpi=300)
    plt.close()
    print("Saved plot: stock_level_distribution.png")

# ---------- Normalize stock_level markers ----------
# Lowercase helper column to catch variants like "LOW", "low stock", etc.
stock_str = df[col].astype(str).str.strip()
stock_lower = stock_str.str.lower()

# Map out-of-stock markers to 0
out_of_stock_mask = stock_lower.isin({"out of stock", "unavailable", "oos"})
df.loc[out_of_stock_mask, col] = 0

# We'll treat low-stock markers separately
low_stock_mask = stock_lower.isin({"low stock", "low", "ls"})

# Convert remaining to numeric (Low Stock and other invalids -> NaN)
numeric = pd.to_numeric(df[col], errors="coerce")

# ---------- Compute median for later imputation ----------
median_val = numeric.median(skipna=True)
print("Median (for missing imputation):", median_val)

# ---------- Compute 'Low Stock' replacement = smallest strictly positive unique value ----------
positive = numeric[numeric > 0].dropna()

if positive.size > 0:
    smallest_unique_val = positive.min()   # first smallest positive
    low_stock_replacement = float(smallest_unique_val)
else:
    # Fallback: if no positive values exist, use overall median
    low_stock_replacement = float(median_val) if pd.notna(median_val) else 0.0
    smallest_unique_val = None

print("Chosen 'Low Stock' replacement (smallest strictly positive value):", low_stock_replacement)
print("Smallest positive used:", smallest_unique_val if smallest_unique_val is not None else "N/A")

# ---------- Apply imputations ----------
# Replace low-stock markers with computed replacement
df.loc[low_stock_mask, col] = low_stock_replacement

# Re-coerce to numeric after replacements
df[col] = pd.to_numeric(df[col], errors="coerce")

# Any remaining NaN -> median
df[col] = df[col].fillna(median_val)

# Cast to int (inventory is integral)
df[col] = df[col].round().astype(int)

print("Imputation complete. Preview:")
print(df[col].head())


numeric = pd.to_numeric(df[col], errors="coerce")
mask_non_integer = numeric.isna() | (numeric % 1 != 0)



# ---------- Cost_price handling ----------
# Drop rows where cost_price is missing
df = df.dropna(subset=['cost_price'])



col = "cost_price"

# Try to find non float price values:

# # Work on the raw string representation
# raw = df[col].astype(str).str.strip()

# # Try to parse each value as a float safely
# def is_float_like(x: str) -> bool:
#     try:
#         float(x)
#         return True
#     except ValueError:
#         return False

# mask_non_float = ~raw.apply(is_float_like)

# # Show rows with non-float values
# print("Non-float values found:")
# print(df.loc[mask_non_float, col])

# # Unique non-float values
# print("\nUnique non-float values:")
# print(df.loc[mask_non_float, col].unique())

# # Value counts of non-float values
# print("\nNon-float value counts:")
# print(df.loc[mask_non_float, col].value_counts())



# Finding: I understand that I only need to remove $ before the number


# Remove the "$" sign and extra spaces
df[col] = df[col].astype(str).str.strip().str.replace("$", "", regex=False)

# Convert to float
df[col] = df[col].astype(float)


# ---------- entry_date handling ----------


ENTRY_COL = "entry_date"
SAVE_OFFENDERS = True
MIN_DATE = pd.Timestamp("2000-01-01")
MAX_DATE = pd.Timestamp(date.today().isoformat()) 

# Keep raw for audit
# df[f"{ENTRY_COL}_raw"] = df[ENTRY_COL]

def parse_entry_date(x):
    """Parse ISO, 'Apr 12, 2025', ISO with T, and MM/DD/YY (US-style).
       Returns pandas.Timestamp or NaT."""
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip().strip('"')  # trim quotes/spaces

    # 1) Try pandas flexible parser (ISO, 'Apr 12, 2025', ISO with T)
    dt = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)

    # 2) If still NaT and looks like slashes with 2-digit year -> enforce US mm/dd/yy
    if pd.isna(dt) and "/" in s:
        dt = pd.to_datetime(s, format="%m/%d/%y", errors="coerce")

    return dt

# ---- Parse ----
parsed = df[ENTRY_COL].apply(parse_entry_date)

# ---- Validate range & normalize ----
valid = parsed.copy()
# Out-of-range -> NaT
oob_mask = valid.notna() & ((valid < MIN_DATE) | (valid > MAX_DATE))
valid[oob_mask] = pd.NaT
# Strip time
valid = valid.dt.normalize()

# ---- Audit ----
n = len(df)
explicit_missing_tokens = df[ENTRY_COL].astype(str).str.strip().str.lower().isin(
    {"", "n/a", "na", "none", "missing", "null", "undefined"}
).sum()
unparseable = parsed.isna().sum()
out_of_range = oob_mask.sum()

print("\n=== ENTRY_DATE Audit ===")
print(f"Rows: {n}")
print(f"Explicit missing tokens: {explicit_missing_tokens}")
print(f"Unparseable to datetime:  {unparseable}")
print(f"Out-of-range (set NaT):   {out_of_range}")

# # Optionally save offenders for inspection
# if SAVE_OFFENDERS:
#     bad_mask = parsed.isna() | oob_mask
#     if bad_mask.any():
#         df.loc[bad_mask, [f"{ENTRY_COL}_raw"]].to_csv("entry_date_offenders.csv", index=True)
#         print("Saved offenders -> entry_date_offenders.csv")

# ---- Apply standardized column ----
df[ENTRY_COL] = valid


print("Standardized entry_date nulls:", df[ENTRY_COL].isna().sum())
# print(df[[f"{ENTRY_COL}_raw", ENTRY_COL]].head(10))

print(df.head())


# ---------- Save cleaned file ----------
df.to_csv("supplier-data-pipeline/data/supplier_feed_cleaned.csv", index=False)
print("✅ Saved cleaned data -> supplier-data-pipeline/data/supplier_feed_cleaned.csv")
