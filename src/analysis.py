import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = "/Users/mojdeh/Projects/Parts-Avatar/supplier-data-pipeline/src/parts_avatar.db"
OUTDIR = Path("/Users/mojdeh/Projects/Parts-Avatar/supplier-data-pipeline/analysis/analysis_outputs")
OUTDIR.mkdir(parents=True, exist_ok=True)

def run_query(conn, sql) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)

def save_table(df: pd.DataFrame, name: str):
    path = OUTDIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"Saved table: {path}")

def plot_bar(df: pd.DataFrame, xcol: str, ycol: str, title: str, outfile: str):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(df[xcol].astype(str), df[ycol])
    ax.set_title(title)
    ax.set_xlabel(xcol)
    ax.set_ylabel(ycol)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    out = OUTDIR / outfile
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"Saved chart: {out}")

def plot_line(df: pd.DataFrame, xcol: str, ycol: str, title: str, outfile: str):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df[xcol].astype(str), df[ycol])
    ax.set_title(title)
    ax.set_xlabel(xcol)
    ax.set_ylabel(ycol)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    out = OUTDIR / outfile
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"Saved chart: {out}")

def main():
    conn = sqlite3.connect(DB_PATH)

    # 1) Average cost price per category
    q1 = """
    SELECT
      pm.category,
      AVG(sd.cost_price) AS avg_cost_price,
      COUNT(sd.cost_price) AS n_rows
    FROM supplier_data sd
    JOIN product_metadata pm
      ON pm.part_id = sd.part_id
    GROUP BY pm.category
    ORDER BY avg_cost_price DESC;
    """
    df1 = run_query(conn, q1)
    if not df1.empty:
        print("AVG cost price per Cat: ",df1.head())
        save_table(df1, "avg_cost_by_category")
        plot_bar(df1, "category", "avg_cost_price",
                 "Average Cost Price by Category",
                 "avg_cost_by_category.png")
    else:
        print("No data for average cost by category (check category & cost_price).")

    # 2) Top 5 parts with highest *current* stock
    #    "current" = latest entry_date per part
    q2 = """
    WITH latest AS (
      SELECT part_id, MAX(entry_date) AS latest_date
      FROM supplier_data
      GROUP BY part_id
    )
    SELECT
      sd.part_id,
      pm.part_name AS product_name,
      sd.stock_level
    FROM supplier_data sd
    JOIN latest l
      ON sd.part_id = l.part_id
     AND sd.entry_date = l.latest_date
    LEFT JOIN product_metadata pm
      ON pm.part_id = sd.part_id
    ORDER BY sd.stock_level DESC
    LIMIT 5;
    """
    df2 = run_query(conn, q2)
    if not df2.empty:
        save_table(df2, "top5_stock")
        plot_bar(df2, "product_name", "stock_level",
                 "Top 5 Parts by Current Stock",
                 "top5_stock.png")
    else:
        print("No data for top-5 current stock (check supplier_data).")

    # 3A) Monthly trend: number of entries per month (row count)
    q3a = """
    SELECT
      SUBSTR(entry_date, 1, 7) AS month,   -- YYYY-MM
      COUNT(*) AS entries
    FROM supplier_data
    GROUP BY SUBSTR(entry_date, 1, 7)
    ORDER BY month;
    """
    df3a = run_query(conn, q3a)
    if not df3a.empty:
        save_table(df3a, "monthly_entries")
        plot_line(df3a, "month", "entries",
                  "Monthly Supplier Entries (All Rows)",
                  "monthly_entries.png")
    else:
        print("No data for monthly entries (check entry_date format).")

    # 3B) Monthly trend: *new parts* first-seen per month (distinct first appearance)
    q3b = """
    WITH first_seen AS (
      SELECT part_id, MIN(entry_date) AS first_entry
      FROM supplier_data
      GROUP BY part_id
    )
    SELECT
      SUBSTR(first_entry, 1, 7) AS month,
      COUNT(*) AS new_parts
    FROM first_seen
    GROUP BY SUBSTR(first_entry, 1, 7)
    ORDER BY month;
    """
    df3b = run_query(conn, q3b)
    if not df3b.empty:
        save_table(df3b, "monthly_new_parts")
        plot_line(df3b, "month", "new_parts",
                  "Monthly New Parts (First Seen)",
                  "monthly_new_parts.png")
    else:
        print("No data for monthly new parts (check entry_date & part_id).")

    
    # 4) Most Frequent Low Stock Products
    q4=  """
    SELECT part_id,
    COUNT(*) AS low_stock_count
    FROM supplier_data
    WHERE stock_level < 6     
    GROUP BY part_id
    ORDER BY low_stock_count DESC
    LIMIT 10;
 """
    df4 = run_query(conn, q4)
    if not df4.empty:
        save_table(df4, "high_frequent_low_stock_product")
        plot_bar(df4, "part_id", "low_stock_count",
                  "high_frequent_low_stock_product",
                  "high_frequent_low_stock_product.png")
    else:        print("No data for High Frequent Low Stock Product")


    # 5) Monthly Trend of numbers of low stock items
    q5=  """

    SELECT SUBSTR(entry_date, 1, 7) AS month,     -- YYYY-MM
    COUNT(*) AS low_stock_count
    FROM supplier_data
    WHERE stock_level < 6                        
    GROUP BY month
    ORDER BY month;

 """
    df5 = run_query(conn, q5)
    if not df4.empty:
        save_table(df5, "monthly_trend_of_low_stock_count")
        plot_line(df5, "month", "low_stock_count",
                  "monthly_trend_of_low_stock_count",
                  "monthly_trend_of_low_stock_count.png")
    else:        print("No data for Monthly Trend of Low Stock Count")
    
    conn.close()
    print("\n✅ Analysis complete. See CSVs & PNGs in:", OUTDIR.resolve())
if __name__ == "__main__":
    main()
