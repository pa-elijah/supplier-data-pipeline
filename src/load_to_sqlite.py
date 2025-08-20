
import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = "/Users/mojdeh/Projects/Parts-Avatar/supplier-data-pipeline/src/parts_avatar.db"
SUPPLIER_CSV = "/Users/mojdeh/Projects/Parts-Avatar/supplier-data-pipeline/data/supplier_feed_cleaned.csv"
META_CSV     = "/Users/mojdeh/Projects/Parts-Avatar/supplier-data-pipeline/data/product_metadata.csv"  

def ensure_db():
    # DB is created by connecting; schema already applied via sqlite3 < schema.sql
    Path(DB_PATH).touch(exist_ok=True)

def load_product_metadata(conn: sqlite3.Connection, supplier_df: pd.DataFrame):
    # If metadata file exists, use it; otherwise derive minimal metadata from distinct part_ids
    try:
        meta = pd.read_csv(META_CSV, dtype={"part_id":"string","part_name":"string","category":"string"})
        print(f"Loaded metadata from {META_CSV} — {len(meta)} rows")
    except FileNotFoundError:
        meta = (supplier_df[["part_id"]]
                .drop_duplicates()
                .assign(part_name=pd.NA, category=pd.NA))
        print(f"No metadata file found — deriving {len(meta)} distinct part_ids")
    # Upsert product_metadata
    rows = meta.fillna("").to_dict(orient="records")
    conn.executemany(
        """
        INSERT OR IGNORE INTO product_metadata (part_id, part_name, category)
        VALUES (:part_id, :part_name, :category)
        """,
        rows
    )
    conn.commit()

def load_supplier_data(conn: sqlite3.Connection, supplier_df: pd.DataFrame):
    # Make sure dtypes align with schema
    required = {"part_id","stock_level","cost_price","entry_date"}
    missing = required - set(supplier_df.columns)
    if missing:
        raise ValueError(f"Missing required columns in supplier csv: {missing}")

    # Cast/clean final types (entry_date should already be 'YYYY-MM-DD' from your cleaning step)
    supplier_df = supplier_df.copy()
    supplier_df["part_id"] = supplier_df["part_id"].astype(str)
    supplier_df["stock_level"] = supplier_df["stock_level"].astype("int64")
    supplier_df["cost_price"] = pd.to_numeric(supplier_df["cost_price"], errors="coerce")
    supplier_df["entry_date"] = supplier_df["entry_date"].astype(str)

    # Use INSERT OR REPLACE to respect (part_id, entry_date) primary key
    rows = supplier_df.to_dict(orient="records")
    conn.executemany(
        """
        INSERT OR REPLACE INTO supplier_data (part_id, stock_level, cost_price, entry_date)
        VALUES (:part_id, :stock_level, :cost_price, :entry_date)
        """,
        rows
    )
    conn.commit()

def main():
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        supplier = pd.read_csv(
            SUPPLIER_CSV,
            dtype={"part_id":"string","stock_level":"Int64","cost_price":"float","entry_date":"string"}
        )
        print(f"Loaded cleaned supplier feed — {len(supplier)} rows")

        load_product_metadata(conn, supplier)
        load_supplier_data(conn, supplier)
        print("✅ Load complete")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
