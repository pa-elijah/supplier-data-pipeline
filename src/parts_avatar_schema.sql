
-- Schema for Parts Avatar Inventory Database


-- Drop table if it exists (for re-creation)
DROP TABLE IF EXISTS product_metadata;

DROP TABLE IF EXISTS supplier_data;

CREATE TABLE IF NOT EXISTS product_metadata (
    part_id   TEXT PRIMARY KEY,
    part_name         TEXT,                 
    category           TEXT
);


CREATE TABLE IF NOT EXISTS supplier_data (
    part_id   TEXT NOT NULL,
    stock_level        INTEGER NOT NULL,
    cost_price         REAL,                  
    entry_date         TEXT NOT NULL,         -- store as 'YYYY-MM-DD'
    created_at         TEXT DEFAULT (DATE('now')), -- ETL load date
    PRIMARY KEY (part_id, entry_date),    -- one row per (part, snapshot_date)
    FOREIGN KEY (part_id) REFERENCES product_metadata(part_id)
);

part_id,stock_level,cost_price,entry_date

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_supplier_entry ON supplier_data(entry_date);
CREATE INDEX IF NOT EXISTS idx_supplier_category ON product_metadata(category);
