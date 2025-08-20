# ------- Configuration -------
PY=python
DB=supplier-data-pipeline/src/parts_avatar.db
SCHEMA=supplier-data-pipeline/src/parts_avatar_schema.sql
TRANSFORM=supplier-data-pipeline/src/transform_data.py
LOAD=supplier-data-pipeline/src/load_to_sqlite.py
ANALYSIS=supplier-data-pipeline/src/analysis.py    
OUTDIR=supplier-data-pipeline/analysis/analysis_outputs

.PHONY: all transform schema load analyze reset clean

# Run everything end-to-end
all: transform schema load analyze

# Step 1: Clean & standardize CSVs
transform:
	$(PY) $(TRANSFORM)

# Step 2: Initialize/refresh DB schema
schema:
	sqlite3 $(DB) < $(SCHEMA)

# Step 3: Load cleaned CSVs into SQLite
load:
	$(PY) $(LOAD)

# Step 4: Run analysis + export charts/CSVs
analyze:
	$(PY) $(ANALYSIS)

# Drop & recreate DB schema (handy during iteration)
reset: clean schema

# Remove generated artifacts (DB + analysis outputs)
clean:
	@if [ -f "$(DB)" ]; then rm -f "$(DB)"; fi
	@if [ -d "$(OUTDIR)" ]; then rm -rf "$(OUTDIR)"; fi
