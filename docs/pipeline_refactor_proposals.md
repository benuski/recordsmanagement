# Pipeline Refactor Proposals - Status Report

This document outlines structural changes made to the `rm` processing pipeline.

## 1. Modularize State Pipelines
**Status:** COMPLETED
- Refactored `harvest.py` to use dynamic imports.
- Created standard `processor.py` for all states (`va`, `tx`, `oh`, `nc`).
- Defined standard `run(args, schema)` entry point.

## 2. Refactor `extractor_engine.py`
**Status:** COMPLETED
- Monolith split into `processing/utils/` package.
- `pdf_utils.py`: Structural analysis.
- `text_utils.py`: Cleaning and extraction.
- `schema_utils.py`: Record lifecycle and scoring.
- `strategy_utils.py`: Specialized parsing engines.
- `extractor_engine.py` is now a high-level orchestrator.

## 3. Standardize State Directory Layout
**Status:** COMPLETED
- Standardized layout for all states:
  - `config.py`: Configuration definitions.
  - `parser.py`: Low-level parsing logic.
  - `processor.py`: High-level orchestration.
  - `resources/`: Auxiliary data (mapping CSVs, etc.).
  - `src/`: Raw source data.

## 4. Decouple Harvesting from Parsing
**Status:** COMPLETED
- Added `--task` flag to `harvest.py` (`harvest`, `parse`, `all`).
- Updated Ohio processor to support independent tasks.

## 5. Centralize Output Logic
**Status:** PENDING
- **Goal:** Move grouping and writing logic into a shared utility function to ensure consistent JSON formatting and directory structures across all states.
