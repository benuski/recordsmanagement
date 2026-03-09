# Python Script Index

This document provides a one-sentence explanation for every Python script in the `rm` processing pipeline.

## Core Pipeline
- **harvest.py**: The main entry point that orchestrates the entire pipeline, routing tasks to state-specific processors.
- **processing/extractor_engine.py**: High-level orchestrator that manages the evaluation of different PDF parsing strategies.
- **processing/base_config.py**: Defines the `StateScheduleConfig` dataclass used to standardize extraction rules across all states.
- **processing/__init__.py**: Initializes the processing directory as a Python package.

## Shared Utilities (`processing/utils/`)
- **processing/utils/output_utils.py**: Centralized logic for grouping and saving extracted records to JSON files.
- **processing/utils/pdf_utils.py**: Provides utilities for PDF pre-flight checks and sorting word objects into readable text.
- **processing/utils/schema_utils.py**: Handles the creation of standardized record dictionaries and quality scoring of extracted data.
- **processing/utils/strategy_utils.py**: Implements specialized parsing engines for tables, vertical silos, and marker-based HTML.
- **processing/utils/text_utils.py**: Contains functions for cleaning raw text, splitting titles from descriptions, and calculating retention years.
- **processing/utils/__init__.py**: Exports the key functions from the utility package for cleaner imports.

## North Carolina (`processing/nc/`)
- **processing/nc/config.py**: Defines the configuration, regex patterns, and URLs for North Carolina's functional schedules.
- **processing/nc/parser.py**: Implements logic for parsing North Carolina's structure-aware JSON data produced by pdfplumber.
- **processing/nc/processor.py**: The high-level pipeline runner for North Carolina that manages file discovery and record saving.
- **processing/nc/__init__.py**: Initializes the North Carolina module as a package.

## Ohio (`processing/oh/`)
- **processing/oh/config.py**: Defines the configuration and regex patterns for Ohio's RIMS web portal records.
- **processing/oh/harvester.py**: Scrapes and downloads HTML detail pages for individual records from Ohio's RIMS portal.
- **processing/oh/parser.py**: Implements logic for parsing Ohio's specific and general schedule HTML files.
- **processing/oh/processor.py**: The high-level pipeline runner for Ohio that manages the distinct harvest and parse tasks.
- **processing/oh/__init__.py**: Initializes the Ohio module as a package.

## Texas (`processing/tx/`)
- **processing/tx/config.py**: Defines the configuration and regex patterns for Texas state agency schedules.
- **processing/tx/parse_agencies.py**: Scrapes and extracts agency names and codes from Texas's online agency index.
- **processing/tx/tx_pdf_processor.py**: Implements a specialized PDF table extractor tailored for Texas's complex multi-column layouts.
- **processing/tx/processor.py**: The high-level pipeline runner for Texas that manages metadata extraction and record grouping.
- **processing/tx/parser.py**: (Legacy) Original parsing logic replaced by the new modular state processor.
- **processing/tx/tx_processing.py**: (Legacy) Original processing logic replaced by the new modular state processor.
- **processing/tx/__init__.py**: Initializes the Texas module as a package.

## Virginia (`processing/va/`)
- **processing/va/config.py**: Defines the configuration and regex patterns for Virginia's library-provided retention PDFs.
- **processing/va/processor.py**: The high-level pipeline runner for Virginia that utilizes the multi-strategy extraction engine.
- **processing/va/__init__.py**: Initializes the Virginia module as a package.

## Archived Scripts (`archive/`)
- **archive/gs-101.py**: Legacy script for processing General Schedule 101.
- **archive/i2t.py**: Early experimentation with image-to-text conversion tools.
- **archive/oh-dl-html.py**: Legacy prototype for downloading Ohio HTML records.
- **archive/oh-general.py**: Legacy prototype for parsing Ohio general schedules.
- **archive/oh-specific-links.py**: Legacy prototype for harvesting Ohio record links.
- **archive/oh-specific-process.py**: Legacy prototype for processing individual Ohio record pages.
- **archive/va-pdfs.py**: Legacy prototype for downloading Virginia retention schedule PDFs.
