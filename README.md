# 📚 OpenAlex ETL Pipeline

This project provides a modular and scalable ETL (Extract, Transform, Load) pipeline for processing large-scale scholarly metadata from the OpenAlex dataset. The pipeline handles the complete data processing flow — from raw CSV ingestion to clean, validated, and relational-ready database inserts.

### 🏗️ Architecture Diagram
![Architecture](https://github.com/Gowtham-AI25/etl_pipeline/blob/main/docs/diagram-export-17-7-2025-10_29_03-pm.png)

## Project directory

```directory
my_etl_pipeline/
│
├── .gitignore                      # Specifies files for Git to ignore (e.g., secrets, data files)
├── README.md                       # High-level project overview, setup, and usage instructions
├── pyproject.toml                  # Modern Python project metadata and dependency management (or requirements.txt)
│
├── config/
│   ├── base.yml                    # Base configuration shared across all environments
│   ├── development.yml             # Settings specific to the development environment
│   └── private_config.yml              # Settings specific to the production environment
│
├── data/                           # (Often in .gitignore) For local test data, sample files
│   ├── raw/
│   └── processed/
│
├── docs/
│   ├── architecture.md             # Diagram and explanation of the pipeline architecture
│   └── data_dictionary.md          # Description of data sources, schemas, and fields
│
├── notebooks/
│   └── 01_exploratory_analysis.ipynb # Jupyter notebooks for analysis, not for production code
│
├── scripts/
│   ├── run_pipeline.sh             # A shell script to execute the entire pipeline
│   └── setup.sh                    # Script for initial environment setup
│
├── src/
│   ├── my_etl_pipeline/
│   │   ├── __init__.py
│   │   │
│   │   ├── extract/
│   │   │   ├── __init__.py
│   │   │   └── from_source_api.py      # Module to extract data from a specific API
│   │   │
│   │   ├── transform/
│   │   │   ├── __init__.py
|   |   |   └── Base_transformer.py     # Base class and all utils for transformation present inside it
│   │   │   └── clean_user_data.py      # Module for data cleaning and transformation logic
│   │   │
│   │   ├── load/
│   │   │   ├── __init__.py
│   │   │   └── to_data_warehouse.py    # Module to load data into the destination
│   │   │
│   │   ├── common/ or utils/
│   │   │   ├── __init__.py
│   │   │   └── helpers.py              # Shared functions (e.g., logging, config loading)
│   │   │
│   │   └── main.py                     # Main entry point to run the pipeline
```


## 🚀 Features

✅ Chunked Reading for Large Files: Reads large OpenAlex CSVs in memory-efficient chunks.

🔄 Data Cleaning & Normalization: Handles nested/multivalued attributes, nulls, unwanted fields, and precision errors.

🧠 Schema Validation: Ensures data integrity before loading into SQL Server using predefined schemas (e.g., for concepts, papers, institutions, sources).

🧾 Type Conversion: Converts strings, decimals, dates, and boolean-like fields into optimized Python and SQL types.

🗃️ Relational Database Loading: Uses PyODBC to insert into normalized SQL Server tables with error handling.

📋 Logging: Includes full logging for monitoring ETL status, tracking insert progress, and debugging.

📊 DBML Schema Documentation: Database schemas are documented using DBML and can be visualized on dbdocs.io.

🔄 Reusable Classes: Modular object-oriented design enables reuse for any OpenAlex entity (e.g., papers, authors, concepts, etc.).

## 🛠️ ETL Overview: Extract, Transform, Load with Memory & Storage Optimization

### 📥 Extract: Efficient Chunk-Based Data Loading

* **Problem**: OpenAlex datasets are massive (many GBs).
* **Solution**:

  * Uses **Pandas `read_csv` with `chunksize`** to read data in batches (e.g., 10,000 rows at a time), instead of loading entire files into memory.
  * Skips unnecessary columns at load time using `usecols`, reducing memory footprint.
* **Memory Optimization Techniques**:

  * Only keeps one chunk in memory at a time.
  * Releases memory (`del df`, `gc.collect()`) after each batch.
  * Logs progress of file reading to avoid reprocessing.

### 🔄 Transform: Clean, Normalize & Restructure

* **Problem**: Raw OpenAlex data contains nested lists, nulls, mixed types, and redundant fields.
* **Solution**:

  * Applies **custom transformers** to:

    * Normalize multivalued fields (e.g., `concepts`, `topics`) into separate relational rows.
    * Flatten nested JSONs into scalar values.
    * Convert strings to types like `datetime`, `bool`, `int`, and `Decimal` for optimized storage.
  * Renames fields consistently using a config dictionary.
* **Storage Optimization Techniques**:

  * Drops unused or auxiliary fields before DB load.
  * Deduplicates rows using `drop_duplicates()`.
  * Applies **lossless type downcasting** (e.g., `float64 → float32`, `int64 → int32`) to reduce RAM usage and disk space.

### 🗃️ Load: Streamlined Database Insertion

* **Problem**: Inserting large volumes into SQL Server can be slow or fail with poor type handling.
* **Solution**:

  * Uses **parameterized SQL inserts** via `pyodbc` for batch-safe loading.
  * Validates each record's data type before insert.
  * Supports single-row and batch insert modes.
* **Storage/Performance Optimizations**:

  * Converts empty strings and NaNs to `NULL` to save space.
  * Uses SQL Server’s native data types to align with Python conversions.
  * Indexes and constraints are enforced **after loading**, avoiding overhead during insert.

### 📘 Logical ER Diagram

#### Zoom in for clarity

![ER Diagram](https://github.com/Gowtham-AI25/etl_pipeline/blob/main/docs/Openalex_Finance_papers_database.png)

---
## ⚙️ Technologies Used

Python 3.11

Pandas

PyODBC

SQL Server

DBML (dbdocs.io)

Logging module

Git/GitHub

## 📂 Entities Processed
concepts

institutions

sources

funders

papers

topics

## 📦 Memory & Storage Concepts Used

| Concept                      | Purpose                                              |
| ---------------------------- | ---------------------------------------------------- |
| **Chunk Processing**         | Handles files that don’t fit in memory               |
| **Selective Column Reading** | Skips irrelevant data to save memory                 |
| **Lazy Evaluation**          | Transforms are done per chunk, not on full dataset   |
| **Data Type Downcasting**    | Reduces memory/storage for numeric and date fields   |
| **Garbage Collection**       | Frees memory after processing each chunk             |
| **Normalization**            | Avoids data redundancy, improves query speed         |
| **Split Multivalues**        | Stores long lists (like concepts) in separate tables |
| **NULL Handling**            | Replaces missing/blank fields with `NULL`            |
| **Incremental Logging**      | Logs ETL progress without using print (disk-based)   |

---

