Here is an in-depth description you can use in your `architecture.md` file, explaining each component of the **OpenAlex ETL pipeline architecture diagram** you provided:

---

# 🏗️ OpenAlex ETL Pipeline – Architecture Overview

This architecture represents a modular and scalable ETL (Extract, Transform, Load) pipeline designed to process OpenAlex datasets efficiently and load them into a SQL Server database. The pipeline handles massive academic metadata files, cleans and normalizes them, and ensures database-friendly ingestion using Python and open libraries.

---

## 🔶 1. Extract Phase

The extraction layer handles **retrieval of raw data from remote sources** in two primary formats — via file storage and HTTP APIs:

### 📦 Data Sources:

* **OpenAlex S3 Bucket**: Hosts large `.gz` or `.json` data dumps for concepts, authors, works, institutions, etc.
* **Custom S3 Bucket**: Optional – for staging or partial backup storage.
* **OpenAlex API**: Provides on-demand, smaller slices of data.

### 🛠 Libraries Used:

* `boto3`: Used to interface with S3 buckets, download `.gz` or `.json` files.
* `httpx`: Used for making async API calls to fetch data from OpenAlex endpoints.

### 🔄 Output:

* Extracted data is stored locally as **JSON files**, then **converted to CSV** for transformation.

---

## 🔷 2. Transform Phase

This is the **core data wrangling stage**, where extracted data is cleaned, normalized, and structured into relational formats suitable for database loading.

### 🔧 Key Components:

* **Python Scripts**:

  * `base_extractor.py`: Base class that handles shared logic for all dataset-specific parsers.
  * `helpers.py`: Utility functions (e.g., type conversion, null cleaning, etc.)

* **Configuration Files**:

  * `config.yml`: Defines dataset paths, field mappings, and behavior flags.
  * `utils.json`: Stores reusable transformation mappings (e.g., concept weights, ID-name mappings).

### ⚙️ Transformation Logic:

* Reads raw CSV files (e.g., `sources.csv`, `papers.csv`).
* Applies:

  * **Data Cleaning**: Remove empty rows, invalid values, and malformed fields.
  * **Type Conversion**: Safely cast fields (e.g., string → int, float → decimal).
  * **Normalization**: Flatten nested JSON arrays into sub-entity CSVs (e.g., work-concepts, work-authorships).
  * **Batch Processing**: Process in small chunks to avoid memory overflow.
* Logs all processing steps to `logging.txt`.

### 📂 Output:

* Cleaned and transformed files are written to the `processed_files/` folder.

---

## 🟩 3. Load Phase

This stage is responsible for ingesting the **transformed CSV files into SQL Server** with schema constraints and referential integrity.

### 📜 SQL Scripts:

1. `create_tables.sql`: Defines all required tables with data types, primary keys, and indexes.
2. `make_connections.sql`: Creates foreign key relationships after bulk insert is complete.

### 🐍 Loader Script:

* `Load_to_database.py`:

  * Uses `pyodbc` to connect to SQL Server.
  * Performs row-by-row or batch inserts depending on performance settings.
  * Converts types properly (e.g., handles nulls, floats, decimals).
  * Manages error logging and restarts from failure points.

### ✅ Final Output:

* Fully populated **SQL Server database** with clean, validated data:

  * Each table maps to a distinct entity: `works`, `authors`, `institutions`, etc.
  * Lookup tables and bridge tables (like `work_concepts`, `author_positions`) are linked via foreign keys.

---

## 🧠 Key Benefits of This Architecture

| Feature                      | Benefit                                                             |
| ---------------------------- | ------------------------------------------------------------------- |
| **Chunked File Processing**  | Prevents memory overflow, supports large-scale input                |
| **Decoupled Extract & Load** | Makes debugging and intermediate inspection easier                  |
| **Flexible Configuration**   | Easily adaptable to other structured datasets                       |
| **Normalized Output**        | Supports analytics-ready schema                                     |
| **Logging System**           | Enables restart from failure and transparent tracking of processing |

---

## 🖼️ Architecture Diagram

![OpenAlex ETL Architecture](https://github.com/Gowtham-AI25/etl_pipeline/blob/main/docs/diagram-export-17-7-2025-10_29_03-pm.png)

---

