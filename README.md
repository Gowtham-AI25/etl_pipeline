#📚 OpenAlex ETL Pipeline

This project provides a modular and scalable ETL (Extract, Transform, Load) pipeline for processing large-scale scholarly metadata from the OpenAlex dataset. The pipeline handles the complete data processing flow — from raw CSV ingestion to clean, validated, and relational-ready database inserts.

##🚀 Features

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

---

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

