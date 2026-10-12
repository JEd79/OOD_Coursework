# SIFFT — Python library architecture and function call map

Source: https://github.com/dvla/sifft, branch `main`; inspected 12 October 2026. Package version reported by `pyproject.toml`: **0.9.3**.

## Key architectural point

The caller orchestrates the pipeline; this is a four-package library, **not** an engine that automatically chains read → CSVW constraint validation → write → move. `process_file()` reads and returns a result; `validate_csvw_constraints()` and `write_table()` are invoked separately by the calling application. `confirm_processed()` must be called after downstream success if deferred tracking is used.

## 1. UML class diagram (all identified library classes)

```mermaid
classDiagram
  direction TB
  namespace file_processing {
    class FileProcessingResult {
      +bool success
      +str file
      +DataFrame dataframe
      +str message
      +dict error
      +int rows_processed
      +int rows_corrupt
      +dict metadata
      +str checksum
      +dict tracking_context
    }
    class TrackingResult {
      +bool success
      +str file_path
      +str location
      +dict error
    }
    class FileProcessingException {
      +str message
      +str error_type
      +str details
      +to_dict() dict
    }
  }
  namespace dataframe_validation {
    class ValidationLog {
      +str message
      +bool matching
      +bool missing_column_name
      +bool type_mismatch
      +DataType expected_type
      +DataType actual_type
    }
    class ConstraintViolation {
      +str column
      +str constraint_type
      +str message
      +int violating_rows
      +int total_rows
    }
    class CSVWConstraintReport {
      +bool valid
      +list violations
      +int total_violations
    }
    class ValidationException {
      +str message
      +str error_type
      +str details
      +to_dict() dict
    }
  }
  namespace table_writing {
    class TableWriteOptions {
      +str format
      +str mode
      +list partition_by
      +str schema_mode
      +list merge_keys
      +bool create_if_not_exists
      +bool add_source_metadata
      +dict source_metadata_columns
      +dict extra_metadata
    }
    class TableWriteResult {
      +bool success
      +str table_name
      +int rows_written
      +int rows_updated
      +int rows_deleted
      +str message
      +dict error
      +float duration_seconds
    }
    class FormatHandler {
      <<interface>>
      +supports_merge() bool
      +write(df, table_name, mode, options)
    }
    class DeltaFormatHandler {
      +supports_merge() bool
      +write(df, table_name, mode, options)
    }
    class ParquetFormatHandler {
      +supports_merge() bool
      +write(df, table_name, mode, options)
    }
    class OrcFormatHandler {
      +supports_merge() bool
      +write(df, table_name, mode, options)
    }
    class TableWritingException {
      +str message
      +str error_type
      +str details
      +to_dict() dict
    }
  }
  namespace file_management {
    class FileOperation {
      <<enumeration>>
      MOVE
      LIST
    }
    class FileOperationResult {
      +FileOperation operation
      +str source_path
      +str destination_path
      +bool success
      +list files
      +dict error
    }
    class FileManagementException {
      +str message
      +str error_type
      +str details
      +to_dict() dict
    }
  }
  CSVWConstraintReport "1" *-- "0..*" ConstraintViolation : violations
  FileOperationResult --> FileOperation : operation
  DeltaFormatHandler ..|> FormatHandler : structurally conforms
  ParquetFormatHandler ..|> FormatHandler : structurally conforms
  OrcFormatHandler ..|> FormatHandler : structurally conforms
  DeltaFormatHandler ..> TableWriteOptions : uses
  ParquetFormatHandler ..> TableWriteOptions : uses
  OrcFormatHandler ..> TableWriteOptions : uses
```

`FormatHandler` is a typing `Protocol`; the concrete handlers conform **structurally**, without explicit Python subclass inheritance.

## 2. Module-level dependency diagram

```mermaid
flowchart LR
    APP["Client application / Spark notebook"]
    subgraph FP["file_processing package"]
      FPP["file_processor.py\nprocess_file / process_files_batch / process_directory"]
      FPR["registry.py\nget_handler"]
      FPD["delimited_files.py\nCSV / TSV / delimited"]
      FPE["excel_file.py\nExcel / pandas"]
      FPC["csvw_metadata.py / csvw_schema.py / csvw_dialect.py"]
      FPH["helpers.py / config.py / models.py / exceptions.py"]
      FPT["tracking.py / checksum.py / delta_tracking.py / storage.py"]
      FPP --> FPR
      FPP --> FPD
      FPP --> FPE
      FPP --> FPT
      FPD --> FPC
      FPD --> FPH
      FPE --> FPH
    end
    subgraph DV["dataframe_validation package"]
      DVV["csvw/validator.py\nvalidate_csvw_constraints"]
      DVC["csvw/column.py + csvw/datatype.py"]
      DVS["dataframe_validation.py\nvalidate_schema / apply_schema"]
      DVR["constraint_registry.py"]
      DVH["csvw/models.py / models.py / helpers.py / compat.py / exceptions.py"]
      DVV --> DVC
      DVC --> DVR
      DVS --> DVH
      DVV --> DVH
    end
    subgraph TW["table_writing package"]
      TWW["writer.py\nwrite_table"]
      TWV["validation.py\nvalidate_write_options"]
      TWM["mode_handlers.py\nmode dispatch"]
      TWF["format_handlers.py\nDelta / Parquet / ORC"]
      TWH["models.py / config.py / helpers.py / exceptions.py"]
      TWW --> TWV
      TWW --> TWM
      TWM -. "registered writer functions" .-> TWW
      TWW --> TWF
      TWW --> TWH
    end
    subgraph FM["file_management package"]
      FMO["file_operations.py\nsafe_move / list_files_in_directory"]
      FMH["models.py / exceptions.py"]
      FMO --> FMH
    end
    APP -->|"1 process_file"| FPP
    APP -->|"2 optional validation"| DVV
    APP -->|"3 write_table"| TWW
    APP -.->|"optional file operation"| FMO
    APP -.->|"4 confirm_processed if tracking"| FPT
```

Each package additionally has its own `__init__.py` that re-exports its public functions and types. The `dataframe_validation/csvw` directory also has `__init__.py`.

### Full source module inventory (excluding tests, docs, and samples)

- `file_processing/` (16): `__init__.py`, `file_processor.py`, `delimited_files.py`, `excel_file.py`, `csvw_metadata.py`, `csvw_schema.py`, `csvw_dialect.py`, `registry.py`, `tracking.py`, `checksum.py`, `delta_tracking.py`, `storage.py`, `helpers.py`, `models.py`, `config.py`, `exceptions.py`.
- `dataframe_validation/` (12): `__init__.py`, `dataframe_validation.py`, `constraint_registry.py`, `compat.py`, `helpers.py`, `models.py`, `exceptions.py`, `csvw/__init__.py`, `csvw/validator.py`, `csvw/column.py`, `csvw/datatype.py`, `csvw/models.py`.
- `table_writing/` (9): `__init__.py`, `writer.py`, `validation.py`, `mode_handlers.py`, `format_handlers.py`, `models.py`, `helpers.py`, `config.py`, `exceptions.py`.
- `file_management/` (4): `__init__.py`, `file_operations.py`, `models.py`, `exceptions.py`.

## 3. Execution sequence for a complete ingestion

```mermaid
sequenceDiagram
    autonumber
    actor App as Calling application
    participant FP as file_processing.file_processor
    participant Track as file_processing.tracking
    participant Reg as file_processing.registry
    participant Read as delimited_files / excel_file
    participant CSVW as CSVW metadata/schema/dialect
    participant Spark as Spark / pandas
    participant Val as dataframe_validation
    participant Writer as table_writing.writer
    participant Mode as mode_handlers / writer mode functions
    participant Format as format_handlers / DeltaTable
    participant Files as file_management
    App->>FP: process_file(path, spark, ...)
    opt tracking configured
      FP->>Track: check_already_processed(...)
      Track-->>FP: seen / not seen
    end
    alt already processed
      FP-->>App: FileProcessingResult(success=True, dataframe=None)
    else needs processing
      FP->>FP: _validate_inputs() -> check_file_exists()
      FP->>FP: _select_handler() -> registry.get_handler()
      FP->>FP: _merge_options()
      FP->>Read: selected handler(path, spark, options)
      alt delimited file
        Read->>CSVW: load_csvw_metadata()
        alt metadata and valid schema
          Read->>CSVW: csvw_to_spark_schema(), extract_csvw_dialect()
          Read->>Spark: spark.read.schema(...).csv(path)
        else no usable metadata
          Read->>Read: _process_with_inference() / detect_* if auto
          Read->>Spark: spark.read.csv(path)
        end
        Read->>Read: _validate_and_return() / checksum
      else Excel file
        Read->>Spark: pandas.read_excel() / spark.createDataFrame()
      end
      Read-->>FP: FileProcessingResult
      FP-->>App: FileProcessingResult
      opt read succeeded and validation requested
        App->>Val: validate_csvw_constraints(df, metadata)
        Val->>Val: column, datatype, primary key checks
        Val-->>App: CSVWConstraintReport
      end
      opt checks pass and write requested
        App->>Writer: write_table(df, table, spark, options)
        Writer->>Writer: add_source_metadata() if configured
        Writer->>Writer: validate_write_options()
        Writer->>Mode: get_mode_handler() -> registered mode function
        alt append / overwrite / errorIfExists
          Mode->>Format: get_format_handler().write()
          Format->>Spark: df.write.saveAsTable(table)
        else Delta merge
          Mode->>Spark: DeltaTable.merge(...).execute()
        end
        Mode-->>Writer: TableWriteResult
        Writer-->>App: TableWriteResult
      end
      opt write succeeded and tracking configured
        App->>FP: confirm_processed(result, spark)
        FP->>Track: record_processed(...)
        Track->>Spark: marker via fsspec OR Delta tracking table
      end
      opt caller elects to move or list source
        App->>Files: safe_move() / list_files_in_directory()
        Files-->>App: FileOperationResult
      end
    end
```

## 4. Function-by-function order

1. **Caller → `file_processing.file_processor.process_file(path, spark, ...)`:** if tracking is configured, it first calls `tracking.check_already_processed`, which delegates to the marker-file (`checksum.is_file_processed`) or Delta (`delta_tracking.is_file_processed_delta`) implementation. An already-seen file returns a successful skip result, **but its `dataframe` is `None`**.
2. **`process_file` → `_validate_inputs` → `helpers.check_file_exists`:** verifies Spark exists, source path is a local `Path`-accessible nonempty file. The current implementation's local `Path` check is worth noting if using remote cloud URIs.
3. **`process_file` → `_select_handler` → `registry.get_handler`:** custom handlers take priority. Then `.xls/.xlsx` choose `_process_excel_file`, recognized delimited extensions choose `_process_delimited_file`, and unknown/no extensions fall back to delimited parsing.
4. **`process_file` → `_merge_options` → selected handler:** defaults are merged with user settings, then storage and explicit CSVW paths are added.
5. **Delimited branch → `_process_delimited_file` → `csvw_metadata.load_csvw_metadata`:** metadata can be discovered as `<stem>.csv-metadata.json`, `<stem>-metadata.json`, or `metadata.json`, or explicitly provided.
6. **Delimited CSVW branch:** `_process_with_csvw` calls `csvw_schema.csvw_to_spark_schema` (which calls `csvw_metadata.get_table_schema`, `_convert_column_to_field`, `_map_csvw_type_to_spark`), `csvw_dialect.extract_csvw_dialect`, and `extract_csvw_null_values`. It configures `spark.read.schema(...).csv(...)`, then `_validate_and_return`, `compute_file_checksum`.
7. **Delimited inference branch:** `_process_with_inference` optionally calls `helpers.detect_delimiter` / `detect_header` only when the corresponding option is `'auto'`, then `spark.read.csv`, `_validate_and_return`, `compute_file_checksum`. `_validate_and_return` counts records, detects corrupt rows, and returns `FileProcessingResult`.
8. **Excel branch:** `_process_excel_file` checks file size, calls `pandas.read_excel`, then `spark.createDataFrame`, and returns `FileProcessingResult`.
9. **Optional schema checks:** caller invokes `dataframe_validation.validate_schema(df, expected_schema)` (returns `dict[str, ValidationLog]`) and optionally `apply_schema` (uses `compat.safe_cast`), as separate API calls.
10. **Optional CSVW checks:** caller invokes `dataframe_validation.validate_csvw_constraints(df, result.metadata)`. In `csvw/validator.py` it calls `_get_table_schema`, iterates columns, calls `_validate_column_constraints` (batched/individual including custom `constraint_registry.get_constraint_validator`) and `_validate_datatype_constraints`, then `_validate_primary_key`, and returns `CSVWConstraintReport` containing `ConstraintViolation` entries.
11. **Caller → `table_writing.writer.write_table(df, table_name, spark, options)`:** creates default `TableWriteOptions` if absent, optionally calls `helpers.add_source_metadata`, then calls `validation.validate_write_options`.
12. **Pre-write validation order:** `_validate_inputs`, `_validate_dataframe_not_empty`, `_validate_format`, `_validate_mode`, `_validate_merge_keys`, `_validate_partitions`, `_validate_table_exists`, `_validate_schema_mode`.
13. **Mode dispatch:** `mode_handlers.get_mode_handler(options.mode)` selects `_write_append`, `_write_overwrite`, `_write_error_if_exists`, or `_write_merge` registered in `writer.py` at import time. The first three use `format_handlers.get_format_handler` and `.write(...)` from a `DeltaFormatHandler`, `ParquetFormatHandler`, or `OrcFormatHandler`, eventually `df.write.saveAsTable`. Merge uses `DeltaTable.forName(...).merge(...).execute()`; if table is missing and creation is allowed it calls `_write_append`.
14. **Write result:** the mode handler returns `TableWriteResult` and `write_table` adds duration.
15. **Optional deferred confirmation:** after verifying successful validation and write, caller calls `file_processing.confirm_processed(result, spark)`. This calls `tracking.record_processed`, dispatching to `checksum.mark_file_processed` (marker via fsspec) or `delta_tracking.mark_file_processed_delta` (Delta tracking table). It is **not automatically invoked** by `process_file` or `write_table`.
16. **Optional file management:** caller separately calls `file_management.file_operations.safe_move` or `list_files_in_directory`, which use fsspec and return `FileOperationResult`.

`process_files_batch` and `process_directory` are wrappers: they loop over files, call `process_file`, collect results, and optionally raise an aggregate error. They do not automatically validate CSVW constraints or write tables.

### Notes

- For known extensions, delimiter defaults are configured (`.csv` comma, `.tsv` tab, `.dat` pipe); auto-detection is active when the option is explicitly set to `'auto'`, or through fallback defaults for unknown/no extension.
- CSVW metadata may influence **parsing schema and dialect**, but the **CSVW constraint checker is a separate call**.
- `process_file` can return `success=True` for a previously processed file with `dataframe=None`; check both `result.success` and `result.dataframe is not None` before validation/writing.
- `count()`, `collect()`, `saveAsTable()` and Delta merge execution can trigger Spark jobs; ingestion is not one atomic transaction spanning all four packages.
- The source code is the authority for tracking semantics; the documentation's older `process_file` tracking example does not show the required deferred `confirm_processed` call.

### Sources

- https://raw.githubusercontent.com/dvla/sifft/main/file_processing/file_processor.py
- https://raw.githubusercontent.com/dvla/sifft/main/file_processing/delimited_files.py
- https://raw.githubusercontent.com/dvla/sifft/main/dataframe_validation/csvw/validator.py
- https://raw.githubusercontent.com/dvla/sifft/main/table_writing/writer.py
- https://raw.githubusercontent.com/dvla/sifft/main/table_writing/format_handlers.py
- https://raw.githubusercontent.com/dvla/sifft/main/file_management/file_operations.py
- https://raw.githubusercontent.com/dvla/sifft/main/pyproject.toml
