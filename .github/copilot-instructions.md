# Loadex - Wind Turbine Loads Data Exchange

## Project Purpose
Loadex processes and analyzes wind turbine loads simulation data for engineering certification purposes, following methods described in **IEC 61400-1** (onshore wind turbines), **IEC 61400-3-1** (offshore bottom-fixed), and **IEC 61400-3-2** (offshore floating). The tool extracts timeseries data from DNV Bladed output files, computes statistics (min/max/mean/rainflow equivalent loads), and stores results in SQLite databases for comparison across Design Load Cases (DLCs).

## Domain Standards Context
- **DLCs (Design Load Cases)**: Defined in IEC 61400-1 Annex A, representing combinations of wind conditions, operational states, and other conditions for load simulations
- **Fatigue Analysis**: Uses rainflow cycle counting to compute Damage Equivalent Loads (DELs) with Wöhler exponents (typically m=3 for steel, m=4 for welds, m=5 for fiberglass)
- **Ultimate Loads**: Maximum/minimum values with partial safety factors for structural design verification
- **Sensors**: Physical measurement locations (e.g., "Tower Mx", "Tower My", "Tower Mz" for tower base moments)

## Architecture & Key Components

### Core Class Hierarchy
- **DataSet** ([src/loadex/classes/dataset.py](src/loadex/classes/dataset.py)): Central orchestration class
  - Contains `FileList`, `SensorList`, and `DesignLoadCaseList`
  - Workflow: `find_files()` → `set_sensors()` → `generate_statistics()` → `to_sql()` or `to_dataframe()`
  - Supports serialization: `to_sql()` saves to database, `from_sql()` reloads complete state

- **File Formats** ([src/loadex/formats/](src/loadex/formats/)): Abstract `File` base class with concrete implementations
  - `BladedOutFile`: Uses `dnv_bladed_results` library for DNV Bladed binary files (`.$TE`, `.$PJ`)
  - `ParquetFile`: Uses pandas for Apache Parquet files
  - **Pattern**: Lazy loading via `@property` decorators (see `BladedOutFile.run`, `BladedOutFile.sensors`)
  - **Pattern**: `clear_connections()` method required before multiprocessing to avoid pickling issues

- **Statistics System** ([src/loadex/classes/statistics.py](src/loadex/classes/statistics.py)):
  - Base `Statistic` class with `aggregation_function(timeseries, timestamps)` abstract method
  - Built-ins: `Mean`, `Max`, `Min`, `Std`, `AbsMax`
  - `EquivalentLoad` (DEL): Rainflow cycle counting with Wöhler exponents (m=3,4,5)
  - Each `Sensor` has a list of statistics computed during `generate_statistics()`

### Data Flow
1. **File Discovery**: `DataSet.find_files()` uses `Path.rglob()` to find files matching format-specific patterns
2. **Sensor Detection**: `set_sensors(fileindex=0)` reads sensor names from first file, creates `SensorList`
3. **Statistics Generation**: 
   - **Sequential**: Iterates through files, calls `File.generate_statistics(sensorlist)`
   - **Parallel** (`parallel=True`): Uses `multiprocessing.Pool` (default 8 processes)
     - CRITICAL: Calls `file.clear_connections()` before pool submission to avoid serialization errors
   - Results cached in DataFrame, then merged into `Sensor.data` via `_insert_generated_statistics()`
4. **Persistence**: `to_sql()` serializes FileList metadata + Sensor statistics to SQLite using SQLAlchemy ORM

### Database Layer
- **SQLAlchemy ORM** ([src/loadex/data/datamodel.py](src/loadex/data/datamodel.py)): Defines tables for files, sensors, statistics
- **SQLite Configuration** ([src/loadex/data/database.py](src/loadex/data/database.py)):
  - WAL mode enabled for concurrent access
  - 60-second busy timeout to handle file locks
  - `get_sqlite_session()` creates/connects to database, auto-creates tables

## Development Workflows

### Environment Setup
Project uses Python venv with pip and editable install:
```powershell
python -m venv venv
venv\Scripts\activate
pip install -e .  # Editable install - changes in src/ immediately available
```
Dependencies are defined in [pyproject.toml](pyproject.toml). Editable mode (`-e`) ensures local testing recognizes latest code changes without reinstalling.

### Running Tests
```powershell
pytest  # Runs all tests in test/ directory
```
Tests use sample Bladed files in [test/data/Bladed/](test/data/Bladed/) (`idling.*` and `parked.*` files).

### CLI Tools
- **process_files** ([src/loadex/cli/process_files.py](src/loadex/cli/process_files.py)):
  ```powershell
  python -m loadex.cli.process_files <directory> -f BladedOutFile -db statistics.db
  ```
  - Processes all files in directory, writes `statistics.db` and `.loadex_log`
  - Runs statistics generation in parallel by default

### Building/Packaging
- Build: `package_build.bat` (root directory)
- Release: `package_release.bat` (root directory)

## Project-Specific Conventions

### Multiprocessing Pattern
When implementing parallel processing:
1. Call `file.clear_connections()` before passing to worker processes
2. Use `multiprocessing.Pool.apply_async()` pattern (see [dataset.py#L113-L126](src/loadex/classes/dataset.py#L113-L126))
3. Files with `_run` or `_sensors` cached properties must be cleared to avoid pickling errors

### Lazy Loading Pattern
Properties like `BladedOutFile.run` and `BladedOutFile.sensors` use lazy initialization:
```python
@property
def run(self):
    if self._run is None:
        self._run = bd.ResultsApi.get_run(...)
    return self._run
```
This defers expensive I/O until data is actually accessed.

### Format Registration
New file formats must:
1. Inherit from `File` base class
2. Implement: `sensor_names`, `get_time()`, `get_data()`, `to_dataframe()`, `set_metadata_from_file()`
3. Define `defaultExtensions()` static method
4. Register in [formats/__init__.py](src/loadex/formats/__init__.py) `format_list`

### Design Load Cases (DLCs)
- DLCs group files for comparison (e.g., "DLC1.2", "DLC6.2")
- Assign via `file.dlc = dlc_object` (see [designloadcases.py](src/loadex/classes/designloadcases.py))
- Files can also have `group` and `hours` attributes for sub-categorization

### MultiIndex DataFrames
`DataSet.to_dataframe()` returns MultiIndex columns: `(filelist|sensor_name, statistic_name)`
This enables side-by-side comparison of metadata and sensor statistics.

## External Dependencies
- **dnv_bladed_results**: DNV's library for reading Bladed binary output files (proprietary format)
- **rainflow**: Rainflow cycle counting for fatigue analysis
- **pyarrow**: Required for Parquet file I/O via pandas

## Key Files to Reference
- [dataset.py](src/loadex/classes/dataset.py): Core workflow logic and parallel processing
- [filelist.py](src/loadex/classes/filelist.py): File abstraction and metadata handling
- [sensorlist.py](src/loadex/classes/sensorlist.py): Sensor statistics aggregation
- [test_dataset.py](test/test_dataset.py): End-to-end usage example with parallel execution
