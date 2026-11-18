# Patstat Importer

A Python package for importing PATSTAT Global database tables and converting them to Parquet files, ready for analysis. It can also download the PATSTAT zip files directly from the EPO BDDS portal.

The package is built on top of [polars](https://pola.rs/) for fast, memory-efficient reading of the multi-gigabyte CSV exports.

## Installation

Ensure you have Python installed on your machine. You can download it from [python.org](https://www.python.org/). This package requires Python 3.10 or newer (the minimum supported by current polars releases).

Install directly from GitHub with pip:

```bash
pip install git+https://github.com/sbreschi/patstat-importer.git
```

If you also want to use the EPO portal downloader, install the optional `download` extra and then download the bundled Chromium that Playwright drives:

```bash
pip install "patstat_importer[download] @ git+https://github.com/sbreschi/patstat-importer.git"
playwright install chromium
```

After installation the package exposes two console scripts:

* `patstat-download` — fetch the PATSTAT zip files from the EPO BDDS portal
* `patstat-process` — convert the zip files to Parquet

## Step 1 — Get the PATSTAT zip files

You can either download the files manually or let the bundled downloader do it for you.

### Option A — Manual download

1. Download all PATSTAT zip files from PATSTAT Global at the [EPO BDDS portal](https://publication-bdds.apps.epo.org/raw-data/products).
2. Put all the zip files in a folder of your choice.
3. **Important:** download and put in the same folder the documentation file as well (e.g. `index_documentation_scripts_PATSTAT_Global*.zip` on older releases or `DOC_SQL.zip` on more recent ones). The documentation ZIP is needed to read the CREATE TABLE scripts and the expected row counts. It is detected automatically as the only zip in the folder that is not a `data_PATSTAT*` bulk archive and contains `.sql` scripts; if more than one zip qualifies, name it explicitly with `doc_zip` / `--doc-zip`.

There is no need to unzip anything.

Both supported zip layouts work out of the box:
* the older layout, where each release is wrapped in a single `data_PATSTAT_*.zip` containing the per-table `tls*.zip` files;
* the newer BDDS layout, where each `tls*.zip` sits directly in the folder.

### Option B — Automated download with `patstat-download`

The `patstat-download` command logs into the EPO BDDS portal, navigates to the PATSTAT Global product page, and downloads every file of the chosen edition into a folder of your choice. It needs a valid EPO subscription.

Set your EPO credentials as environment variables (the command never reads them from the command line):

```cmd
set EPO_USERNAME=your@email.com
set EPO_PASSWORD=yourpassword
```

Then run the downloader. The `--edition` value must match the edition name as shown on the BDDS portal:

```cmd
patstat-download --edition "PATSTAT Global 2025 Autumn" ^
                 --output-dir "D:/Download/Patstat/"
```

Since late 2026 the EPO login asks for a second factor after the password: a one-time code sent to the e-mail address of the EPO account. The downloader detects this step, clicks "Send email" and then waits on the terminal for you to type the code (or to press Enter after clicking the verification link in the e-mail). Because of this, `patstat-download` has to be run from an interactive terminal; in a non-interactive shell it stops before requesting the e-mail and exits with an error.

By default the browser runs headless. If something goes wrong (for instance the first time you log in, or after a portal UI change), pass `--visible` to watch the session:

```cmd
patstat-download --edition "PATSTAT Global 2025 Autumn" ^
                 --output-dir "D:/Download/Patstat/" --visible
```

The downloader is resumable: zip files that are already present on disk and look valid are skipped. Truncated or broken files are removed and re-downloaded.

## Step 2 — Convert the zip files to Parquet

You can convert the zip files from Python or from the command line. In both cases the documentation ZIP and the inner subfolder containing the CREATE TABLE scripts are auto-detected, so you normally do not need to point at them explicitly.

### From Python

Use the `process_tables` function, specifying:

1. `data_folder` — the folder where you put the zip files downloaded from EPO-PATSTAT (data + documentation);
2. `output_folder` — the folder where the Parquet tables will be written;
3. (optional) `tables_to_process` — a list of tables to import (see examples below). If omitted, all tables are processed;
4. (optional) `verbose=True` to print detailed progress;
5. (optional) `total_ram_gb` — kept for API compatibility; no longer used (polars manages memory internally);
6. (optional) `doc_zip` — filename of the documentation zip (e.g. `'DOC_SQL.zip'`). Auto-detected if omitted;
7. (optional) `sql_subfolder` — inner subfolder of the documentation zip containing the CREATE TABLE scripts (e.g. `'CreateTableScripts'` or `'SQL/CreateTable'`). Auto-detected if omitted;
8. (optional) `tmp_dir` — folder where the CSV files are extracted before conversion (see *Disk space and time* below). Defaults to `output_folder`; point it at a fast local disk when the output goes to a network share.

#### Examples

Read a single table:

```python
from patstat_importer import process_tables
process_tables(data_folder, output_folder, tables_to_process=['tls201'], verbose=True)
```

Read a list of tables:

```python
from patstat_importer import process_tables
process_tables(data_folder, output_folder, tables_to_process=['tls201', 'tls202'], verbose=True)
```

Read all tables (omit `tables_to_process`):

```python
from patstat_importer import process_tables
process_tables(data_folder, output_folder, verbose=True)
```

### From the command line

The same functionality is exposed via `patstat-process`:

```cmd
patstat-process --data-folder "D:/Download/Patstat/" ^
                --output-folder "D:/Patstat/parquet/" ^
                --verbose
```

To process only a subset of tables, pass them with `--tables`:

```cmd
patstat-process --data-folder "D:/Download/Patstat/" ^
                --output-folder "D:/Patstat/parquet/" ^
                --tables tls201 tls206 ^
                --verbose
```

Other flags:

* `--doc-zip` — filename of the documentation zip (auto-detected if omitted);
* `--sql-subfolder` — inner subfolder of the documentation zip containing the CREATE TABLE scripts (auto-detected if omitted);
* `--tmp-dir` — folder for the temporary CSV extraction (default: the output folder); use a local disk when the output folder is on a network share (see below);
* `--ram` — informational only; kept for backward compatibility.

### Disk space and time

Each table is converted in one pass: all of its CSV files are extracted from the zips into a temporary folder, read by polars and streamed into a single Parquet file, and the temporary folder is then deleted. The temporary folder therefore needs room for the largest table's uncompressed CSVs. In PATSTAT Global 2026 Autumn that is tls231 (143 GB), followed by tls203 (100 GB), so allow about 150 GB. The temporary folder defaults to the output folder; when the output goes to a network share, point `tmp_dir` / `--tmp-dir` at a local disk, which is faster and avoids failures on transient network errors.

For reference, the 2026 Autumn release is 85 GB of zips and converts to 73 GB of Parquet. On a desktop PC extracting to a local SSD and writing the Parquet files to a NAS, the whole release took about an hour.

### Table naming

The table below provides the full list of input table names and the name under which they are saved.

| Input      | Saved as ...                     |
|------------|--------------------------------|
| tls201     | tls201_appln                   |
| tls202     | tls202_appln_title             |
| tls203     | tls203_appln_abstr             |
| tls204     | tls204_appln_prior             |
| tls205     | tls205_tech_rel                |
| tls206     | tls206_person                  |
| tls207     | tls207_pers_appln              |
| tls209     | tls209_appln_ipc               |
| tls210     | tls210_appln_n_cls             |
| tls211     | tls211_pat_publn               |
| tls212     | tls212_citation                |
| tls214     | tls214_npl_publn               |
| tls215     | tls215_citn_categ              |
| tls216     | tls216_appln_contn             |
| tls222     | tls222_appln_jp_class          |
| tls224     | tls224_appln_cpc               |
| tls225     | tls225_docdb_fam_cpc           |
| tls226     | tls226_person_orig             |
| tls227     | tls227_pers_publn              |
| tls228     | tls228_docdb_fam_citn          |
| tls229     | tls229_appln_nace2             |
| tls230     | tls230_appln_techn_field       |
| tls231     | tls231_inpadoc_legal_event     |
| tls801     | tls801_country                 |
| tls803     | tls803_legal_event_code        |
| tls901     | tls901_techn_field_ipc         |
| tls902     | tls902_ipc_nace2               |
| tls904     | tls904_nuts                    |

## Validation

Every Parquet file produced is validated against the documentation ZIP before the next table is processed:

* **Row count** — must match the value reported in PATSTAT's `RowCount` report.
* **Schema** — the columns written to Parquet must match the columns read from the CSV.

If either check fails, processing aborts with a `RuntimeError` and the offending Parquet file is kept on disk so you can inspect it.

## Using the Parquet files

You can query the Parquet files with the tool of your choice.

With **polars** (the engine used by `patstat-importer` itself):

```python
import polars as pl
df = pl.scan_parquet('./tls201_appln.parquet') \
       .filter(pl.col('appln_auth') == 'EP') \
       .select(['appln_id', 'appln_auth']) \
       .collect()
```

With **pandas**:

```python
import pandas as pd
df = pd.read_parquet(
    './tls201_appln.parquet',
    columns=['appln_id', 'appln_auth'],
    filters=[('appln_auth', '==', 'EP')],
)
```

With **pyarrow**:

```python
from pyarrow.parquet import ParquetFile
import pyarrow as pa

pf = ParquetFile('./tls201_appln.parquet')
first_ten_rows = next(pf.iter_batches(batch_size=10))
df = pa.Table.from_batches([first_ten_rows]).to_pandas()
```

For Stata users, you can try [stata-parquet](https://github.com/mcaceresb/stata-parquet) (though I personally discourage using Stata with this data).

## Handling of missing or unknown values

The files coming from PATSTAT do not contain any NULL values. Depending on the data type / domain, PATSTAT represents missing values like this:

* Missing values in attributes of type `date` are represented as `9999-12-31`.
* Missing values in attributes of type `string` are represented as zero-length strings (like `""`) or as fixed-length strings containing spaces (like `"  "`).
* Missing values in numerical attributes are represented as number zero.

When importing the data to Parquet, the following rules are applied:

* Dates are kept as PATSTAT delivers them: `9999-12-31` stays a real date value and is **not** converted to `null` (in 2026 Autumn, 6.4 million of the 146 million `appln_filing_date` values are `9999-12-31`). Only an empty field becomes `null` (polars) / `NaT` (pandas). Filter the sentinel out explicitly, e.g. `.filter(pl.col('appln_filing_date') != pl.date(9999, 12, 31))`.
* Strings are stripped of surrounding spaces; an empty or all-space string becomes `null` / `None`.
* Missing values in numerical variables are represented as number zero.
* Missing values in variables denoting years are represented by the integer 9999.

## Testing

The library has been tested on **PATSTAT Global 2025 Autumn** and **PATSTAT Global 2026 Autumn** (full 28-table imports, all row counts matching). The 2026 Autumn release changed the format of the RowCount report; the parser accepts it alongside the earlier formats.

## Disclaimer

The package has been successfully tested on the aforementioned PATSTAT version; however, it may not function correctly with future (or past) releases. This is particularly true if the EPO makes significant changes to the structure and content of the documentation ZIP (`index_documentation_scripts_PATSTAT_Global*` or `DOC_SQL.zip`). That file is crucial as it provides the table names, the variables included in each table, their data types, and the expected row counts used for validation. I will strive to keep the library up to date with future PATSTAT releases.
