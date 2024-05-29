# Patstat Importer

A Python package designed for importing PATSTAT database tables and converting them to Parquet format. It facilitates easy manipulation and analysis of patent data.

## Installation

Ensure you have Python installed on your machine. You can download it from python.org. This package is compatible with Python 3.7 and above.

You can install your package directly from GitHub using pip

```bash
pip install git+https://github.com/sbreschi/patstat-importer.git
```

## Instructions

1. Download all Patstat zip files from Patstat Global at the [EPO](https://publication-bdds.apps.epo.org/raw-data/products)
2. Put all the zip files in a separate folder of your choice
3. Important: download and put in the same folder also the index and documentation file (index_documentation_scripts_PATSTAT_Global)

Note: there is no need to unzip the files.

## Usage

Use the `process_tables` function, specifying:

1. the `input_folder`, i.e. the folder where you put the zip files downloaded from EPO-PATSTAT
2. the `output_folder`, i.e. the folder where you want to save the parquet tables
3. (optional) Indicate a list of tables to import (see examples, below). If you do not specify any table, by default it will read all tables
4. (optional) Set `verbose=True` if you want to track the execution
5. (optional) Specify the amount of RAM available (default 8Gb). Some of the tables are huge and are read/written in chunks.

### Examples

Read a single table. 

```python
from patstat_importer import process_tables
process_tables(input_folder, output_folder, tables_to_process=['tls201'], verbose=True, total_ram_gb=32.0)
```


Read a list of tables. 

```python
from patstat_importer import process_tables
process_tables(input_folder, output_folder, tables_to_process=['tls201', 'tls202'], verbose=True, total_ram_gb=32.0)
```


Read all tables (just omit the `tables_to_process` argument). 

```python
from patstat_importer import process_tables
process_tables(input_folder, output_folder, verbose=True, total_ram_gb=32.0)
```

The table below provides the full list of table names to import and the name under which they are imported.

| Input      | Saved as ...                     |
|------------|--------------------------------|
| tls201     | tls201_appln                   |
| tls202     | tls202_appln_title             |
| tls203     | tls203_appln_abstr             |
| tls204     | tls204_prior                   |
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


## Use parquet files

You can easily read and query parquet files, either with `pandas.read_parquet()` 

```python
import pandas as pd
df = pd.read_parquet('./tls201_appln.parquet', columns=['appln_id', 'appln_auth'], filters=[('appln_auth', '==', 'EP')])
```

or with `pyarrow`:

```python
from pyarrow.parquet import ParquetFile
import pyarrow as pa

pf = ParquetFile('./tls201_appln.parquet')
first_ten_rows = next(pf.iter_batches(batch_size=10))
df = pa.Table.from_batches([first_ten_rows]).to_pandas()
```

For Stata users, you can try using the code you find at this [site](https://github.com/mcaceresb/stata-parquet) (though I personally discourage using Stata with this data).


## Handling of missing or unknown values

The files coming from PATSTAT do not contain any NULL values. Depending on the data type / domain, PATSTAT represents missing values like this:
* Missing values in attributes of type `date` are represented as '9999-12-31'.
* Missing values in attributes of type `string` are represented as zero length strings (like "") or as fixed length strings containing spaces (like "  ").
* Missing values in numerical attributes are represented as number zero.

When importing the data to parquet, I followed these rules:
* Missing values in variables of type `date` are represented as `NaT` (i.e., Not a Time)
* Missing values in variables of type `string` are represented as `None`
* Missing values in numerical variables are represented as number zero.
* Missing values in variables denoting years are represented by the integer 9999.

## Testing

The library has been tested on Patstat Global 2023 Autumn and 2024 Spring.

## Disclaimer

The package has been successfully tested on the aforementioned PATSTAT versions; however, it may not function correctly with future (or past) releases. This is particularly true if the EPO makes significant changes to the structure and content of the "index_documentation_scripts_PATSTAT_Global" file. This file is crucial as it provides the names of tables, the variables included in each table, and their data types. I will strive to maintain the library to ensure compatibility with future versions of PATSTAT as much as possible.

