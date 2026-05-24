import argparse
import os
import re
import zipfile
from io import BytesIO
from zipfile import ZipFile

import polars as pl

from .zip_handling import list_zip_files, zip_tables, list_files_in_zip
from .get_dtypes import (
    read_sql_script,
    parse_sql_script,
    convert_sql_to_polars_dtype,
    _find_doc_zip,
)


def patstat_tables(directory):
    all_zip_files = list_zip_files(directory)
    zip_files = []
    for file in all_zip_files:
        zip_file_path = os.path.join(directory, file)
        zips = list_files_in_zip(zip_file_path)
        zip_files.extend(zips)
    return sorted(set(x.split('_')[0] for x in zip_files if 'tls' in x))


_ROW_COUNT_LINE = re.compile(
    r'^\s*(tls\d+\w*)\s+count:\s+(\d+)\s*$', re.IGNORECASE
)


def table_names(directory, doc_zip=None):
    """Read the per-table expected row counts from the RowCount report file
    in the documentation ZIP.

    Tolerant of two formats observed across PATSTAT releases:
        ``tls201_appln Count: 140525582``   (older)
        ``tls201_appln count: 140525582``   (newer, lower-case, surrounded by
        separator lines and ``(1 row affected)`` noise)
    """
    if doc_zip is None:
        doc_zip = _find_doc_zip(directory)
    filename = doc_zip
    filenames = list_files_in_zip(os.path.join(directory, filename))
    file = next(
        x for x in filenames
        if 'RowCount' in x and (x.endswith('.txt') or x.endswith('.rpt'))
    )

    with zipfile.ZipFile(os.path.join(directory, filename), 'r') as z:
        if file not in z.namelist():
            raise FileNotFoundError("RowCount file not found in documentation zip.")
        with z.open(file) as f:
            raw_text = f.read().decode('utf-8', errors='replace')

    rows = []
    for line in raw_text.splitlines():
        m = _ROW_COUNT_LINE.match(line)
        if not m:
            continue
        table_name = m.group(1)
        count = int(m.group(2))
        short_name = table_name.split('_')[0]
        rows.append({
            'short_name': short_name,
            'table_name': table_name,
            'count': count,
        })

    if not rows:
        raise ValueError(
            "Could not parse any 'tls... count: N' lines from "
            f"{file!r} in the documentation ZIP."
        )

    return pl.DataFrame(rows).select(['short_name', 'table_name', 'count'])


def process_tables(data_folder, output_folder, tables_to_process=None, verbose=False,
                   total_ram_gb=8.0, doc_zip=None, sql_subfolder=None):
    """
    Process PATSTAT tables from ZIP archives and save them as Parquet files.

    Args:
        data_folder (str): Directory containing the ZIP files with data.
        output_folder (str): Directory where Parquet files will be saved.
        tables_to_process (list, optional): Table names to process. None = all tables.
        verbose (bool, optional): Print detailed processing logs.
        total_ram_gb (float, optional): Kept for API compatibility; no longer used.
        doc_zip (str, optional): Filename of the documentation zip (e.g. 'DOC_SQL.zip').
            Auto-detected if None.
        sql_subfolder (str, optional): Inner subfolder of the documentation zip
            containing the CREATE TABLE scripts (e.g. 'CreateTableScripts' or
            'SQL/CreateTable'). Auto-detected if None.
    """
    if tables_to_process is not None:
        valid_tables = patstat_tables(data_folder)
        for name in tables_to_process:
            if name not in valid_tables:
                raise ValueError(f"Invalid table name: {name}")

    print("Starting to process tables...\n")
    os.makedirs(output_folder, exist_ok=True)

    all_zip_files = list_zip_files(data_folder)
    if verbose:
        print('ZIP data files found:\n')
        for x in all_zip_files:
            print(x)
        print()

    zip_files = []
    for file in all_zip_files:
        zip_file_path = os.path.join(data_folder, file)
        zips = list_files_in_zip(zip_file_path)
        zip_files.extend(os.path.join(data_folder, file, z) for z in zips)

    if tables_to_process is None:
        unique_tables = set(fn.split('tls')[1] for fn in zip_files if 'tls' in fn)
        tables_to_process = sorted(set(f'tls{t.split("_")[0]}' for t in unique_tables))
    else:
        tables_to_process = sorted(set(tables_to_process))

    if verbose:
        print('Tables to process:\n')
        for x in tables_to_process:
            print(x)
        print()

    counts_df = table_names(data_folder, doc_zip=doc_zip)
    processed_tables = []

    for table_prefix in tables_to_process:
        focal_table = table_prefix
        relevant_tables = zip_tables(focal_table, zip_files)

        relevant_script = read_sql_script(
            data_folder, focal_table, doc_zip=doc_zip, sql_subfolder=sql_subfolder
        )
        columns_info = parse_sql_script(relevant_script)
        polars_schema = convert_sql_to_polars_dtype(columns_info)

        if focal_table == 'tls201':
            polars_schema['appln_nr_epodoc'] = pl.String

        date_cols = {col for col, dtype in polars_schema.items()
                     if dtype in (pl.Date, pl.Datetime)}

        full_table_name = (
            counts_df.filter(pl.col('short_name') == focal_table)['table_name'][0]
        )
        output_filename = os.path.join(output_folder, f"{full_table_name}.parquet")

        frames = []
        csv_columns = None

        for input_data in relevant_tables:
            zips = [x for x in list_files_in_zip(input_data) if focal_table in x]
            with ZipFile(input_data) as z:
                for table in zips:
                    with z.open(table) as z2:
                        print(f'Processing {table}')
                        if table.lower().endswith('.csv'):
                            # New BDDS portal: tls*.zip contains the CSV directly.
                            raw_bytes = z2.read()
                        else:
                            # Old layout: tls*.zip is itself nested inside a
                            # data_PATSTAT*.zip wrapper, and contains an inner
                            # CSV named after the zip.
                            csv_name = table.replace('.zip', '.csv')
                            with ZipFile(BytesIO(z2.read())) as nested_zip:
                                with nested_zip.open(csv_name) as csvfile:
                                    raw_bytes = csvfile.read()

                    frame = _read_csv_chunk(raw_bytes, polars_schema, date_cols)

                    if csv_columns is None:
                        csv_columns = frame.columns
                    frames.append(frame)

        if not frames:
            print(f'No data found for {focal_table}')
            continue

        out_df = pl.concat(frames, rechunk=True)
        out_df.write_parquet(output_filename, compression='zstd')
        del out_df
        frames.clear()

        # ── Row count validation (hard fail) ──────────────────────────────────
        actual_count = pl.scan_parquet(output_filename).select(pl.len()).collect().item()
        expected_count = counts_df.filter(pl.col('short_name') == focal_table)['count'][0]
        if actual_count != expected_count:
            raise RuntimeError(
                f"Row count mismatch for {focal_table}: "
                f"expected {expected_count:,}, got {actual_count:,}. "
                f"Aborting. Output file (kept for inspection): {output_filename}. "
                f"Already processed: {processed_tables}"
            )
        print(f'Row count for {focal_table}: OK ({actual_count:,})')

        # ── Column validation (hard fail) ─────────────────────────────────────
        pq_cols = pl.scan_parquet(output_filename).collect_schema().names()
        if sorted(csv_columns) != sorted(pq_cols):
            missing = set(csv_columns) - set(pq_cols)
            extra = set(pq_cols) - set(csv_columns)
            raise RuntimeError(
                f"Schema mismatch for {focal_table}. "
                f"Missing columns: {missing or '∅'}; extra columns: {extra or '∅'}. "
                f"Aborting. Output file (kept for inspection): {output_filename}. "
                f"Already processed: {processed_tables}"
            )
        print(f'Schema for {focal_table}: OK\n')

        processed_tables.append(focal_table)

    print(processed_tables)


def main():
    p = argparse.ArgumentParser(
        description="Convert PATSTAT zip files to Parquet"
    )
    p.add_argument("--data-folder", required=True,
                   help="Folder containing the downloaded PATSTAT zip files")
    p.add_argument("--output-folder", required=True,
                   help="Folder where Parquet files will be written")
    p.add_argument("--tables", nargs="+", default=None,
                   help="Tables to process (e.g. tls201 tls206). Default: all")
    p.add_argument("--ram", type=float, default=8.0,
                   help="Available RAM in GB (informational only, default: 8)")
    p.add_argument("--doc-zip", default=None,
                   help="Filename of the documentation zip (e.g. DOC_SQL.zip). "
                        "Auto-detected if not provided.")
    p.add_argument("--sql-subfolder", default=None,
                   help="Inner subfolder of the documentation zip containing the "
                        "CREATE TABLE scripts (e.g. 'CreateTableScripts' or "
                        "'SQL/CreateTable'). Auto-detected if not provided.")
    p.add_argument("--verbose", action="store_true", default=False)
    args = p.parse_args()

    process_tables(
        data_folder=args.data_folder,
        output_folder=args.output_folder,
        tables_to_process=args.tables,
        verbose=args.verbose,
        total_ram_gb=args.ram,
        doc_zip=args.doc_zip,
        sql_subfolder=args.sql_subfolder,
    )


def _read_csv_chunk(raw_bytes, polars_schema, date_cols):
    """Read CSV bytes into a polars DataFrame with proper types and null handling."""
    # Read everything as strings first to avoid parse errors on edge cases
    frame = pl.read_csv(
        raw_bytes,
        infer_schema=False,
        null_values=['', 'NULL'],
        encoding='utf8',
    )

    cast_exprs = []
    for col in frame.columns:
        dtype = polars_schema.get(col, pl.String)

        if dtype == pl.String:
            stripped = pl.col(col).str.strip_chars()
            cast_exprs.append(
                pl.when(stripped == '').then(None).otherwise(stripped).alias(col)
            )
        elif col in date_cols and dtype == pl.Date:
            cast_exprs.append(
                pl.col(col).str.to_date(format='%Y-%m-%d', strict=False).alias(col)
            )
        elif col in date_cols and dtype == pl.Datetime:
            cast_exprs.append(
                pl.col(col).str.to_datetime(format='%Y-%m-%d', strict=False).alias(col)
            )
        else:
            cast_exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))

    return frame.with_columns(cast_exprs)
