import argparse
import os
import re
import shutil
import tempfile
import zipfile
from zipfile import ZipFile

import polars as pl

from .zip_handling import list_zip_files, zip_tables, list_files_in_zip
from .get_dtypes import (
    read_sql_script,
    parse_sql_script,
    convert_sql_to_polars_dtype,
    _find_doc_zip,
)

_COPY_CHUNK = 64 * 1024 * 1024  # 64 MiB


def _stream_extract(zip_path, member, dest_path):
    """Copy a ZIP member to dest_path in bounded-memory chunks."""
    with ZipFile(zip_path) as z, z.open(member) as src, open(dest_path, 'wb') as dst:
        shutil.copyfileobj(src, dst, length=_COPY_CHUNK)


def _stream_extract_nested(outer_zip, inner_member, csv_member, dest_path, tmpdir):
    """Stream-extract a CSV from an outer.zip → inner.zip → csv chain via temp file."""
    inner_path = os.path.join(tmpdir, '_inner_' + os.path.basename(inner_member))
    try:
        _stream_extract(outer_zip, inner_member, inner_path)
        _stream_extract(inner_path, csv_member, dest_path)
    finally:
        if os.path.exists(inner_path):
            os.unlink(inner_path)


def _build_cast_exprs(columns, polars_schema, date_cols):
    """Build per-column cast expressions for a string-typed scan."""
    cast_exprs = []
    for col in columns:
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
    return cast_exprs


def patstat_tables(directory):
    all_zip_files = list_zip_files(directory)
    zip_files = []
    for file in all_zip_files:
        zip_file_path = os.path.join(directory, file)
        zips = list_files_in_zip(zip_file_path)
        zip_files.extend(zips)
    return sorted(set(x.split('_')[0] for x in zip_files if 'tls' in x))


_ROW_COUNT_LINE = re.compile(
    r'^\s*(tls\d+\w*)\s+count:?\s+(\d+)\s*$', re.IGNORECASE
)


def table_names(directory, doc_zip=None):
    """Read the per-table expected row counts from the RowCount report file
    in the documentation ZIP.

    Tolerant of three formats observed across PATSTAT releases:
        ``tls201_appln Count: 140525582``   (older)
        ``tls201_appln count: 140525582``   (2025, lower-case, surrounded by
        separator lines and ``(1 row affected)`` noise)
        ``tls201_appln count\t146147007``    (2026b, tab-separated, no colon,
        preceded by a ``table_name\trow_count`` header line)
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
                   total_ram_gb=8.0, doc_zip=None, sql_subfolder=None, tmp_dir=None):
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
        tmp_dir (str, optional): Directory where the CSV files are extracted
            before conversion. Needs room for the largest table's uncompressed
            CSVs (about 150 GB: tls231 of PATSTAT Global 2026 Autumn expands
            to 143 GB). Defaults to ``output_folder``; point it at a fast
            local disk when ``output_folder`` is on a network share.
    """
    if tables_to_process is not None:
        valid_tables = patstat_tables(data_folder)
        for name in tables_to_process:
            if name not in valid_tables:
                raise ValueError(f"Invalid table name: {name}")

    print("Starting to process tables...\n")
    os.makedirs(output_folder, exist_ok=True)
    if tmp_dir is None:
        tmp_dir = output_folder
    os.makedirs(tmp_dir, exist_ok=True)

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

        with tempfile.TemporaryDirectory(
            prefix=f'{focal_table}_', dir=tmp_dir
        ) as tmpdir:
            csv_paths = []

            for input_data in relevant_tables:
                zips = [x for x in list_files_in_zip(input_data) if focal_table in x]
                for table in zips:
                    print(f'Processing {table}')
                    base = os.path.basename(table)
                    if table.lower().endswith('.csv'):
                        # New BDDS portal: tls*.zip contains the CSV directly.
                        dest = os.path.join(tmpdir, base)
                        _stream_extract(input_data, table, dest)
                    else:
                        # Old layout: tls*.zip is itself nested inside a
                        # data_PATSTAT*.zip wrapper, and contains an inner
                        # CSV named after the zip.
                        csv_name = table.replace('.zip', '.csv')
                        dest = os.path.join(tmpdir, os.path.basename(csv_name))
                        _stream_extract_nested(
                            input_data, table, csv_name, dest, tmpdir
                        )
                    csv_paths.append(dest)

            if not csv_paths:
                print(f'No data found for {focal_table}')
                continue

            lazy = pl.scan_csv(
                csv_paths,
                infer_schema=False,
                null_values=['', 'NULL'],
                encoding='utf8',
            )
            csv_columns = lazy.collect_schema().names()
            lazy = lazy.with_columns(
                _build_cast_exprs(csv_columns, polars_schema, date_cols)
            )
            lazy.sink_parquet(output_filename, compression='zstd')

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
    p.add_argument("--tmp-dir", default=None,
                   help="Directory for the temporary CSV extraction (about 150 GB for "
                        "the largest table). Default: the output folder.")
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
        tmp_dir=args.tmp_dir,
    )
