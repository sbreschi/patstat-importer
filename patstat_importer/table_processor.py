import os
import re
import pandas as pd
import numpy as np
import io
from io import BytesIO
import zipfile
from zipfile import ZipFile
import gc
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetDataset

from .zip_handling import list_zip_files, zip_tables, list_files_in_zip
from .utils import estimate_memory_usage_per_row, calculate_optimal_chunksize, pandas_to_pyarrow_schema
from .get_dtypes import read_sql_script, parse_sql_script, convert_sql_to_pandas_dtype

def patstat_tables(directory):
    all_zip_files = list_zip_files(directory)
    zip_files = []
    for file in all_zip_files:
        zip_file_path = os.path.join(directory, file)
        zips = list_files_in_zip(zip_file_path)
        zip_files.extend(zips)
    zip_files = sorted(set([x.split('_')[0] for x in zip_files if 'tls' in x]))
    return zip_files


def table_names(directory):
    filename = [f for f in os.listdir(directory) if f.endswith('.zip') and f.startswith('index_documentation_scripts')][0]
    filenames = list_files_in_zip(os.path.join(directory, filename))
    file = [x for x in filenames if 'RowCount' in x and (x.endswith('.txt') or x.endswith('.rpt'))][0]

    # Open the ZIP file
    zip_file_path = os.path.join(directory, filename)
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        # Check if the CSV file exists in the ZIP
        if file in z.namelist():
            # Read the CSV file into a pandas DataFrame
            with z.open(file) as f:
                # Use io.TextIOWrapper for encoding compatibility
                data = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'), header=None)
                return data
        else:
            print("CSV file not found in the ZIP archive.")


def process_tables(data_folder, output_folder, tables_to_process=None, verbose=False, total_ram_gb=8.0):
    """
    Process specified PATSTAT tables or all available tables found in the specified data folder,
    and save them to the specified output folder.

    Args:
        data_folder (str): Directory containing the ZIP files with data.
        output_folder (str): Directory where the Parquet files will be saved.
        tables_to_process (list, optional): List of table names to process. If None, processes all tables.
        verbose (bool, optional): If True, print detailed logs of the processing steps.
        total_ram_gb (float, optional): Total RAM available for processing in gigabytes. Defaults to 8.0 GB.
    """

    if tables_to_process is not None:
        valid_tables = patstat_tables(data_folder)
        for name in tables_to_process:
            if name not in valid_tables:
                raise ValueError(f"Invalid input table name: {name}")

    print("Starting to process tables...", "\n")

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    all_zip_files = list_zip_files(data_folder)

    if verbose:
        print('All zipped data files in Patstat directory:\n')
        for x in all_zip_files:
            print(x)
    print('\n')

    zip_files = []
    for file in all_zip_files:
        zip_file_path = os.path.join(data_folder, file)
        zips = list_files_in_zip(zip_file_path)
        zips = [os.path.join(data_folder, file, z) for z in zips]
        zip_files.extend(zips)

    if tables_to_process is None:
        unique_tables = set([filename.split('tls')[1] for filename in zip_files if 'tls' in filename])
        tables_to_process = sorted(set([f'tls{table.split("_")[0]}' for table in unique_tables]))
    else:
        tables_to_process = sorted(set(tables_to_process))

    if verbose:
        print('Tables to process:\n')
        for x in tables_to_process:
            print(x)
    print('\n')

    df = table_names(data_folder)
    df['table_name'] = df[0].str.split(' Count: ').str[0]
    df['count'] = df[0].str.split(' Count: ').str[1]
    df['short_name'] = df['table_name'].str.split('_').str[0]
    df['count'] = df['count'].astype(int)
    df = df[['short_name', 'table_name', 'count']]

    processed_tables = []

    for table_prefix in tables_to_process:
        focal_table = table_prefix
        relevant_tables = zip_tables(focal_table, zip_files)

        relevant_script = read_sql_script(data_folder, focal_table)
        columns_info = parse_sql_script(relevant_script)

        if focal_table == 'tls201':
            data_types = convert_sql_to_pandas_dtype(columns_info)
            data_types['appln_nr_epodoc'] = 'string'
            schema = pandas_to_pyarrow_schema(data_types)        
        else:
            data_types = convert_sql_to_pandas_dtype(columns_info)
            schema = pandas_to_pyarrow_schema(data_types)

        date_columns = [col for col, dtype in data_types.items() if dtype == 'datetime64[ns]']
        for key in date_columns:
            if key in data_types:
                data_types[key] = 'string'

        full_table_name = df[df['short_name'] == focal_table]['table_name'].iloc[0]
        output_filename = os.path.join(output_folder, f"{full_table_name}.parquet")

        pqwriter = None

        for input_data in relevant_tables:
            zips = list_files_in_zip(input_data)
            zips = [x for x in zips if focal_table in x]
            with ZipFile(input_data) as z:
                for table in zips:
                    with z.open(table) as z2:
                        print('Processing table {}'.format(table))
                        table = table.replace('.zip', '.csv')
                        z2_filedata = BytesIO(z2.read())
                        with ZipFile(z2_filedata) as nested_zip:
                            with nested_zip.open(table) as csvfile:
                                # Read the first chunk to get the column names
                                first_chunk = pd.read_csv(csvfile, dtype=data_types, nrows=1, encoding='utf-8',
                                                          low_memory=False)
                                csv_columns = first_chunk.columns.tolist()

                                # Reset the file pointer to the beginning
                                csvfile.seek(0)

                                memory_per_row = estimate_memory_usage_per_row(csvfile)
                                chunk_size = calculate_optimal_chunksize(memory_per_row, total_ram_gb)
                                csvfile.seek(0)

                                for chunk in pd.read_csv(csvfile, dtype=data_types, chunksize=chunk_size,
                                                         encoding='utf-8', low_memory=False):
                                    for col, col_info in columns_info.items():
                                        if pd.api.types.is_string_dtype(chunk[col]) or pd.api.types.is_object_dtype(
                                                chunk[col]):
                                            chunk[col] = chunk[col].str.replace('^\s+$', '', regex=True).replace('',
                                                                                                                 np.nan)

                                    for key in date_columns:
                                        if key in chunk.columns:
                                            chunk[key] = pd.to_datetime(chunk[key], format='%Y-%m-%d', errors='coerce')

                                    table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)

                                    if pqwriter is None:
                                        pqwriter = pq.ParquetWriter(output_filename, schema=schema)

                                    pqwriter.write_table(table)
                                    del chunk
                                    gc.collect()

        if pqwriter:
            pqwriter.close()

        try:
            dataset = pq.ParquetDataset(output_filename)
            actual_count = sum(p.count_rows() for p in dataset.fragments)
            check_count = df[df['short_name'] == focal_table]['count'].iloc[0]
            if actual_count == check_count:
                print(f'Number of records of table {focal_table} is correct')
            else:
                raise ValueError(f"Expected {check_count} records, but found {actual_count} records.")
            del dataset
            gc.collect()
        except ValueError as e:
            print(f"Error: {e}")

        # Check the schema of the written Parquet file
        try:
            parquet_file = pq.ParquetFile(output_filename)
            schema = parquet_file.schema
            pq_columns = schema.names
            if sorted(csv_columns) == sorted(pq_columns):
                print(f"Schema of table {focal_table} is correct\n")
            else:
                diff_cols = set(csv_columns).difference(set(pq_columns))
                print(f"Schema mismatch for table {focal_table}")
                print(f"Missing column(s): {diff_cols}")
                print("Check documentation. Column(s) may be deprecated from Patstat\n")
        except ValueError as e:
            print(f"Error: {e}")
        
        processed_tables.append(focal_table)

    print(processed_tables)