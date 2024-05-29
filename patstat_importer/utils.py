import os
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
import pyarrow as pa

def estimate_memory_usage_per_row(file_obj, sample_size=1000):
    """Estimate the memory usage per row by reading a sample of the CSV.
    
    Args:
        file_obj (file-like object): CSV file object
        sample_size (int): Number of rows to sample for estimation

    Returns:
        float: Estimated memory usage per row in bytes
    """
    sample_df = pd.read_csv(file_obj, nrows=sample_size)
    total_memory = sample_df.memory_usage(deep=True).sum()
    memory_per_row = total_memory / sample_size
    return memory_per_row


def calculate_optimal_chunksize(memory_per_row, total_ram_gb):
    """Calculate the optimal chunk size based on memory usage per row and total RAM.
    
    Args:
        memory_per_row (float): Estimated memory usage per row in bytes
        total_ram_gb (float): Total RAM available in gigabytes

    Returns:
        int: Optimal chunk size in number of rows
    """
    max_ram_bytes = total_ram_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    optimal_chunksize = max_ram_bytes // memory_per_row
    return int(optimal_chunksize)


def pandas_to_pyarrow_schema(data_types):
    """ Define a mapping from pandas data types to pyarrow data types """
    pandas_to_pyarrow = {
        'Int32': pa.int32(),
        'Int64': pa.int64(),
        'Int16': pa.int16(),
        'Int8': pa.int8(),
        'UInt32': pa.uint32(),
        'UInt64': pa.uint64(),
        'UInt16': pa.uint16(),
        'UInt8': pa.uint8(),
        'float': pa.float32(),
        'float32': pa.float32(),
        'float64': pa.float64(),
        'double': pa.float64(),
        'string': pa.string(),
        'object': pa.string(),
        'datetime64': pa.timestamp('ns'),
        'datetime64[ns]': pa.timestamp('ns'),
        'bool': pa.bool_(),
    }
    
    fields = []
    for column, dtype in data_types.items():
        if dtype not in pandas_to_pyarrow:
            raise ValueError(f"Data type '{dtype}' for column '{column}' is not supported.")
        fields.append(pa.field(column, pandas_to_pyarrow[dtype]))
    
    return pa.schema(fields)