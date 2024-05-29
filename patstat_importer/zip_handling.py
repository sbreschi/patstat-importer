import os
from zipfile import ZipFile, BadZipFile

def list_zip_files(directory):
    """
    List all ZIP files in the specified directory.
    
    Args:
    directory (str): Path to the directory to search for ZIP files.

    Returns:
    list: A list of filenames of the ZIP files found in the directory.
    """
    if not os.path.exists(directory):
        print(f"The directory {directory} does not exist.")
        return []

    return [f for f in os.listdir(directory) if f.endswith('.zip') and f.startswith('data_PATSTAT')]


def list_files_in_zip(zip_filepath):
    """
    Extract and return a list of all file names contained within a zip file.

    Parameters:
    - zip_filepath (str): The file path to the zip file.

    Returns:
    - list of str: A list containing the names of all the files within the zip file.
      Returns an empty list if the zip file is invalid or the file path does not exist.

    Raises:
    - Prints an error message if the file is not a valid zip file or if the file cannot be found.

    Example:
    - list_files_in_zip('example.zip') -> ['file1.txt', 'file2.txt', ...]

    Notes:
    - The function reads the zip file using 'ZipFile' from the zipfile module.
    - It handles two common errors: 'BadZipFile' when the file is not a valid zip and 'FileNotFoundError'
      when the file path does not exist. In both cases, an error message is printed and an empty list is returned.
    """
    try:
        with ZipFile(zip_filepath, 'r') as zip_ref:
            return [filename for filename in zip_ref.namelist()]
    except BadZipFile:
        print(f"Error: The file {zip_filepath} is not a valid ZIP file.")
    except FileNotFoundError:
        print(f"Error: The file {zip_filepath} was not found.")
    return []


def zip_tables(focal_table, zip_files):
    """
    Filter, process, and return a sorted list of unique table names from zip files that contain a specific table name.

    Parameters:
    - focal_table (str): The name of the table to look for within zip file names.
    - zip_files (list of str): List of zip file names which may include directory paths.

    Returns:
    - list of str: A sorted list of unique table names, modified by splitting at 'tls' and removing any path.

    The function:
    1. Filters zip file names to include only those that contain the 'focal_table'.
    2. Strips the part of the filename after 'tls' to focus on the relevant table name.
    3. Removes directory path to isolate the table name.
    4. Returns a sorted list of unique names to prevent duplicates and provide ordered results.
    """
    zip_tables = [x for x in zip_files if focal_table in x]
    zip_tables = [x.split('zip')[0] + 'zip' for x in zip_tables]
    zip_tables = sorted(list(set(zip_tables)))
    return zip_tables