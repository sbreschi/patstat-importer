
import numpy as np
import os
import re
import zipfile
from zipfile import ZipFile


def read_sql_script(data_folder, focal_table):
	"""Read SQL script of table from a zip file in a given directory.

	Args:
		data_folder (str): Path to the directory containing the zip file.
		focal_table (str): Identifier of the table for which the SQL script is needed.

	Returns:
		str: Contents of the SQL script file.

	Raises:
		FileNotFoundError: If no appropriate zip file or SQL script is found.
	"""
	# Find all zip files that start with 'index_documentation_scripts'
	docs = [file for file in os.listdir(data_folder) if
			file.startswith('index_documentation_scripts') and file.endswith('.zip')]
	if not docs:
		raise FileNotFoundError("No zip file found starting with 'index_documentation_scripts'.")

	file_path = os.path.join(data_folder, docs[0])  # Use the first found zip file
	target_subfolder = 'CreateTableScripts'

	with zipfile.ZipFile(file_path, 'r') as zip:
		# List all files in the zip
		all_files = zip.namelist()

		# Filter to find only the files within the "CreateScripts" subfolder and end with .sql
		sql_files = [file for file in all_files if target_subfolder in file and file.endswith('.sql')]

		# Filter to find files that contain the focal_table in their name
		relevant_files = [file for file in sql_files if focal_table.lower() in file.lower().split('/')[-1]]

		if not relevant_files:
			raise FileNotFoundError(f"No SQL script found containing '{focal_table}' in the zip file.")

		# Open and read the first relevant SQL file
		with zip.open(relevant_files[0], 'r') as file:
			return file.read().decode('utf-8')


def parse_sql_script(script_content):
	pattern = re.compile(
        r'\[(\w+)\]\s+\[(\w+)\](?:\((\d+|max)\))?\s+NOT NULL(?:\s+DEFAULT\s+\(?\'?([^\)\'\s]*)\'?\)?)?', re.IGNORECASE
    )

	variables = {}
	matches = pattern.findall(script_content)

	for match in matches:
		varname, vartype, length, default_value = match
		# Clean up default value and handle specific cases
		default_value = default_value.strip('\'').strip('(') if default_value else None
		variables[varname.lower()] = {
            'type': vartype.lower(),
            'length': length if length != 'max' else 'max',
            'default': default_value
            }

	return variables


def convert_sql_to_pandas_dtype(sql_dict):
	sql_to_pandas_dtype = {
	    'int': 'Int64',
	    'smallint': 'Int16',
	    'tinyint': 'Int8',
	    'mediumint': 'Int32',
	    'bigint': 'Int64',
	    'float': 'float32',
	    'double': 'float64',
	    'decimal': 'float64',
	    'numeric': 'float64',
	    'char': 'string',
	    'varchar': 'string',
	    'nvarchar': 'string',
	    'text': 'string',
	    'tinytext': 'string',
	    'mediumtext': 'string',
	    'longtext': 'string',
	    'blob': 'object',
	    'tinyblob': 'object',
	    'mediumblob': 'object',
	    'longblob': 'object',
	    'date': 'datetime64[ns]',
	    'datetime': 'datetime64[ns]',
	    'timestamp': 'datetime64[ns]',
	    'time': 'object',
	    'year': 'Int16',
	    'bit': 'bool',
	    'bool': 'bool',
	    'boolean': 'bool',
	    'enum': 'string',
	    'set': 'object'
	}
	
	pandas_dtype_dict = {}

	for varname, attributes in sql_dict.items():
		sql_type = attributes['type']
		pandas_type = sql_to_pandas_dtype.get(sql_type, 'object')
		pandas_dtype_dict[varname] = pandas_type

	return pandas_dtype_dict