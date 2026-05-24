import os
import re
import zipfile
from zipfile import ZipFile
import polars as pl


def _find_doc_zip(data_folder):
    """Locate the PATSTAT documentation ZIP in ``data_folder`` by inspecting contents.

    Returns the filename (not the full path) of the single ZIP in ``data_folder``
    that (a) is not a bulk data ZIP (does not start with ``data_PATSTAT``) and
    (b) contains at least one ``.sql`` entry.

    Raises FileNotFoundError if zero or more than one candidate is found, with a
    message instructing the caller to pass ``doc_zip`` / ``--doc-zip`` explicitly.
    """
    candidates = []
    for name in os.listdir(data_folder):
        if not name.lower().endswith('.zip'):
            continue
        if name.startswith('data_PATSTAT'):
            continue
        path = os.path.join(data_folder, name)
        try:
            with zipfile.ZipFile(path, 'r') as z:
                if any(n.lower().endswith('.sql') for n in z.namelist()):
                    candidates.append(name)
        except zipfile.BadZipFile:
            continue

    if not candidates:
        raise FileNotFoundError(
            "No PATSTAT documentation ZIP (containing .sql scripts) was found in "
            f"{data_folder!r}. Pass --doc-zip to specify its filename."
        )
    if len(candidates) > 1:
        raise FileNotFoundError(
            "Multiple candidate documentation ZIPs found: "
            f"{candidates}. Pass --doc-zip to disambiguate."
        )
    return candidates[0]


def _find_sql_subfolder(zip_obj):
    """Locate the inner subfolder of ``zip_obj`` that holds the CREATE TABLE scripts.

    Looks for entries whose basename matches ``create*.sql`` (case-insensitive)
    and returns the directory prefix they share (e.g. ``'CreateTableScripts/'`` or
    ``'SQL/CreateTable/'``). The returned value always ends with ``'/'`` or is
    the empty string when the .sql files sit at the ZIP root.

    Raises FileNotFoundError if the create-scripts span multiple directories or
    none are found, instructing the caller to pass ``sql_subfolder`` explicitly.
    """
    create_files = [
        n for n in zip_obj.namelist()
        if n.lower().endswith('.sql')
        and os.path.basename(n).lower().startswith('createtls')
    ]
    if not create_files:
        sql_files = [n for n in zip_obj.namelist() if n.lower().endswith('.sql')]
        raise FileNotFoundError(
            "No 'create*.sql' files found in documentation ZIP. "
            f"SQL entries seen: {sql_files[:10]}. "
            "Pass --sql-subfolder to specify the subfolder containing the CREATE TABLE scripts."
        )

    dirs = {os.path.dirname(n) for n in create_files}
    if len(dirs) > 1:
        raise FileNotFoundError(
            "'create*.sql' files span multiple directories: "
            f"{sorted(dirs)}. Pass --sql-subfolder to disambiguate."
        )

    subfolder = dirs.pop()
    return subfolder + '/' if subfolder else ''


def read_sql_script(data_folder, focal_table, doc_zip=None, sql_subfolder=None):
    """Read SQL script of table from a zip file in a given directory.

    Both the documentation ZIP filename and the inner subfolder holding the
    CREATE TABLE scripts vary between PATSTAT releases (e.g.
    ``index_documentation_scripts_*.zip`` with ``CreateTableScripts/`` vs.
    ``DOC_SQL.zip`` with ``SQL/CreateTable/``). When either argument is left
    as None it is auto-detected by inspecting the ZIPs' contents.

    Args:
        data_folder (str): Directory containing the documentation ZIP.
        focal_table (str): Identifier of the table for which the SQL script is needed.
        doc_zip (str, optional): Filename of the documentation ZIP. If None,
            auto-detected as the single non-data ZIP in ``data_folder`` that
            contains ``.sql`` entries.
        sql_subfolder (str, optional): Inner subfolder containing the CREATE TABLE
            scripts. If None, auto-detected by finding the directory holding the
            ``create*.sql`` files.

    Returns:
        str: Contents of the SQL script file.

    Raises:
        FileNotFoundError: If the documentation ZIP, the SQL subfolder, or the
            script for ``focal_table`` cannot be located.
    """
    if doc_zip is None:
        doc_zip = _find_doc_zip(data_folder)

    file_path = os.path.join(data_folder, doc_zip)

    with zipfile.ZipFile(file_path, 'r') as zip:
        if sql_subfolder is None:
            target_subfolder = _find_sql_subfolder(zip)
        else:
            target_subfolder = sql_subfolder.rstrip('/') + '/' if sql_subfolder else ''

        all_files = zip.namelist()
        sql_files = [
            f for f in all_files
            if (target_subfolder == '' or f.startswith(target_subfolder))
            and f.lower().endswith('.sql')
        ]
        relevant_files = [f for f in sql_files if focal_table.lower() in f.lower().split('/')[-1]]

        if not relevant_files:
            raise FileNotFoundError(f"No SQL script found containing '{focal_table}' in the zip file.")

        with zip.open(relevant_files[0], 'r') as file:
            return file.read().decode('utf-8')


def parse_sql_script(script_content):
    pattern = re.compile(
        r'\[(\w+)\]\s+\[(\w+)\](?:\((\d+|max)\))?\s+NOT NULL(?:\s+DEFAULT\s+\(?\'?([^\)\'\s]*)\'?\)?)?',
        re.IGNORECASE
    )

    variables = {}
    for match in pattern.findall(script_content):
        varname, vartype, length, default_value = match
        default_value = default_value.strip('\'').strip('(') if default_value else None
        variables[varname.lower()] = {
            'type': vartype.lower(),
            'length': length if length != 'max' else 'max',
            'default': default_value,
        }
    return variables


_SQL_TO_POLARS = {
    'int':        pl.Int64,
    'smallint':   pl.Int16,
    'tinyint':    pl.Int8,
    'mediumint':  pl.Int32,
    'bigint':     pl.Int64,
    'float':      pl.Float32,
    'double':     pl.Float64,
    'decimal':    pl.Float64,
    'numeric':    pl.Float64,
    'char':       pl.String,
    'varchar':    pl.String,
    'nvarchar':   pl.String,
    'text':       pl.String,
    'tinytext':   pl.String,
    'mediumtext': pl.String,
    'longtext':   pl.String,
    'blob':       pl.String,
    'tinyblob':   pl.String,
    'mediumblob': pl.String,
    'longblob':   pl.String,
    'date':       pl.Date,
    'datetime':   pl.Datetime,
    'timestamp':  pl.Datetime,
    'time':       pl.String,
    'year':       pl.Int16,
    'bit':        pl.Boolean,
    'bool':       pl.Boolean,
    'boolean':    pl.Boolean,
    'enum':       pl.String,
    'set':        pl.String,
}


def convert_sql_to_polars_dtype(sql_dict):
    """Map a parsed SQL column dict to polars data types.

    Args:
        sql_dict (dict): Output of parse_sql_script().

    Returns:
        dict[str, polars.DataType]: Column name → polars dtype.
    """
    return {col: _SQL_TO_POLARS.get(attrs['type'], pl.String)
            for col, attrs in sql_dict.items()}
