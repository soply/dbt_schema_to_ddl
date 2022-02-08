import yaml
import argparse

def get_ddl_primary_key(schema_name, table_data):
    """ Extracts primary key and returns the DDL messages as a string.
    If no primary key can be found, returns None

    Parameters
    ----------
    schema_name : str
        Name of the Postgres schema to create the primary key in
        
    table_data : dict
        .yml dbt extract for a particular table

    Returns
    -------
    str or None
        DDL statement for creating the primary key, None if no primary key can be found
    """
    table_name = table_data.get("name", None)
    primary_key_str = table_data.get('primary_key', None)
    if table_name and primary_key_str:
        return f"ALTER TABLE { schema_name }.{ table_name } ADD PRIMARY KEY ({ primary_key_str });"
    else:
        return None


def get_ddl_non_null(schema_name, table_data):
    """ Extracts primary key and returns the DDL messages as a string.
    If no primary key can be found, returns None.

    Parameters
    ----------
    schema_name : str
        Name of the Postgres schema to create the  primary key in 

    table_data : dict
        .yml dbt extract for a particular table

    Returns
    -------
    List of strs
        List of ddl non null constraints to add for this table
    """
    ddl_statements = []
    table_name = table_data.get("name", None)
    if table_name:
        for column in table_data.get('columns', []):
            if 'not_null' in column.get('tests', ''):
                ddl_statements.append(f"ALTER TABLE { schema_name }.{ table_name } ALTER COLUMN { column['name'] } SET NOT NULL;")
        return ddl_statements
    else:
        return []

def get_ddl_unique(schema_name, table_data):
    """ Extracts primary key and returns the DDL messages as a string.
    If no primary key can be found, returns None.

    Parameters
    ----------
    schema_name : str
        Name of the Postgres schema to create the  primary key in 

    table_data : dict
        .yml dbt extract for a particular table

    Returns
    -------
    List of strs
        List of ddl unique constraints to add for this table
    """
    ddl_statements = []
    table_name = table_data.get("name", None)
    if table_name:
        for column in table_data.get('columns', []):
            if 'unique' in column.get('tests', ''):
                ddl_statements.append(f"ALTER TABLE { schema_name }.{ table_name } ADD CONSTRAINT unique_{ schema_name }_{ table_name }_{ column['name'] } UNIQUE ({ column['name'] });")
        return ddl_statements
    else:
        return []

def get_ddl_foreign_keys(schema_name, table_data):
    """ Extracts primary key and returns the DDL messages as a string.
    If no primary key can be found, returns None.

    Parameters
    ----------
    schema_name : str
        Name of the Postgres schema to create the  primary key in 

    table_data : dict
        .yml dbt extract for a particular table

    Returns
    -------
    List of strs
        List of ddl foreign key constraints to add for this table
    """
    ddl_statements = []
    table_name = table_data.get("name", None)
    if table_name:
        # Loop through columns of table
        for column in table_data.get('columns', []):
            # Loop through tests associated with a column
            for tests in column.get('tests', []):
                # Check if test is related to a foreign key constraint
                if type(tests) is dict and 'relationships' in tests.keys():
                    destination_table = tests['relationships']['to'].split("ref('")[1].split("')")[0]
                    destination_column = tests['relationships']['field']
                    ddl_statement = (f"ALTER TABLE { schema_name }.{ table_name } ADD CONSTRAINT "
                                     f"fk_{ schema_name }_{ table_name }_{ column['name'] }_{ destination_table }_{ destination_column } "
                                     f"FOREIGN KEY ({ column['name'] }) REFERENCES { schema_name }.{ destination_table } ({ destination_column });")
                    ddl_statements.append(ddl_statement)
        return ddl_statements
    else:
        return []


def process_schema(in_file, schema_name):
    """ Calls other functions to create
    the ddl statements from the given dbt schema file.

    Parameters
    ----------
    in_file : str
        Path to input file (dbt schema file)
    schema_name : str
        Target schema name

    Returns
    -------
    dict
        Dictionary with DDL statements per table and type. Types 
        are split in 'primary_key', 'not_null', 'uniquness', and 'foreign_keys'

    Raises
    ------
    RuntimeError
        If the given dbt schema file does not match our requirement.
    """
    ddl_statements = {}
    with open(in_file, 'r') as file:
        data = yaml.safe_load(file)
    if 'models' not in data.keys():
        raise RuntimeError("'models' not contained in first level keys of the given yml file")
    for table in data["models"]:
        if "name" not in table.keys():
            raise RuntimeError(f"table { table['name'] } does not have a name key in the given yml file")
        ddl_statements[table['name']] = {}
        # Add potential primary key DDL
        primary_key_ddl = get_ddl_primary_key(schema_name, table)
        if primary_key_ddl:
            ddl_statements[table['name']]["primary_key"] = primary_key_ddl
        # Add non null constraints
        ddl_statements[table['name']]["non_null"] = get_ddl_non_null(schema_name, table)
        # Add uniquness constraints
        ddl_statements[table['name']]["uniqueness"] = get_ddl_unique(schema_name, table)
        # Add foreign key constraints
        ddl_statements[table['name']]["foreign_keys"] = get_ddl_foreign_keys(schema_name, table)
    return ddl_statements


def write_ddl_to_file(in_file, out_file, schema_name):
    """ Writes DDL statements from given in_file (dbt schema file)
    to the specified out file. Uses given schema_name for all DDL statements

    Parameters
    ----------
    in_file : str
        Path to input file (dbt schema file)
    out_file : str
        Path to output file (should be some *.sql file)
    schema_name : str
        Target schema name
    """
    ddl_statements = process_schema(in_file, schema_name)
    ddl_text = ""
    ddl_text += f"-- Adding primary_key, non_null, uniqueness ddl statements \n\n"
    for key, val in ddl_statements.items():
        ddl_text += f"-- Processing table {key}\n\n"
        if val['primary_key']:
            ddl_text += val['primary_key'] + '\n'
        for ddl_statement in val['non_null']:
             ddl_text += ddl_statement + '\n'
        for ddl_statement in val['uniqueness']:
             ddl_text += ddl_statement + '\n'
        ddl_text += '\n\n'
    ddl_text += f"-- Adding foreign key ddl statements \n\n"
    for key, val in ddl_statements.items():
        if len(val['foreign_keys']) == 0:
            continue
        ddl_text += f"-- Processing table {key}\n\n"
        for ddl_statement in val['foreign_keys']:
             ddl_text += ddl_statement + '\n'
        ddl_text += '\n\n'
    
    with open(out_file, 'w') as fout:
        fout.write(ddl_text)

parser = argparse.ArgumentParser(description='Simple conversion from dbt schema file to actual ddl statements. Only considers limited set of ddls.')
parser.add_argument('in_file', type=str,
                    help='Path to input file (dbt schema file)')
parser.add_argument('out_file', type=str,
                    help='Path to output file (dbt schema file)')
parser.add_argument('target_schema', type=str,
                    help='Target schema against which to run DDL statements')
args = parser.parse_args()

write_ddl_to_file(args.in_file, args.out_file, args.target_schema)


