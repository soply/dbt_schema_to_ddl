## Requirements
Basic Python 3

## Usage
Run the command 

```python
python generate_ddl.py <inschema.yml> <target_ddl_statements.sql> <target_postgres_schema_name>
```

Currently limited support:

1) not null
2) primary keys
3) uniquness
4) foreign keys with one column