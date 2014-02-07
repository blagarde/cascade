cascade
=======

Delete from PostgreSQL as if all foreign keys had "on delete cascade".

## Dependencies
- [networkx](http://networkx.github.io/documentation/latest/)
- [psycopg2](https://pypi.python.org/pypi/psycopg2)

A relational database is just a directed graph where tables (nodes) are linked together by foreign keys (edges).

This script leverages that fact to recursively find (and delete) any entries the one you wish to delete depends on.

It is a useful tool to undo accidental transactions with many nested inserts.

### 1. Edit config.py
- Some database schemas have FKEY relations which cause the graph to be cyclic. List the names of these relations in `TROUBLE`. A list of all FKEY relations can be obtained by running the query inside `fkey.sql`.
- `UNLOADABLES` contains the list of `<tablename>` tables the entries of which are allowed to be deleted.

### 2. Run the script
```
python cascase.py --mode <tablename> <uid>
```
Note: The script should work but is still experimental, therefore please use with caution.
It has a `--dryrun` option that prints out all the SQL statements instead of executing them.

Use it. :)
