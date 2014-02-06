SELECT t.table_name,
             kcu.constraint_name,
             kcu.column_name,
             kcu.ordinal_position
    FROM    INFORMATION_SCHEMA.TABLES t
             LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                     ON tc.table_catalog = t.table_catalog
                     AND tc.table_schema = t.table_schema
                     AND tc.table_name = t.table_name
                     AND tc.constraint_type = 'PRIMARY KEY'
             LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                     ON kcu.table_catalog = tc.table_catalog
                     AND kcu.table_schema = tc.table_schema
                     AND kcu.table_name = tc.table_name
                     AND kcu.constraint_name = tc.constraint_name
    WHERE   t.table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY t.table_catalog,
             t.table_schema,
             t.table_name,
             kcu.constraint_name,
             kcu.ordinal_position