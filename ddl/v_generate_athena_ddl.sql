--DROP VIEW v_generate_athena_ddl;
/**********************************************************************************************
Purpose: View to get the Amazon Athena DDL for a table.
         This will contain the S3 LOCATION, TBLPROPERTIES(for AWS Glue), etc.
Notes:   
         The following filters are useful:
           where tablename in ('t1', 't2')     -- only get DDL for specific tables
           where schemaname in ('s1', 's2')    -- only get DDL for specific schemas
         So for example if you want to order DDL on tablename and only want the tables 't1', 't2'
         and 't4' you can do so by using a query like:
           select ddl from (
           ) where tablename in ('t1', 't2', 't4');
History:
2018-01-26 Satoru Ishikawa Created
**********************************************************************************************/
CREATE OR REPLACE VIEW v_generate_athena_ddl
AS
SELECT 
  REGEXP_REPLACE (schemaname, '^zzzzzzzz', '') AS schemaname, 
  REGEXP_REPLACE (tablename, '^zzzzzzzz', '') AS tablename, 
  seq, 
  ddl 
FROM 
  (
    SELECT 
      schemaname, 
      tablename, 
      seq, 
      ddl 
    FROM 
      (
        --DROP TABLE
        SELECT 
          n.nspname AS schemaname, 
          c.relname AS tablename, 
          0 AS seq, 
          '-- DROP TABLE ' + QUOTE_IDENT(n.nspname || '_db') + '.' + QUOTE_IDENT(c.relname) + ';' AS ddl 
        FROM 
          pg_namespace AS n 
          INNER JOIN pg_class AS c ON n.oid = c.relnamespace 
        WHERE 
          c.relkind = 'r' 
        --CREATE TABLE
        UNION 
        SELECT 
          n.nspname AS schemaname, 
          c.relname AS tablename, 
          2 AS seq, 
          'CREATE EXTERNAL TABLE ' + QUOTE_IDENT(n.nspname || '_db') + '.' + QUOTE_IDENT(c.relname) + '' AS ddl 
        FROM 
          pg_namespace AS n 
          INNER JOIN pg_class AS c ON n.oid = c.relnamespace 
        WHERE 
          c.relkind = 'r' 
        --OPEN PAREN COLUMN LIST
        UNION 
        SELECT 
          n.nspname AS schemaname, 
          c.relname AS tablename, 
          5 AS seq, 
          '(' AS ddl 
        FROM 
          pg_namespace AS n 
          INNER JOIN pg_class AS c ON n.oid = c.relnamespace 
        WHERE 
          c.relkind = 'r' 
        --COLUMN LIST
        UNION 
        SELECT 
          schemaname, 
          tablename, 
          seq, 
          '\t' + col_delim + col_name + ' ' + col_datatype AS ddl 
        FROM 
          (
            SELECT 
              n.nspname AS schemaname, 
              c.relname AS tablename, 
              100000000 + a.attnum AS seq, 
              CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim, 
              QUOTE_IDENT(a.attname) AS col_name, 
              CASE 
                WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'CHARACTER VARYING'
                ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'CHARACTER VARYING', 
                'VARCHAR'
              ) WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'CHARACTER'
              ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'CHARACTER', 
                'CHAR'
              ) WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'INTEGER'
              ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'INTEGER', 
                'INT'
              ) WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'NUMERIC'
              ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'NUMERIC', 
                'DECIMAL'
              ) WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'DOUBLE'
              ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'DOUBLE PRECISION', 
                'DOUBLE'
              ) WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'REAL'
              ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'REAL', 
                'FLOAT'
              ) WHEN STRPOS(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'TIMESTAMP'
              ) > 0 THEN REPLACE(
                UPPER(
                  format_type(a.atttypid, a.atttypmod)
                ), 
                'TIMESTAMP WITHOUT TIME ZONE', 
                'TIMESTAMP'
              ) ELSE UPPER(
                format_type(a.atttypid, a.atttypmod)
              ) END AS col_datatype
            FROM 
              pg_namespace AS n 
              INNER JOIN pg_class AS c ON n.oid = c.relnamespace 
              INNER JOIN pg_attribute AS a ON c.oid = a.attrelid 
              LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid 
              AND a.attnum = adef.adnum 
            WHERE 
              c.relkind = 'r' 
              AND a.attnum > 0 
            ORDER BY 
              a.attnum
          ) 
        --CLOSE PAREN COLUMN LIST
        UNION 
        SELECT 
          n.nspname AS schemaname, 
          c.relname AS tablename, 
          299999999 AS seq, 
          ')' AS ddl 
        FROM 
          pg_namespace AS n 
          INNER JOIN pg_class AS c ON n.oid = c.relnamespace 
        WHERE 
          c.relkind = 'r' 
        --END SEMICOLON
        UNION 
        SELECT 
          n.nspname AS schemaname, 
          c.relname AS tablename, 
          600000000 AS seq, 
          '-- PARTITIONED BY (col_name data_type [, … ])\n' ||
          'ROW FORMAT DELIMITED\n' ||
          '  FIELDS TERMINATED BY ''\\t''\n' ||
          'STORED AS INPUTFORMAT\n' ||
          '  ''org.apache.hadoop.mapred.TextInputFormat''\n' ||
          'OUTPUTFORMAT\n' ||
          '  ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''\n' ||
          'LOCATION\n' ||
          '  ''s3://bucket/folder/''\n' ||
          'TBLPROPERTIES (\n' ||
          '--  ''CrawlerSchemaDeserializerVersion''=''1.0'',\n' ||
          '--  ''CrawlerSchemaSerializerVersion''=''1.0'',\n' ||
          '--  ''UPDATED_BY_CRAWLER''=''my_crawler'',\n' ||
          '--  ''averageRecordSize''=''100'',\n' ||
          '  ''classification''=''csv'',\n' ||
          '--  ''columnsOrdered''=''true'',\n' ||
          '--  ''compressionType''=''gzip'',\n' ||
          '  ''delimiter''=''\\t'',\n' ||
          '--  ''objectCount''=''1000'',\n' ||
          '--  ''recordCount''=''10000000000'',\n' ||
          '--  ''sizeKey''=''1000000000000'',\n' ||
          '--   ''skip.header.line.count''=''1'',\n' ||
          '  ''typeOfData''=''file'')\n' ||
          ';' AS ddl 
        FROM 
          pg_namespace AS n 
          INNER JOIN pg_class AS c ON n.oid = c.relnamespace 
        WHERE 
          c.relkind = 'r'
      ) 
    WHERE 1=1
--       AND schemaname in ('s1', 's2') -- listed Traget schemas. 
--       AND tablename  in ('t1', 't2') -- listed Traget tables. 
    ORDER BY 
      schemaname, 
      tablename, 
      seq
  );
