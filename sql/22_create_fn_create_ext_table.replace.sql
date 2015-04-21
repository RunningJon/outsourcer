DROP FUNCTION IF EXISTS os.fn_create_ext_table(text, text[], integer, text);
DROP FUNCTION IF EXISTS os.fn_create_ext_table(integer);

CREATE OR REPLACE FUNCTION os.fn_create_ext_table(p_custom_sql_id integer, p_os_server character varying)
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_create_ext_table';
        v_location int;
        
        v_sql text;
        v_columns text[];
        v_column_datatypes text[];
        v_table_name text;
        v_ext_location text;
        v_count int;
        v_os_server text;
        v_os_port text;
        v_gpfdist_port int;
        
BEGIN
        v_location := 1000;
        SELECT LOWER(table_name) as table_name, columns, column_datatypes, gpfdist_port
        INTO v_table_name, v_columns, v_column_datatypes, v_gpfdist_port
        FROM os.custom_sql
        WHERE id = p_custom_sql_id;

        GET DIAGNOSTICS v_count = ROW_COUNT;
        
        v_location := 1500;
        IF v_count = 0 THEN
                RAISE EXCEPTION 'CustomSQLId: "%" is not valid', p_custom_sql_id;
        END IF;

        v_location := 3000;
        FOR i IN array_lower(v_columns, 1)..array_upper(v_columns,1) LOOP
                IF i = 1 THEN
                        v_sql := 'CREATE EXTERNAL TABLE ' || v_table_name || E' \n' ||
                                '("' || v_columns[i] || '" ' || v_column_datatypes[i];
                ELSE
                        v_sql := v_sql || E', \n' || '"' || v_columns[i] || '" ' || v_column_datatypes[i];
                END IF;

        END LOOP;

        v_location := 3500;
        v_sql := v_sql || E')\n';

        v_location := 4000;        
        v_ext_location := 'LOCATION (''gpfdist://' || p_os_server || ':' || v_gpfdist_port || '/config.properties+' || p_custom_sql_id || '#transform=externaldata'')';

        v_location := 5000;
        v_sql := v_sql || v_ext_location || ' FORMAT ''TEXT'' (delimiter ''|'' null ''null'')';
        RAISE INFO '%', v_sql;
        EXECUTE v_sql;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
