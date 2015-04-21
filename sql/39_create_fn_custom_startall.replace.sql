CREATE OR REPLACE FUNCTION os.fn_custom_startall(p_os_server text, p_os_port int)
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_custom_startall';
        v_location int;
        v_rec record;
	v_gpfdist_port int;
	v_sql text;
BEGIN
        v_location := 1000;
        FOR v_rec IN SELECT id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, gpfdist_port FROM os.custom_sql LOOP

                v_location := 2000;
                SELECT gpfdist_port
                INTO v_gpfdist_port
                FROM os.customstart;
                
                v_location := 2100;
                INSERT INTO os.ao_custom_sql 
                (id, table_name, columns, column_datatypes, sql_text, source_type, 
                source_server_name, source_instance_name, source_port, source_database_name, 
                source_user_name, source_pass, gpfdist_port)
                VALUES
                (v_rec.id, v_rec.table_name, v_rec.columns, v_rec.column_datatypes, v_rec.sql_text, v_rec.source_type, 
                v_rec.source_server_name, v_rec.source_instance_name, v_rec.source_port, v_rec.source_database_name, 
                v_rec.source_user_name, v_rec.source_pass, v_gpfdist_port);

                v_location := 2300;
                v_rec.table_name := LOWER(v_rec.table_name);
                v_sql := 'DROP EXTERNAL TABLE IF EXISTS ' || v_rec.table_name;
                EXECUTE v_sql;
                
                v_location := 2400;
                PERFORM os.fn_create_ext_table(v_rec.id, p_os_server);

        END LOOP;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
