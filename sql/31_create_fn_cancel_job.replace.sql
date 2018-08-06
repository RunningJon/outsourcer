CREATE OR REPLACE FUNCTION os.fn_cancel_job(p_id integer)
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_cancel_job';
        v_location int;
        v_procpid int;
        v_found boolean := FALSE;
BEGIN
	SET OPTIMIZER=OFF;
        v_location := 1000;
        --check for External Tables loading data
        WITH q as       (SELECT 'INSERT INTO "' || target_schema_name || '"."' || target_table_name || '"' AS current_query
                        FROM os.queue 
                        WHERE status = 'processing' 
                        AND id = p_id)
        SELECT p.procpid
        INTO v_procpid
        FROM pg_stat_activity p, q
        WHERE p.datname = current_database()
        AND p.current_query LIKE q.current_query || '%';
        
        WHILE v_procpid IS NOT NULL LOOP
                v_found := TRUE;
                
                PERFORM pg_cancel_backend(v_procpid);
                
                PERFORM pg_sleep(1);

                WITH q as       (SELECT 'INSERT INTO "' || target_schema_name || '"."' || target_table_name || '"' AS current_query
                                FROM os.queue 
                                WHERE status = 'processing' 
                                AND id = p_id)
                SELECT p.procpid
                INTO v_procpid
                FROM pg_stat_activity p, q
                WHERE p.datname = current_database()
                AND p.current_query LIKE q.current_query || '%';
                
        END LOOP;

        IF v_found = FALSE THEN
                v_location := 2000;
                --job not found so check for other types of processes like Transform steps
                WITH q AS       (SELECT coalesce(replace(sql_text, ';', ''), target_table_name) as current_query
                                FROM os.queue 
                                WHERE status = 'processing' 
                                AND id = p_id)
                SELECT p.procpid
                INTO v_procpid
                FROM pg_stat_activity p, q
                WHERE p.datname = current_database()
                AND POSITION(p.current_query IN q.current_query) > 0;

                WHILE v_procpid IS NOT NULL LOOP

                        v_found := TRUE;
                        
                        PERFORM pg_cancel_backend(v_procpid);
                        
                        PERFORM pg_sleep(1);

                        WITH q AS       (SELECT coalesce(replace(sql_text, ';', ''), target_table_name) as current_query
                                        FROM os.queue 
                                        WHERE status = 'processing' 
                                        AND id = p_id)
                        SELECT p.procpid
                        INTO v_procpid
                        FROM pg_stat_activity p, q
                        WHERE p.datname = current_database()
                        AND p.current_query LIKE '%' || q.current_query || '%';
                END LOOP;
        END IF;

        IF v_found = FALSE THEN
                v_location := 3000;
                --process for job not found at all so mark as an orphaned job
                INSERT INTO os.ao_queue
                (queue_id, status, queue_date, start_date, end_date, error_message,
                num_rows, id, refresh_type, target_schema_name, target_table_name,
                target_append_only, target_compressed, target_row_orientation,
                source_type, source_server_name, source_instance_name, source_port,
                source_database_name, source_schema_name, source_table_name,
                source_user_name, source_pass, column_name, sql_text, snapshot)                
                SELECT queue_id, 'failed' as status, queue_date, start_date, clock_timestamp() AS end_date, 
                'Database session for job not found!' AS error_message,
                0 AS num_rows, id, refresh_type, target_schema_name, target_table_name,
                target_append_only, target_compressed, target_row_orientation,
                source_type, source_server_name, source_instance_name, source_port,
                source_database_name, source_schema_name, source_table_name,
                source_user_name, source_pass, column_name, sql_text, snapshot
                FROM os.queue q
                WHERE q.status = 'processing'
                AND q.id = p_id;

        END IF;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;

