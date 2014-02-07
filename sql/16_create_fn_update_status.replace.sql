CREATE OR REPLACE FUNCTION os.fn_update_status()
  RETURNS os.queue AS
$$
DECLARE
        /* used to update status to processing */
        v_function_name character varying := 'os.fn_update_status';
        v_location int;

        v_max int := 0;
        v_count int;

        v_rec os.queue%rowtype;

BEGIN
        v_location := 1000;
        select max_jobs::int 
        into v_max
        from os.fn_get_variable('max_jobs') as max_jobs;

        v_location := 1500;
        SELECT COUNT(*) INTO v_count 
        FROM os.queue 
        WHERE status = 'processing';

        v_location := 2000;
        IF v_count < v_max THEN

                v_location := 2100;
                SELECT * INTO v_rec 
                FROM os.queue 
                WHERE status = 'queued' 
                        AND clock_timestamp()::timestamp > queue_date 
                ORDER BY queue_date LIMIT 1;

                v_location := 2200;
                IF v_rec.id IS NOT NULL THEN
                        v_location := 2300;
                        v_rec.status = 'processing';
                        v_rec.start_date := clock_timestamp();
                        v_rec.end_date := null;
                        v_rec.error_message := null;
                        v_rec.num_rows := 0;

                        v_location := 2400;
                        INSERT INTO os.ao_queue 
                        (queue_id, status, queue_date, start_date, end_date, error_message, 
                        num_rows, id, refresh_type, target_schema_name, target_table_name, 
                        target_append_only, target_compressed, target_row_orientation, 
                        source_type, source_server_name, source_instance_name, source_port, 
                        source_database_name, source_schema_name, source_table_name, 
                        source_user_name, source_pass, column_name, sql_text, snapshot)
                        VALUES 
                        (v_rec.queue_id, v_rec.status, v_rec.queue_date, v_rec.start_date, v_rec.end_date, v_rec.error_message, 
                        v_rec.num_rows, v_rec.id, v_rec.refresh_type, v_rec.target_schema_name, v_rec.target_table_name, 
                        v_rec.target_append_only, v_rec.target_compressed, v_rec.target_row_orientation, 
                        v_rec.source_type, v_rec.source_server_name, v_rec.source_instance_name, v_rec.source_port, 
                        v_rec.source_database_name, v_rec.source_schema_name, v_rec.source_table_name, 
                        v_rec.source_user_name, v_rec.source_pass, v_rec.column_name, v_rec.sql_text, v_rec.snapshot);

                END IF;
                
        END IF;

        RETURN v_rec;
                
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
