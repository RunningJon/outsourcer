CREATE OR REPLACE FUNCTION os.fn_queue_all()
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_queue_all';
        v_location int;
BEGIN
        v_location := 1000;
        --insert only inserts jobs that aren't already in the queue as processing or queued
        INSERT INTO os.ao_queue(status, queue_date, id, refresh_type,                 
                target_schema_name, target_table_name, target_append_only, target_compressed, target_row_orientation,
                source_type, source_server_name, source_instance_name, source_port, source_database_name, 
                source_schema_name, source_table_name, source_user_name, source_pass,
                column_name, sql_text, snapshot)
        SELECT 'queued' as status, clock_timestamp()::timestamp as queue_date, j.id, j.refresh_type, 
                j.target_schema_name, j.target_table_name, j.target_append_only, j.target_compressed, j.target_row_orientation,
                j.source_type, j.source_server_name, j.source_instance_name, j.source_port, j.source_database_name, 
                j.source_schema_name, j.source_table_name, j.source_user_name, j.source_pass,
                j.column_name, j.sql_text, j.snapshot
        FROM os.job j left join (SELECT queue_id,
                                        id
                                 FROM os.queue
                                 WHERE status in ('processing', 'queued')) q on j.id = q.id
        WHERE q.queue_id IS NULL
        ORDER BY j.id;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
