CREATE OR REPLACE FUNCTION os.fn_update_status(p_queue_id int, p_status text, p_queue_date timestamp, p_start_date timestamp, p_error_message text, 
       p_num_rows int, p_id int, p_refresh_type text, p_target_schema_name text, p_target_table_name text, 
       p_target_append_only boolean, p_target_compressed boolean, p_target_row_orientation boolean, 
       p_source_type text, p_source_server_name text, source_instance_name text, p_source_port int, 
       p_source_database_name text, p_source_schema_name text, p_source_table_name text, 
       p_source_user_name text, p_source_pass text, p_column_name text, p_sql_text text, p_snapshot boolean)
  RETURNS void AS
$$
	SET OPTIMIZER=OFF;

        INSERT INTO os.ao_queue 
        (queue_id, status, queue_date, start_date, end_date, error_message, 
        num_rows, id, refresh_type, target_schema_name, target_table_name, 
        target_append_only, target_compressed, target_row_orientation, 
        source_type, source_server_name, source_instance_name, source_port, 
        source_database_name, source_schema_name, source_table_name, 
        source_user_name, source_pass, column_name, sql_text, snapshot)
        VALUES 
        ($1, $2, $3, $4, clock_timestamp(), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25);

$$
  LANGUAGE sql;
