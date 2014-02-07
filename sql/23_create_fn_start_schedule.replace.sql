CREATE OR REPLACE FUNCTION os.fn_start_schedule()
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_start_schedule';
        v_location int;
BEGIN

        v_location := 1000;
        INSERT INTO os.ao_job
        (id, refresh_type, target_schema_name, target_table_name, target_append_only, 
         target_compressed, target_row_orientation, source_type, source_server_name, 
         source_instance_name, source_port, source_database_name, source_schema_name, 
         source_table_name, source_user_name, source_pass, column_name, 
         sql_text, snapshot, schedule_desc, schedule_next, schedule_change)
        SELECT j.id, refresh_type, target_schema_name, target_table_name, target_append_only, 
                 target_compressed, target_row_orientation, source_type, source_server_name, 
                 source_instance_name, source_port, source_database_name, source_schema_name, 
                 source_table_name, source_user_name, source_pass, column_name, 
                 sql_text, snapshot, schedule_desc, sub2.schedule_next, schedule_change
        FROM    (SELECT j.id, min(exec_time) AS schedule_next
                FROM (
                SELECT CASE WHEN interval_trunc = 'minute' THEN date_trunc('hour', now()::timestamp) + (interval_quantity::interval * i)
                ELSE date_trunc(interval_trunc, (now()::timestamp - ('1' || interval_trunc)::interval)) + (('1' || interval_trunc)::interval * i) + interval_quantity::interval END AS exec_time,  
                description
                FROM os.schedule, generate_series(0, 60) AS i
                ) as sub 
                JOIN os.job j ON sub.description = j.schedule_desc
                WHERE exec_time > now()
                GROUP BY j.id) as sub2,
                os.job j
        WHERE sub2.id = j.id;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
