CREATE OR REPLACE FUNCTION os.fn_schedule()
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_schedule';
        v_location int;
        v_rec record;
BEGIN
	SET OPTIMIZER=OFF;

        v_location := 1000;
        --insert jobs into the queue that have a schedule_next less than now
        FOR v_rec IN (SELECT id FROM os.job WHERE schedule_next < clock_timestamp()::timestamp) LOOP
                INSERT INTO os.ao_job
                (id, refresh_type, target_schema_name, target_table_name, target_append_only, 
                 target_compressed, target_row_orientation, source_type, source_server_name, 
                 source_instance_name, source_port, source_database_name, source_schema_name, 
                 source_table_name, source_user_name, source_pass, column_name, 
                 sql_text, snapshot, schedule_desc, schedule_next, schedule_change)
                SELECT id, refresh_type, target_schema_name, target_table_name, target_append_only, 
                         target_compressed, target_row_orientation, source_type, source_server_name, 
                         source_instance_name, source_port, source_database_name, source_schema_name, 
                         source_table_name, source_user_name, source_pass, column_name, 
                         sql_text, snapshot, schedule_desc, null::timestamp as schedule_next, schedule_change
                FROM os.job
                WHERE id = v_rec.id;
                PERFORM os.fn_queue(v_rec.id);
        END LOOP;

        v_location := 2000;
        --update the schedule_next for jobs that have completed
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
        FROM os.job j, 
             (SELECT j.id, min(exec_time) AS schedule_next
             FROM       (
                        SELECT CASE WHEN interval_trunc = 'minute' THEN date_trunc('hour', now()::timestamp) + (interval_quantity::interval * i)
                        ELSE date_trunc(interval_trunc, (now()::timestamp - ('1' || interval_trunc)::interval)) + (('1' || interval_trunc)::interval * i) + interval_quantity::interval END AS exec_time,  
                        description
                        FROM os.schedule, generate_series(0, 60) AS i
                        ) as sub 
             JOIN os.job j ON sub.description = j.schedule_desc
             WHERE exec_time > now()
             GROUP BY j.id) as sub2
        WHERE sub2.id = j.id
        AND NOT EXISTS (SELECT NULL FROM os.queue q WHERE j.id = q.id AND status IN ('processing', 'queued'))
        AND j.schedule_next IS NULL;

        v_location := 3000;
        --update the schedule_next for jobs that have changed
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
               sql_text, snapshot, schedule_desc, sub2.schedule_next, false as schedule_change
        FROM os.job j,
             (SELECT j.id, min(exec_time) AS schedule_next
             FROM       (
                        SELECT CASE WHEN interval_trunc = 'minute' THEN date_trunc('hour', now()::timestamp) + (interval_quantity::interval * i)
                        ELSE date_trunc(interval_trunc, (now()::timestamp - ('1' || interval_trunc)::interval)) + (('1' || interval_trunc)::interval * i) + interval_quantity::interval END AS exec_time,  
                        description
                        FROM os.schedule, generate_series(0, 60) AS i
                        ) as sub 
             JOIN os.job j ON sub.description = j.schedule_desc
             WHERE exec_time > now()
             GROUP BY j.id) AS sub2
        WHERE sub2.id = j.id
        AND NOT EXISTS (SELECT NULL FROM os.queue q WHERE j.id = q.id AND status IN ('processing', 'queued'))
        AND j.schedule_next IS NULL;
        
        v_location := 4000;
        --remove schedule_next if the schedule_desc is removed
        INSERT INTO os.ao_job
        (id, refresh_type, target_schema_name, target_table_name, target_append_only, 
         target_compressed, target_row_orientation, source_type, source_server_name, 
         source_instance_name, source_port, source_database_name, source_schema_name, 
         source_table_name, source_user_name, source_pass, column_name, 
         sql_text, snapshot, schedule_desc, schedule_next, schedule_change)
        SELECT id, refresh_type, target_schema_name, target_table_name, target_append_only, 
                 target_compressed, target_row_orientation, source_type, source_server_name, 
                 source_instance_name, source_port, source_database_name, source_schema_name, 
                 source_table_name, source_user_name, source_pass, column_name, 
                 sql_text, snapshot, schedule_desc, null::timestamp as schedule_next, schedule_change
        FROM os.job j
        WHERE schedule_desc IS NULL AND schedule_next IS NOT NULL;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql VOLATILE;
