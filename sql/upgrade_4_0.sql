CREATE EXTERNAL WEB TABLE os.osstop
(foo text) 
EXECUTE '/usr/local/os/bin/osstop' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');

CREATE EXTERNAL WEB TABLE os.osstart
(foo text) 
EXECUTE '/usr/local/os/bin/osstart' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');

CREATE EXTERNAL WEB TABLE os.osstatus
(status text) 
EXECUTE '/usr/local/os/bin/osstatus' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');

CREATE EXTERNAL WEB TABLE os.agentstop
(foo text)
EXECUTE '/usr/local/os/bin/agentstop' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');

CREATE EXTERNAL WEB TABLE os.agentstart
(foo text)
EXECUTE '/usr/local/os/bin/agentstart' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');

CREATE EXTERNAL WEB TABLE os.agentstatus
(status text)
EXECUTE '/usr/local/os/bin/agentstatus' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');

ALTER TABLE os.variables ADD restart boolean default true NOT NULL;

DELETE FROM os.variables 
WHERE name NOT IN ('Xmx', 'Xms', 'max_jobs', 'osJar', 'msJar', 'oJar');

INSERT INTO os.variables VALUES ('osAgentJar', '/usr/local/os/jar/OutsourcerScheduler.jar', true);
INSERT INTO os.variables VALUES ('osUIJar', '/usr/local/os/jar/OutsourcerUI.jar', true);
INSERT INTO os.variables VALUES ('gpdbJar', '/usr/local/os/jar/gpdb.jar', true);
INSERT INTO os.variables VALUES ('oFetchSize', '2000', false);
 
UPDATE os.variables 
SET restart = false 
WHERE name IN ('max_jobs', 'oFetchSize');

CREATE TABLE os.sessions
(session_id int NOT NULL,
 expire_date timestamp NOT NULL DEFAULT current_timestamp + interval '15 minutes')
DISTRIBUTED BY (session_id);

ALTER TABLE os.job DROP CONSTRAINT job_check;
ALTER TABLE os.job
  ADD CONSTRAINT job_check
  CHECK ((refresh_type = 'refresh'::text AND column_name IS NULL AND snapshot IS NULL
          AND (source).type IN ('oracle'::text, 'sqlserver'::text) AND (source).server_name IS NOT NULL
          AND (source).database_name IS NOT NULL AND (source).schema_name IS NOT NULL
          AND (source).table_name IS NOT NULL AND (source).user_name IS NOT NULL
          AND (source).pass IS NOT NULL)  OR
         (refresh_type = 'append'::text AND column_name IS NOT NULL AND snapshot IS NULL) OR
         (refresh_type = 'replication'::text AND column_name IS NOT NULL AND snapshot IS NOT NULL) OR
         (refresh_type = 'transform' AND (source).type IS NULL AND (source).server_name IS NULL
          AND (source).instance_name IS NULL AND (source).port IS NULL
          AND (source).database_name IS NULL AND (source).schema_name IS NULL AND (source).table_name IS NULL AND (source).user_name IS NULL
          AND (source).pass IS NULL AND column_name IS NULL AND sql_text IS NOT NULL AND snapshot IS NULL ) OR
         (refresh_type = 'ddl'::text AND column_name IS NULL AND snapshot IS NULL 
          AND (source).type IN ('oracle'::text, 'sqlserver'::text) AND (source).server_name IS NOT NULL
          AND (source).database_name IS NOT NULL AND (source).schema_name IS NOT NULL
          AND (source).table_name IS NOT NULL AND (source).user_name IS NOT NULL
          AND (source).pass IS NOT NULL)
         );

ALTER TABLE os.queue DROP CONSTRAINT job_check;
ALTER TABLE os.queue
  ADD CONSTRAINT job_check
  CHECK ((refresh_type = 'refresh'::text AND column_name IS NULL AND snapshot IS NULL
          AND (source).type IN ('oracle'::text, 'sqlserver'::text) AND (source).server_name IS NOT NULL
          AND (source).database_name IS NOT NULL AND (source).schema_name IS NOT NULL
          AND (source).table_name IS NOT NULL AND (source).user_name IS NOT NULL
          AND (source).pass IS NOT NULL)  OR
         (refresh_type = 'append'::text AND column_name IS NOT NULL AND snapshot IS NULL) OR
         (refresh_type = 'replication'::text AND column_name IS NOT NULL AND snapshot IS NOT NULL) OR
         (refresh_type = 'transform' AND (source).type IS NULL AND (source).server_name IS NULL
          AND (source).instance_name IS NULL AND (source).port IS NULL
          AND (source).database_name IS NULL AND (source).schema_name IS NULL AND (source).table_name IS NULL AND (source).user_name IS NULL
          AND (source).pass IS NULL AND column_name IS NULL AND sql_text IS NOT NULL AND snapshot IS NULL ) OR
         (refresh_type = 'ddl'::text AND column_name IS NULL AND snapshot IS NULL
          AND (source).type IN ('oracle'::text, 'sqlserver'::text) AND (source).server_name IS NOT NULL
          AND (source).database_name IS NOT NULL AND (source).schema_name IS NOT NULL
          AND (source).table_name IS NOT NULL AND (source).user_name IS NOT NULL
          AND (source).pass IS NOT NULL)
         );

CREATE OR REPLACE FUNCTION os.fn_update_status()
  RETURNS os.queue AS
$$
DECLARE
        /* used to update status to processing */
        v_function_name character varying := 'os.fn_update_status';
        v_location int;

        v_max int := os.fn_get_variable('max_jobs');
        v_count int;

        v_rec os.queue%rowtype;

BEGIN
        v_location := 1000;
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
                        UPDATE os.queue
                        SET status = 'processing', start_date = clock_timestamp()::timestamp, error_message = null
                        WHERE queue_id = v_rec.queue_id;
                END IF;
                
        END IF;

        RETURN v_rec;
                
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql VOLATILE;

CREATE TABLE os.schedule
(description text NOT NULL PRIMARY KEY,
 interval_trunc text NOT NULL,  --example: day
 interval_quantity text NOT NULL  --example: 1 day 4 hours
 ) 
 DISTRIBUTED BY (description);

INSERT INTO os.schedule (description, interval_trunc, interval_quantity) VALUES ('Hourly', 'hour', '1 hour');
INSERT INTO os.schedule (description, interval_trunc, interval_quantity) VALUES ('Daily', 'day', '1 day 4 hours');
INSERT INTO os.schedule (description, interval_trunc, interval_quantity) VALUES ('Weekly', 'week', '1 week 4 hours');
INSERT INTO os.schedule (description, interval_trunc, interval_quantity) VALUES ('Monthly', 'month', '1 month 4 hours');
INSERT INTO os.schedule (description, interval_trunc, interval_quantity) VALUES ('5 Minutes', 'minute', '5 minutes');

ALTER TABLE os.job ADD schedule_desc text REFERENCES os.schedule(description);
ALTER TABLE os.job add schedule_next timestamp;
ALTER TABLE os.job ADD schedule_change boolean DEFAULT FALSE;

CREATE OR REPLACE FUNCTION os.fn_start_schedule() RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_start_schedule';
        v_location int;
BEGIN
        v_location := 1000;
        UPDATE os.job
        SET schedule_next = sub2.schedule_next
        FROM    (SELECT j.id, min(exec_time) AS schedule_next
                FROM (
                SELECT CASE WHEN interval_trunc = 'minute' THEN date_trunc('hour', now()::timestamp) + (interval_quantity::interval * i)
                ELSE date_trunc(interval_trunc, (now()::timestamp - ('1' || interval_trunc)::interval)) + (('1' || interval_trunc)::interval * i) + interval_quantity::interval END AS exec_time,  
                description
                FROM os.schedule, generate_series(0, 60) AS i
                ) as sub 
                JOIN os.job j ON sub.description = j.schedule_desc
                WHERE exec_time > now()
                GROUP BY j.id
                ) AS sub2
        WHERE sub2.id = os.job.id;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION os.fn_schedule() RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_schedule';
        v_location int;
        v_rec record;
BEGIN
        v_location := 1000;
        --insert jobs into the queue that have a schedule_next less than now
        FOR v_rec IN (SELECT id FROM os.job WHERE schedule_next < clock_timestamp()::timestamp) LOOP
                UPDATE os.job SET schedule_next = NULL WHERE id = v_rec.id;
                PERFORM os.fn_queue(v_rec.id);
        END LOOP;

        v_location := 2000;
        --update the schedule_next for jobs that have completed
        UPDATE os.job j
        SET schedule_next = sub2.schedule_next
        FROM    (SELECT j.id, min(exec_time) AS schedule_next
                FROM (
                SELECT CASE WHEN interval_trunc = 'minute' THEN date_trunc('hour', now()::timestamp) + (interval_quantity::interval * i)
                ELSE date_trunc(interval_trunc, (now()::timestamp - ('1' || interval_trunc)::interval)) + (('1' || interval_trunc)::interval * i) + interval_quantity::interval END AS exec_time,  
                description
                FROM os.schedule, generate_series(0, 60) AS i
                ) as sub 
                JOIN os.job j ON sub.description = j.schedule_desc
                WHERE exec_time > now()
                GROUP BY j.id
                ) AS sub2
        WHERE sub2.id = j.id
        AND NOT EXISTS (SELECT NULL FROM os.queue q WHERE j.id = q.id AND status IN ('processing', 'queued'))
        AND j.schedule_next IS NULL;

        v_location := 3000;
        --update the schedule_next for jobs that have changed
        UPDATE os.job j
        SET schedule_next = sub2.schedule_next, schedule_change = FALSE
        FROM    (SELECT j.id, min(exec_time) AS schedule_next
                FROM (
                SELECT CASE WHEN interval_trunc = 'minute' THEN date_trunc('hour', now()::timestamp) + (interval_quantity::interval * i)
                ELSE date_trunc(interval_trunc, (now()::timestamp - ('1' || interval_trunc)::interval)) + (('1' || interval_trunc)::interval * i) + interval_quantity::interval END AS exec_time,  
                description
                FROM os.schedule, generate_series(0, 60) AS i
                ) as sub 
                JOIN os.job j ON sub.description = j.schedule_desc
                WHERE exec_time > now()
                GROUP BY j.id
                ) AS sub2
        WHERE sub2.id = j.id
        AND NOT EXISTS (SELECT NULL FROM os.queue q WHERE j.id = q.id AND status IN ('processing', 'queued'))
        AND j.schedule_change IS TRUE;
        

        v_location := 4000;
        --remove schedule_next if the schedule_desc is removed
        UPDATE os.job
        SET schedule_next = NULL
        WHERE schedule_desc IS NULL;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION os.fn_create_ext_table(p_table text, p_columns text[], p_connection_id integer, p_sql_text text)
  RETURNS void AS
$BODY$
DECLARE
        v_function_name text := 'os.fn_create_ext_table';
        v_location int;
        
        v_sql text;
        v_table text;
        v_java text;
        v_count int;
BEGIN
        v_location := 1000;
        PERFORM NULL
        FROM os.ext_connection
        WHERE id = p_connection_id;

        GET DIAGNOSTICS v_count = ROW_COUNT;
        
        v_location := 1500;
        IF v_count = 0 THEN
                RAISE EXCEPTION 'ConnectionID: "%" is not valid', p_connection_id;
        END IF;

        v_location := 2000;
        v_table := LOWER(p_table);
        v_sql := 'DROP EXTERNAL TABLE IF EXISTS ' || v_table;
        RAISE INFO '%', v_sql;
        EXECUTE v_sql;

        v_location := 3000;
        FOR i IN array_lower(p_columns, 1)..array_upper(p_columns,1) LOOP
                IF i = 1 THEN
                        v_sql := 'CREATE EXTERNAL WEB TABLE ' || v_table || E' \n' ||
                                '(' || p_columns[i];
                ELSE
                        v_sql := v_sql || E', \n' || p_columns[i];
                END IF;

        END LOOP;

        v_location := 3500;
        v_sql := v_sql || E')\n' || 'EXECUTE E''java -classpath ';

        v_location := 4000;
        SELECT os.jar || ':' || g.jar || ':' || CASE WHEN ms.jar IS NOT NULL THEN ms.jar || ':'  ELSE '' END || COALESCE(o.jar, '') ||
                ' -Xms' || s.xms || ' -Xmx' || x.xmx || ' -Djava.security.egd=file:///dev/urandom ExternalData ' || current_database() || ' ' || p.port || ' ' || e.id || 
                ' "' || p_sql_text || '"'
        INTO v_java
        FROM    (SELECT value AS jar FROM os.variables WHERE name = 'gpdbJar') AS g,
                (SELECT value AS jar FROM os.variables WHERE name = 'osJar') AS os,
                (SELECT value AS jar FROM os.variables WHERE name = 'msJar') AS ms,
                (SELECT value AS jar FROM os.variables WHERE name = 'oJar') AS o,
                (SELECT value AS xms FROM os.variables WHERE name = 'Xms') AS s,
                (SELECT value AS xmx FROM os.variables WHERE name = 'Xmx') AS x,
                (SELECT setting AS port FROM pg_settings WHERE name = 'port') AS p,
                os.ext_connection e
        WHERE e.id = p_connection_id;

        v_location := 5000;
        v_sql := v_sql || v_java || E''' ON MASTER FORMAT ''TEXT'' (delimiter ''|'' null ''null'')';
        RAISE INFO '%', v_sql;
        EXECUTE v_sql;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql VOLATILE;

