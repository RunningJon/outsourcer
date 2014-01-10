/*DROP SCHEMA IF EXISTS os CASCADE; */


DROP SCHEMA IF EXISTS ext CASCADE;

CREATE SCHEMA os;

--external tables go here
CREATE SCHEMA ext;

create type os.type_target as
( schema_name text,
  table_name text);

create type os.type_source as
( type text, 
  server_name text,
  instance_name text,
  port int, --used by Oracle
  database_name text,
  schema_name text,
  table_name text,
  user_name text,
  pass text);

CREATE TABLE os.job
( id serial primary key NOT NULL,
  refresh_type text NOT NULL,
  target os.type_target,
  source os.type_source,
  column_name text,
  sql_text text,
  snapshot boolean)
DISTRIBUTED BY (id);

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
         (refresh_type = 'ddl'::text AND column_name IS NULL AND snapshot IS NULL AND sql_text IS NULL
          AND (source).type IN ('oracle'::text, 'sqlserver'::text) AND (source).server_name IS NOT NULL
          AND (source).database_name IS NOT NULL AND (source).schema_name IS NOT NULL
          AND (source).table_name IS NOT NULL AND (source).user_name IS NOT NULL
          AND (source).pass IS NOT NULL)
         );

ALTER TABLE os.job ADD CONSTRAINT job_check_port
  CHECK (((source).port > 0 AND (source).type = 'oracle'::text) OR ((source).type <> 'oracle'::text AND (source).port IS NULL));

ALTER TABLE os.job ADD CONSTRAINT job_refresh_type
  CHECK (refresh_type in ('append', 'refresh', 'replication', 'transform', 'ddl'));

CREATE TABLE os.queue
(
  queue_id serial primary key NOT NULL,
  status text NOT NULL check(status in ('queued', 'success', 'failed', 'processing')),
  queue_date timestamp without time zone NOT NULL,
  start_date timestamp without time zone,
  end_date timestamp,
  error_message text,
  num_rows int not null default 0,
  LIKE os.job INCLUDING CONSTRAINTS)
DISTRIBUTED BY (queue_id);

CREATE TABLE os.variables
( name text primary key NOT NULL,
  value text,
  restart boolean default true NOT NULL
)
  distributed by (name);

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

INSERT INTO os.variables VALUES ('Xmx', '256M', true);
INSERT INTO os.variables VALUES ('Xms', '128M', true);
INSERT INTO os.variables VALUES ('max_jobs', '4', false);
INSERT INTO os.variables VALUES ('osJar', '/usr/local/os/jar/Outsourcer.jar', true);
INSERT INTO os.variables VALUES ('osAgentJar', '/usr/local/os/jar/OutsourcerScheduler.jar', true);
INSERT INTO os.variables VALUES ('osUIJar', '/usr/local/os/jar/OutsourcerUI.jar', true);
INSERT INTO os.variables VALUES ('gpdbJar', '/usr/local/os/jar/gpdb.jar', true);
INSERT INTO os.variables VALUES ('oJar', '/usr/local/os/jar/ojdbc6.jar', true);
INSERT INTO os.variables VALUES ('msJar', '/usr/local/os/jar/sqljdbc4.jar', true);
INSERT INTO os.variables VALUES ('oFetchSize', '2000', false);

CREATE TABLE os.sessions
(session_id int NOT NULL,
 expire_date timestamp NOT NULL DEFAULT current_timestamp + interval '15 minutes')
DISTRIBUTED BY (session_id);

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

CREATE OR REPLACE FUNCTION os.fn_get_variable(p_name text)
  RETURNS text AS
$$
DECLARE
        v_function_name text := 'os.fn_get_variable';
        v_location int;

        v_value os.variables.value%type;
BEGIN

        v_location := 1000;
        SELECT VALUE INTO v_value 
        FROM os.variables 
        WHERE name = p_name;

        v_location := 2000;
        RETURN v_value;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;

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

CREATE OR REPLACE FUNCTION os.fn_update_status(p_queue_id integer, p_status text, p_num_rows int, p_error_message text)
  RETURNS void AS
$$
DECLARE
        /* used to update status to success or failed */
        v_function_name character varying := 'os.fn_update_status';
        v_location int;

        v_target_table text;
        v_id os.queue.queue_id%type;
        
BEGIN
        v_location := 1000;

        IF p_status = 'success' THEN
                UPDATE os.queue SET status = p_status, end_date = clock_timestamp()::timestamp, num_rows = p_num_rows, error_message = null WHERE queue_id = p_queue_id;
        ELSIF p_status = 'failed' THEN
                UPDATE os.queue SET status = p_status, end_date = clock_timestamp()::timestamp, num_rows = 0, error_message = p_error_message WHERE queue_id = p_queue_id;
        ELSE
                RAISE EXCEPTION 'Status: % is not valid!', p_status;
        END IF;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION os.fn_queue_all()
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_queue_all';
        v_location int;
BEGIN
        v_location := 1000;
        --insert only inserts jobs that aren't already in the queue as processing or queued
        INSERT INTO os.queue(status, queue_date, 
                id, refresh_type, target, source, column_name, sql_text, snapshot)
        SELECT 'queued' as status, clock_timestamp()::timestamp as queue_date, 
                j.id, j.refresh_type, j.target, j.source, j.column_name, j.sql_text, j.snapshot
        FROM os.job j left join (SELECT queue_id,
                                        id
                                 FROM os.queue
                                 WHERE status in ('processing', 'queued')) q on j.id = q.id
        WHERE q.queue_id IS NULL;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION os.fn_queue(p_id integer)
  RETURNS void AS
$$
DECLARE
        v_function_name text := 'os.fn_queue';
        v_location int;
BEGIN
        v_location := 1000;
        INSERT INTO os.queue(status, queue_date, 
                id, refresh_type, target, source, column_name, sql_text, snapshot)
        SELECT 'queued' as status, clock_timestamp()::timestamp as queue_date, 
                j.id, j.refresh_type, j.target, j.source, j.column_name, j.sql_text, j.snapshot
        FROM os.job j left join (SELECT queue_id,
                                        id
                                 FROM os.queue
                                 WHERE status in ('processing', 'queued')) q on j.id = q.id
        WHERE q.queue_id IS NULL
                AND j.id = p_id;
EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;
  
CREATE OR REPLACE FUNCTION os.fn_replication_setup(p_target_schema text, p_target_table text, p_ext_schema text, p_stage_table text, p_arch_table text, p_column_name text)
  RETURNS void AS
$$
DECLARE
        v_function text := 'os.fn_replication_setup';
        v_location int;
        v_sql text;
        v_debug BOOLEAN := true;

BEGIN
        v_location := 1000;
        v_sql := 'DROP TABLE IF EXISTS "' || p_ext_schema || '"."' || p_stage_table || '"';

        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;

        EXECUTE v_sql;

        v_location := 1500;
        v_sql := 'CREATE TABLE "' || p_ext_schema || '"."' || p_stage_table || '" ' ||
                 '("' || LOWER(p_column_name) || '" bigint not null, ' ||
                 'change_type char(1) NOT NULL, ' ||
                 'LIKE "' || p_target_schema || '"."' || p_target_table || '") ' ||
                 'DISTRIBUTED BY ("' || LOWER(p_column_name) || '")';
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        
        EXECUTE v_sql;

        v_location := 2000;
        v_sql := 'DROP TABLE IF EXISTS "' || p_ext_schema || '"."' || p_arch_table || '"';

        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;

        EXECUTE v_sql;

        v_location := 2500;
        v_sql := 'CREATE TABLE "' || p_ext_schema || '"."' || p_arch_table || '" ' ||
                 '("' || LOWER(p_column_name) || '" bigint not null, ' ||
                 'change_type char(1) NOT NULL, ' ||
                 'LIKE "' || p_target_schema || '"."' || p_target_table || '") ' ||
                 'DISTRIBUTED BY ("' || LOWER(p_column_name) || '")';
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        
        EXECUTE v_sql;

EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION os.fn_replication(p_target_schema text, p_target_table text,
                                             p_stage_schema text, p_stage_table text, 
                                             p_append_column_name text)
  RETURNS void AS
$$
DECLARE
        v_location INT;
        v_procedure VARCHAR := 'os.fn_replication';

        v_debug BOOLEAN := true;
        v_rec RECORD;
        v_sql TEXT;
        v_columns TEXT;        
        v_pk_columns TEXT;
        v_pk_delete text;
        i INT := 0;
        v_arch_table TEXT;
        v_stage_table TEXT;
        
BEGIN
        v_location := 1000;
        --take the columns from the primary key and create the join statement for the DELETE below
        FOR v_rec IN (SELECT column_name 
                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                      WHERE table_schema = p_target_schema 
                      AND table_name = p_target_table
                      ORDER BY ordinal_position) LOOP
                i := i + 1;
                
                IF i = 1 THEN
                        v_pk_delete := 'x.' || v_rec.column_name || ' = y.' || v_rec.column_name;
                        v_pk_columns := v_rec.column_name;
                ELSE
                        v_pk_delete := v_pk_delete || ' AND x.' || v_rec.column_name || ' = y.' || v_rec.column_name;
                        v_pk_columns := v_pk_columns || ', ' || v_rec.column_name;
                END IF;

        END LOOP;
        
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_pk_delete;
        END IF;
        
        --create the temp table that is distributed the same as the target table
        v_location := 2000;
        v_stage_table := p_stage_table || '_stage';
        v_sql := 'CREATE TEMPORARY TABLE temp_' || p_stage_schema || '_' || p_stage_table || ' ON COMMIT DROP AS ' ||
                         'SELECT * FROM "' || p_stage_schema || '"."' || p_stage_table || '" ' ||
                         'DISTRIBUTED BY (' || v_pk_columns || ')';

        v_location := 2200;
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;
        
        --delete the Update and Delete records in the Target Table
        v_location := 3500;
        v_sql := 'DELETE FROM ' || p_target_schema || '.' || p_target_table || ' x USING temp_' || p_stage_schema || '_' || p_stage_table || ' y ' || 
                         'WHERE ' || v_pk_delete || ' AND change_type IN (''U'', ''D'')';

        v_location := 3600;
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

        --get the columns from the target table
        v_location := 3800;
        i := 0;
        FOR v_rec in SELECT column_name --, CASE WHEN data_type = 'character' THEN 'text' ELSE data_type END as data_type
                     FROM information_schema.columns 
                     WHERE table_schema = p_target_schema
                     AND table_name = p_target_table
                     ORDER BY ordinal_position LOOP
                
                i := i + 1;
                IF i = 1 THEN
                    v_columns := v_rec.column_name;
                ELSE
                    v_columns := v_columns || ', ' || v_rec.column_name;
                END IF;
        
        END LOOP;
        
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_columns;
        END IF;
        --insert the Update and Insert records
        v_location := 4000;
        v_sql := 'INSERT INTO "' || p_target_schema || '"."' || p_target_table || '" ( ' || v_columns || ') ' ||
                         'SELECT ' || v_columns || ' FROM (' ||
                         'SELECT rank() over (partition by ' || v_pk_columns || ' order by "' || LOWER(p_append_column_name) || '" desc) as rank, ' || v_columns || ', change_type ' ||
                         'FROM temp_' || p_stage_schema || '_' || p_stage_table || ' ) AS SUB1 ' ||
                         'WHERE SUB1.rank = 1 AND change_type IN (''U'', ''I'')';

        v_location := 4100;
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

        v_location := 5000;
        v_arch_table := p_target_schema || '_' || p_target_table || '_arch';
        
        v_sql := 'TRUNCATE TABLE "' || p_stage_schema || '"."' || v_arch_table || '"';
        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

        v_location := 6000;
        v_sql := 'INSERT INTO "' || p_stage_schema || '"."' || v_arch_table || '" ' ||
                'SELECT * FROM "' || 'temp_' || p_stage_schema || '_' || p_stage_table || '"';

        IF v_debug THEN
                RAISE INFO '%:%', v_location, v_sql;
        END IF;
        EXECUTE v_sql;

EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION'(%:%:%)', v_procedure, v_location, sqlerrm;
END;
$$
  LANGUAGE plpgsql;

CREATE TABLE os.ext_connection
( id serial primary key NOT NULL,
  type text,
  server_name text NOT NULL,
  instance_name text,  --optional by SQL Server
  port int, --used by Oracle
  database_name text,  --used by Oracle
  user_name text NOT NULL,
  pass text NOT NULL)
DISTRIBUTED BY (id);

ALTER TABLE os.ext_connection
  ADD CONSTRAINT ext_check
  CHECK ( (type = 'oracle'::text AND database_name IS NOT NULL AND instance_name IS NULL) OR
          (type = 'sqlserver' AND database_name IS NULL) );

ALTER TABLE os.ext_connection ADD CONSTRAINT ext_check_port
  CHECK ((port > 0 AND type = 'oracle'::text) OR (type = 'sqlserver'::text AND port IS NULL));

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

