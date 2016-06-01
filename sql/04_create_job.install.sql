CREATE TABLE os.ao_job
( id serial NOT NULL,
  refresh_type text NOT NULL,
  
  target_schema_name text,
  target_table_name text,
  target_append_only boolean NOT NULL,
  target_compressed boolean NOT NULL,
  target_row_orientation boolean NOT NULL,
  
  source_type text, 
  source_server_name text,
  source_instance_name text,
  source_port int, 
  source_database_name text,
  source_schema_name text,
  source_table_name text,
  source_user_name text,
  source_pass text,
  
  column_name text,
  sql_text text,
  snapshot boolean NOT NULL,
  
  schedule_desc text,
  schedule_next timestamp,
  schedule_change boolean DEFAULT FALSE,

  deleted boolean NOT NULL DEFAULT FALSE,
  insert_id serial NOT NULL)
 WITH (appendonly=true)
:DISTRIBUTED_BY;
--DISTRIBUTED BY (id);

CREATE VIEW os.job AS
SELECT id, refresh_type, target_schema_name, target_table_name, target_append_only, 
       target_compressed, target_row_orientation, source_type, source_server_name, 
       source_instance_name, source_port, source_database_name, source_schema_name, 
       source_table_name, source_user_name, source_pass, column_name, 
       sql_text, snapshot, schedule_desc, schedule_next, schedule_change
FROM    (
        SELECT  id, refresh_type, target_schema_name, target_table_name, target_append_only, 
                target_compressed, target_row_orientation, source_type, source_server_name, 
                source_instance_name, source_port, source_database_name, source_schema_name, 
                source_table_name, source_user_name, source_pass, column_name, 
                sql_text, snapshot, schedule_desc, schedule_next, schedule_change,
                row_number() OVER (PARTITION BY id ORDER BY insert_id DESC) AS rownum, deleted
        FROM os.ao_job
        ) AS sub
WHERE rownum = 1 AND NOT deleted;

ALTER TABLE os.ao_job
  ADD CONSTRAINT job_check
  CHECK ((refresh_type = 'refresh'::text AND column_name IS NULL 
          AND source_type IN ('oracle'::text, 'sqlserver'::text) AND source_server_name IS NOT NULL
          AND source_database_name IS NOT NULL AND source_schema_name IS NOT NULL
          AND source_table_name IS NOT NULL AND source_user_name IS NOT NULL AND source_pass IS NOT NULL
          AND target_schema_name IS NOT NULL AND target_table_name IS NOT NULL
          )  OR
         (refresh_type = 'append'::text AND column_name IS NOT NULL
          AND source_type IN ('oracle'::text, 'sqlserver'::text) AND source_server_name IS NOT NULL
          AND source_database_name IS NOT NULL AND source_schema_name IS NOT NULL
          AND source_table_name IS NOT NULL AND source_user_name IS NOT NULL AND source_pass IS NOT NULL
          AND target_schema_name IS NOT NULL AND target_table_name IS NOT NULL
          )
         OR
         (refresh_type = 'replication'::text AND column_name IS NOT NULL
          AND source_type IN ('oracle'::text, 'sqlserver'::text) AND source_server_name IS NOT NULL
          AND source_database_name IS NOT NULL AND source_schema_name IS NOT NULL
          AND source_table_name IS NOT NULL AND source_user_name IS NOT NULL AND source_pass IS NOT NULL
          AND target_schema_name IS NOT NULL AND target_table_name IS NOT NULL
         ) OR
         (refresh_type = 'transform' AND source_type IS NULL AND source_server_name IS NULL
          AND source_instance_name IS NULL AND source_port IS NULL
          AND source_database_name IS NULL AND source_schema_name IS NULL AND source_table_name IS NULL AND source_user_name IS NULL
          AND source_pass IS NULL AND column_name IS NULL AND sql_text IS NOT NULL
          AND target_schema_name IS NULL AND target_table_name IS NULL
          ) OR
         (refresh_type = 'ddl'::text AND column_name IS NULL AND sql_text IS NULL
          AND source_type IN ('oracle'::text, 'sqlserver'::text) AND source_server_name IS NOT NULL
          AND source_database_name IS NOT NULL AND source_schema_name IS NOT NULL
          AND source_table_name IS NOT NULL AND source_user_name IS NOT NULL AND source_pass IS NOT NULL
          AND target_schema_name IS NOT NULL AND target_table_name IS NOT NULL
         )
         );

ALTER TABLE os.ao_job ADD CONSTRAINT job_check_port
  CHECK ((source_port > 0 AND source_type = 'oracle'::text) OR (source_type <> 'oracle'::text AND source_port IS NULL));

ALTER TABLE os.ao_job ADD CONSTRAINT job_refresh_type
  CHECK (refresh_type in ('append', 'refresh', 'replication', 'transform', 'ddl'));
