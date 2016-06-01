CREATE TABLE os.ao_ext_connection
( id serial NOT NULL,
  source_type text,
  source_server_name text NOT NULL,
  source_instance_name text,  --optional by SQL Server
  source_port int, --used by Oracle
  source_database_name text,  --used by Oracle
  source_user_name text NOT NULL,
  source_pass text NOT NULL,
  deleted boolean NOT NULL DEFAULT FALSE,
  insert_id serial NOT NULL)
 WITH (appendonly=true)
:DISTRIBUTED_BY;
 --DISTRIBUTED BY (id);

CREATE VIEW os.ext_connection AS
SELECT id, source_type, source_server_name, source_instance_name, source_port, 
       source_database_name, source_user_name, source_pass
FROM    (
        SELECT id, source_type, source_server_name, source_instance_name, source_port, 
               source_database_name, source_user_name, source_pass,
               row_number() OVER (PARTITION BY id ORDER BY insert_id DESC) AS rownum, deleted
        FROM os.ao_ext_connection
        ) AS sub
WHERE rownum = 1 AND NOT deleted;

ALTER TABLE os.ao_ext_connection
  ADD CONSTRAINT ext_check
  CHECK ( (source_type = 'oracle'::text AND source_database_name IS NOT NULL AND source_instance_name IS NULL) OR
          (source_type = 'sqlserver' AND source_database_name IS NULL) );

ALTER TABLE os.ao_ext_connection ADD CONSTRAINT ext_check_port
  CHECK ((source_port > 0 AND source_type = 'oracle'::text) OR (source_type = 'sqlserver'::text AND source_port IS NULL));
