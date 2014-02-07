CREATE TABLE os.ao_variables
( name text NOT NULL,
  value text,
  restart boolean default true NOT NULL,
  deleted boolean NOT NULL DEFAULT FALSE,
  insert_id serial NOT NULL
)
 WITH (appendonly=true)
 DISTRIBUTED BY (name);

INSERT INTO os.ao_variables (name, value, restart) VALUES ('Xmx', '256M', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('Xms', '128M', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('max_jobs', '4', false);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('osJar', '/usr/local/os/jar/Outsourcer.jar', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('osAgentJar', '/usr/local/os/jar/OutsourcerScheduler.jar', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('osUIJar', '/usr/local/os/jar/OutsourcerUI.jar', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('gpdbJar', '/usr/local/os/jar/gpdb.jar', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('oJar', '/usr/local/os/jar/ojdbc6.jar', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('msJar', '/usr/local/os/jar/sqljdbc4.jar', true);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('oFetchSize', '2000', false);

CREATE VIEW os.variables AS
SELECT name, value, restart
FROM    (
        SELECT name, value, restart,
               row_number() OVER (PARTITION BY name ORDER BY insert_id DESC) AS rownum, deleted
        FROM os.ao_variables
        ) AS sub
WHERE rownum = 1 AND NOT deleted;
