DROP VIEW IF EXISTS os.variables;
DROP TABLE IF EXISTS os.ao_variables;

CREATE TABLE os.ao_variables
( name text NOT NULL,
  value text,
  restart boolean default true NOT NULL,
  deleted boolean NOT NULL DEFAULT FALSE,
  insert_id serial NOT NULL
)
 WITH (appendonly=true)
 DISTRIBUTED BY (name);

INSERT INTO os.ao_variables (name, value, restart) VALUES ('max_jobs', '4', false);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('oFetchSize', '2000', false);
INSERT INTO os.ao_variables (name, value, restart) VALUES ('gpfdistUrl', :gpfdisturl, true);

CREATE VIEW os.variables AS
SELECT name, value, restart
FROM    (
        SELECT name, value, restart,
               row_number() OVER (PARTITION BY name ORDER BY insert_id DESC) AS rownum, deleted
        FROM os.ao_variables
        ) AS sub
WHERE rownum = 1 AND NOT deleted;
