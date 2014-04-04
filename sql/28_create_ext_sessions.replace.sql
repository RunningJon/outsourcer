DROP EXTERNAL TABLE IF EXISTS os.sessions;

CREATE EXTERNAL WEB TABLE os.sessions
(
  session_id integer,
  expire_date timestamp without time zone
)
EXECUTE 'cat /usr/local/os/log/sessions.txt' on master
FORMAT 'TEXT' (DELIMITER AS '|');
