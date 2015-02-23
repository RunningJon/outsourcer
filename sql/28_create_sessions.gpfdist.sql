DROP EXTERNAL TABLE IF EXISTS os.sessions;

CREATE EXTERNAL TABLE os.sessions
(
  session_id integer,
  expire_date timestamp without time zone
)
LOCATION (:LOCATION) FORMAT 'text' (DELIMITER '|');
