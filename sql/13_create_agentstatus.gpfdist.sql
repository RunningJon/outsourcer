DROP EXTERNAL TABLE IF EXISTS os.agentstatus;

CREATE EXTERNAL TABLE os.agentstatus
(status text)
LOCATION (:LOCATION) FORMAT 'text' (DELIMITER '|');
