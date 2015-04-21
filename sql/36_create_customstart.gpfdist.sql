DROP EXTERNAL TABLE IF EXISTS os.customstart;

CREATE EXTERNAL TABLE os.customstart
(gpfdist_port int)
LOCATION (:LOCATION) FORMAT 'text' (DELIMITER '|');

