DROP EXTERNAL TABLE IF EXISTS os.uistart;

CREATE EXTERNAL WEB TABLE os.uistart
(foo text) 
EXECUTE '/usr/local/os/bin/uistart' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
