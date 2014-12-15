DROP EXTERNAL TABLE IF EXISTS os.uistop;

CREATE EXTERNAL WEB TABLE os.uistop
(foo text) 
EXECUTE '/usr/local/os/bin/uistop' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
