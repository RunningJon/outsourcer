CREATE EXTERNAL WEB TABLE os.osstop
(foo text) 
EXECUTE '/usr/local/os/bin/osstop' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
