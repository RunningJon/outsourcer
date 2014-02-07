CREATE EXTERNAL WEB TABLE os.osstatus
(status text) 
EXECUTE '/usr/local/os/bin/osstatus' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
