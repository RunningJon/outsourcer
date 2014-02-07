CREATE EXTERNAL WEB TABLE os.osstart
(foo text) 
EXECUTE '/usr/local/os/bin/osstart' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
