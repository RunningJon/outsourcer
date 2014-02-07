CREATE EXTERNAL WEB TABLE os.agentstop
(foo text)
EXECUTE '/usr/local/os/bin/agentstop' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
