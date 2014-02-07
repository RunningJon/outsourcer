CREATE EXTERNAL WEB TABLE os.agentstatus
(status text)
EXECUTE '/usr/local/os/bin/agentstatus' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
