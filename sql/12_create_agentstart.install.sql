CREATE EXTERNAL WEB TABLE os.agentstart
(foo text)
EXECUTE '/usr/local/os/bin/agentstart' ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null');
