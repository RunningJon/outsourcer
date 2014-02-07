CREATE TABLE os.ao_sessions
(session_id int NOT NULL,
 expire_date timestamp NOT NULL DEFAULT current_timestamp + interval '15 minutes')
 WITH (appendonly=true)
DISTRIBUTED BY (session_id);
