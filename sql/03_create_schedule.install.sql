CREATE TABLE os.ao_schedule
(description text NOT NULL,
 interval_trunc text NOT NULL,  --example: day
 interval_quantity text NOT NULL,  --example: 1 day 4 hours
 deleted boolean NOT NULL DEFAULT FALSE,
 insert_id serial NOT NULL
 ) 
 WITH (appendonly=true)
 DISTRIBUTED BY (description);

CREATE VIEW os.schedule AS
SELECT description, interval_trunc, interval_quantity
FROM    (
        SELECT description, interval_trunc, interval_quantity, row_number() OVER (PARTITION BY description ORDER BY insert_id DESC) AS rownum, deleted
        FROM os.ao_schedule
        ) AS sub
WHERE rownum = 1 AND NOT deleted;

INSERT INTO os.ao_schedule (description, interval_trunc, interval_quantity) VALUES ('Hourly', 'hour', '1 hour');
INSERT INTO os.ao_schedule (description, interval_trunc, interval_quantity) VALUES ('Daily', 'day', '1 day 4 hours');
INSERT INTO os.ao_schedule (description, interval_trunc, interval_quantity) VALUES ('Weekly', 'week', '1 week 4 hours');
INSERT INTO os.ao_schedule (description, interval_trunc, interval_quantity) VALUES ('Monthly', 'month', '1 month 4 hours');
INSERT INTO os.ao_schedule (description, interval_trunc, interval_quantity) VALUES ('5 Minutes', 'minute', '5 minutes');
