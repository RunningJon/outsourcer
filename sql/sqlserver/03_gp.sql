/*
Step 3
After all jobs complete in the queue, check the tables:
select * from os.queue order by status;

*/
--refresh
select * from os_demo.sql_refresh;
--append (notice there are 5 rows)
select * from os_demo.sql_append order by id;
--replication (notice there are 5 rows and the salaries)
select * from os_demo.sql_replication order by id;
