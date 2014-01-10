/*
Step 6
After all jobs complete in the queue, check the tables:
select * from os.queue order by status;

*/

--append will have 10 rows where it had 5 rows at first
select * from os_demo.append_test order by id;
--replication (notice there are now 4 rows and the salaries doubled)
select * from os_demo.replication_test order by id;
