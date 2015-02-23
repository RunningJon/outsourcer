README file for Outsourcer
Note: BETA version that isn't ready yet.
***********************************************************************************
Site: http://www.PivotalGuru.com
Author: Jon Roberts
Email: jgronline@gmail.com
***********************************************************************************
Outsourcer automates the tasks typically done manually by ETL developers to source 
data from SQL Server and Oracle and load into Greenplum or HAWQ database.  

* Connects to Source databases (SQL Server or Oracle)
* Converts Source DDL to Greenplum/HAWQ syntax and creates target schema and table
* Loads data into Greenplum/HAWQ automatically
* Supports multiple job types:
	* Refresh (truncate and reload)
	* Append (add only new data from source)
	* Replication (tracks changes in the source and applies the changes)
	* Transform (SQL commands to execute in Greenplum/HAWQ)
* Multi-threaded with dynamic number of jobs to execute concurrently
* Scheduler for recurring job execution
* Open Source!
* Automatic data cleansing
* Utilizes gpfdist for fast data loading
