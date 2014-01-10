README file for Outsourcer
*******************************************************************************************************
Site: http://www.PivotalGuru.com
Version 4.0.1
Author: Jon Roberts
Email: jgronline@gmail.com
Date: 2013-10-22
*******************************************************************************************************

History of Outsourcer
*******************************************************************************************************
Outsourcer was born out of the need to keep a Greenplum database up to date in the most efficient 
manner possible from various SQL Server and Oracle databases.  The ideal solution would NOT require 
creating files or manually scrubbing data.  It should connect natively to the source databases and load 
the data into Greenplum.

The solution should automate tasks done to load data so that new tables could be identified and added 
to Greenplum without having to code anything.

The solution should also be able to dynamically throttle the number of tables being loaded at once so 
that the source systems will still be able to respond.

The solution should provide a Change Data Capture process that is automated and optimized for 
Greenplum.

Features
*******************************************************************************************************
Outsourcer automates the tasks typically done manually by ETL developers to source data from SQL Server 
and Oracle to Greenplum.  

Outsourcer is highly efficient and optimized for Greenplum.

Outsourcer converts DDL from your source database to Greenplum DDL.

Outsourcer creates the target schema and table in Greenplum.

Outsourcer loads data into Greenplum WITHOUT CREATING FILES!

Outsourcer handles not so clean data by cleansing it and ensuring character set/code page matching to 
minimize errors and greatly reduce the time it takes to start using Greenplum.

Outsourcer not only can Refresh your data in Greenplum, but it can also Append only new data (example: 
tables used for logging), Replicate (captures changes and applies the changes for exceptionally fast 
updating of data), and Transformation routines that you may create in functions in Greenplum.

Outsourcer is a complete solution for you to Extract from Oracle and SQL Server, Load to Greenplum, 
and then Transform it in the database.  No coding, no errors, no files, no problems!

