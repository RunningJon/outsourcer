Test cases for SQLServer connections. 

The examples have the source server as "jonnywin" which is the name of my Virtual Machine with
SQL Server on it.  Please adjust this to your hostname.

In SQLServer, connect with a DBA account like sa to execute scripts.

In Greenplum, connect as gpadmin

Cleanup in SQL Server can be done by using this:

use master
go
drop login os_test;
go
drop database os_demo;
go

