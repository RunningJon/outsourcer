Test cases for Oracle connections. 

The examples have the source server as "jonnywin" which is the name of my Virtual Machine with
Oracle on it.  Please adjust this to your hostname and port.

In Oracle, connect with SQL*Plus like this:
	sqlplus /nolog
	connect / as sysdba


In Greenplum, connect as gpadmin


Cleanup in Oracle can be done by using this:
	DROP USER os_test CASCADE;
