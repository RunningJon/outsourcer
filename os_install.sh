#!/bin/bash
##################################################################
#
# Outsourcer Installer
#
##################################################################
set -e
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/os_path.sh
configFile=$OSHOME/config.properties
myHost=`hostname`
installLog=$OSHOME/log/install.log
installSQLLog=$OSHOME/log/install_sql.log

clear
exec > >(tee $installLog)

echo "##############################################################################################"
echo "Outsourcer installer"
echo "http://pivotalguru.com"
echo "##############################################################################################"
echo ""
d=`date`
echo ""
echo "Installation started at: $d"
echo "Installation started at: $d" > $installSQLLog
echo ""
echo "##############################################################################################"
echo "Reading the default ports from $OSHOME/os_path.sh"
echo "User Interface (UIPORT): $UIPORT"
echo "gpfdist ports:"
echo "	OSPORT: $OSPORT"
echo "	Note: Do not overlap with OSPORT or CUSTOM port ranges!!"
echo "	OSPORT_LOWER: $OSPORT_LOWER"
echo "	OSPORT_UPPER: $OSPORT_UPPER"
echo "	Note: Do not overlap with OSPORT or JOB port ranges!!"
echo "	OSPORT_CUSTOM_LOWER: $OSPORT_CUSTOM_LOWER"
echo "	OSPORT_CUSTOM_UPPER: $OSPORT_CUSTOM_UPPER"
echo ""
echo "If either of these ports are not acceptable, cancel the installer.  Next, edit"
echo "$OSHOME/os_path.sh then re-run the installer."
echo ""
echo "If you need to change these values after installation, be sure to re-run this installer."
echo "##############################################################################################"
echo ""
read -p "Press [Enter] to start continue or ctrl+c to cancel..."
echo ""

echo "##############################################################################################"
echo "Checking prerequisites"
echo "##############################################################################################"
echo ""
echo "Checking for psql..."
gpclient=`psql --version 2> /dev/null | grep PostgreSQL | grep 8.2 | wc -l`
	if [ $gpclient -eq 0 ]; then
		echo "psql not found.  Please install the Greenplum / HAWQ client and try again."
		echo "##############################################################################################"
		echo "Installation failed!"
		echo "##############################################################################################"
		exit 1
	else
		echo "Found!"
	fi
echo "Checking for gpfdist..."
gpfdistclient=`gpfdist --version 2> /dev/null | wc -l`
	if [ $gpfdistclient -eq 0 ]; then
		echo "gpfdist not found.  Please install the Greenplum / HAWQ gpfdist utility and try again."
		echo "##############################################################################################"
		echo "Installation failed!"
		echo "##############################################################################################"
		exit 1
	else
		echo "Found!"
	fi
echo ""
if [ ! -f $OJAR ]; then
	echo "##############################################################################################"
	echo "Oracle JDBC driver is missing."
	echo "- Download ojdbc6.jar from "
	echo "  http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html"
	echo "- Place the ojdbc6.jar file in $OSHOME/jar directory"
	echo "- Restart Outsourcer"
	echo "Note: You can rename the jar file but be sure to set this value with OJAR in:"
	echo "$OSHOME/os_path.sh"
	echo "##############################################################################################"
	echo ""
	echo "##############################################################################################"
	echo "Installation failed!"
	echo "##############################################################################################"
	exit 1
fi

if [ ! -f $MSJAR ]; then
	echo "##############################################################################################"
	echo "Microsoft SQL Server JDBC driver is missing."
	echo "##############################################################################################"
	p=`ping -c 1 -W 1 download.microsoft.com 2>&1 | grep transmitted | awk -F ',' '{ print $2 }' | awk -F ' ' '{ print $1 }'`
	if [ $p -eq 1 ]; then
		echo "Downloading Microsoft SQL Server JDBC Driver"
		curl -L 'http://download.microsoft.com/download/0/2/A/02AAE597-3865-456C-AE7F-613F99F850A8/sqljdbc_4.0.2206.100_enu.tar.gz' | tar xz
		mv sqljdbc_4.0/enu/sqljdbc4.jar $OSHOME/jar/
		rm -r sqljdbc_4.0
		echo ""
		echo "Succesfully installed Microsoft SQL Server JDBC driver."
	else

		echo "##############################################################################################"
		echo "Unable to connect to download.microsoft.com.  Please download the driver from"
		echo "http://download.microsoft.com/download/0/2/A/02AAE597-3865-456C-AE7F-613F99F850A8/sqljdbc_4.0.2206.100_enu.tar.gz"
		echo "After downloading, gunzip and extract with tar"
		echo "Next, mv sqljdbc_4.0/enu/sqljdbc4.jar to $OSHOME/jar"
		echo "Note: You can rename the jar file but be sure to set this value with MSJAR in:"
		echo "$OSHOME/os_path.sh"
		echo "##############################################################################################"
		echo ""
		echo "##############################################################################################"
		echo "Installation failed!"
		echo "##############################################################################################"
		exit 1
	fi
fi

echo "osport=$OSPORT" > $configFile
echo ""
echo "##############################################################################################"
echo "What is the name of THIS host?"
echo "Note: This name MUST be accessible by ALL nodes on the private network via this name!"
echo "Hit [enter] for the default of $myHost."
echo "##############################################################################################"
read osserver
if [ "$osserver" = "" ]; then
	osserver=$myHost
fi
echo "osserver=$osserver" >> $configFile
echo "osserver=$osserver"
while [ -z $gpserver ]; do
	echo "##############################################################################################"
	echo "What is the name of the MASTER server running Greenplum or HAWQ?"
	echo "##############################################################################################"
	read gpserver
	if [ "$gpserver" = "" ]; then
		echo "Please provide a value."
		echo ""
	fi
done
echo "gpserver=$gpserver" >> $configFile
echo "gpserver=$gpserver"
echo "##############################################################################################"
echo "Which database do you want to install Outsourcer?"
echo "Hit [enter] for the default of gpadmin"
echo "##############################################################################################"
read gpdatabase
if [ "$gpdatabase" = "" ]; then
	gpdatabase=gpadmin
fi

echo "gpdatabase=$gpdatabase" >> $configFile
echo "gpdatabase=$gpdatabase"

if [ "$gpdatabase" != "" ]; then
	export PGDATABASE=$gpdatabase
fi

echo "##############################################################################################"
echo "What is the port number of the MASTER server running Greenplum or HAWQ?"
echo "Hit [enter] for the default of 5432."
echo "##############################################################################################"
read gpport
if [ "$gpport" = "" ]; then
	gpport=5432
fi
echo "gpport=$gpport" >> $configFile
echo "gpport=$gpport"
echo "##############################################################################################"
echo "What is the username that Outsourcer will use to connect?  This needs to be a SUPERUSER."
echo "Hit [enter] for the default of gpadmin"
echo "##############################################################################################"
read gpusername
if [ "$gpusername" = "" ]; then
	gpusername=gpadmin
fi
echo "gpusername=$gpusername" >> $configFile
echo "gpusername=$gpusername"
while [ -z $gppassword ]; do
	echo "##############################################################################################"
	echo "What is the database password (not the operating system password) for $gpusername?"
	echo "##############################################################################################"
	read gppassword
	if [ "$gppassword" = "" ]; then
		echo "Please provide a value."
		echo ""
	fi
done
echo "gppassword=$gppassword" >> $configFile
echo "gppassword=********"
echo "##############################################################################################"
echo "Set the permissions on the config.properties file to 600"
echo "##############################################################################################"
chmod 600 $configFile
echo ""
echo "##############################################################################################"
echo "Create/edit .pgpass file so psql can execute with a password"
echo "##############################################################################################"
echo "$gpserver:$gpport:$gpdatabase:$gpusername:$gppassword" > ~/.pgpass_os

if [ -f ~/.pgpass ]; then
	echo "Making new .pgpass file"
	i=0
	while [ -z $pgpass_backup ]; do
		i=`expr $i + 1`
		if [ ! -f ~/.pgpass_backup_$i ]; then
			pgpass_backup=.pgpass_backup_$i
		fi
	done
	echo "Backing up old .pgpass file to $pgpass_backup"
	mv ~/.pgpass ~/$pgpass_backup
	echo "Updating the .pgpass file"
fi
mv ~/.pgpass_os ~/.pgpass
chmod 600 ~/.pgpass

echo ""
echo "##############################################################################################"
echo "Update the .bash_profile file"
echo "##############################################################################################"
if [ -f ~/.bash_profile ]; then
	echo "Making new .bash_profile file"
	grep -v PGPORT ~/.bash_profile | grep -v os_path | grep -v PGDATABASE > ~/.bash_profile_os
	i=0
	while [ -z $bash_profile_backup ]; do
		i=`expr $i + 1`
		if [ ! -f ~/.bash_profile_backup_$i ]; then
			bash_profile_backup=.bash_profile_backup_$i
		fi
	done
	echo "Backing up old .bash_profile file to $bash_profile_backup"
	mv ~/.bash_profile ~/$bash_profile_backup
	echo "Updating the .bash_profile file"
	mv ~/.bash_profile_os ~/.bash_profile
fi
echo ""
echo "##############################################################################################"
echo "Update the .bashrc file"
echo "##############################################################################################"
echo "Making new .bashrc file"
grep -v os_path.sh ~/.bashrc > ~/.bashrc_os
echo "Adding source to os_path.sh to your .bashrc file"
echo "source $OSHOME/os_path.sh" >> ~/.bashrc_os
i=0
while [ -z $bashrc_backup ]; do
        i=`expr $i + 1`
        if [ ! -f ~/.bashrc_backup_$i ]; then
                bashrc_backup=.bashrc_backup_$i
        fi
done
echo "Backing up old .bashrc file to $bashrc_backup"
mv ~/.bashrc ~/$bashrc_backup
echo "Updating the .bashrc file"
mv ~/.bashrc_os ~/.bashrc
echo ""
echo "##############################################################################################"
echo "Validate database connection"
echo "##############################################################################################"
t=`psql -A -t -c "SELECT version()" -U $gpusername -d $gpdatabase -h $gpserver 2> /dev/null | wc -l`
if [ $t -eq 1 ]; then
	echo "Database connection test passed!"
	echo ""
else
	echo ""
	echo "Database connection failed!"
	echo ""
	echo "##############################################################################################"
	echo "Be sure to allow external connections to connect to Greenplum/HAWQ that requires a password."
	echo "If needed, edit your \$MASTER_DATA_DIRECTORY/pg_hba.conf file on the MASTER host of "
	echo "Greenplum/HAWQ.  Add the following line to the bottom of the file:"
	echo "host     all         all             0.0.0.0/0             md5"
	echo ""
	echo "Next, execute this so the updated pg_hba.conf file will used:"
	echo "gpstop -u"
	echo ""
	echo "This will allow external connections to all databases but require a password to be provided."
	echo ""
	echo "Note: this password is NOT the operating system password.  By default, Greenplum and HAWQ"
	echo "do not have a password set for the gpadmin account because it uses local \"trust\""
	echo "authentication."
	echo ""
	echo "You can set the password for the database account with the following command:"
	echo ""
	echo "EXAMPLE: ALTER USER gpadmin PASSWORD 'changeme';"
	echo "##############################################################################################"
	echo ""
	echo "Verify this connection information and try again:"
	echo "Host: $gpserver"
	echo "Database: $gpdatabase"
	echo "Port: $gpport"
	echo "Username: $gpusername"
	echo "Password: $gppassword"
	echo ""
	echo "##############################################################################################"
	echo "Installation failed!"
	echo "##############################################################################################"
	echo ""
	exit 1
fi
echo ""
echo "##############################################################################################"
echo "Validate network connectivity between nodes and this host"
echo "##############################################################################################"

cd $OSHOME/sql

psql -c "DROP EXTERNAL TABLE IF EXISTS os_installer_test" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
psql -f 00_os_installer_test.sql -v EXECUTE="'ping -c 1 -W 1 $osserver 2>&1 | grep transmitted | awk -F '','' ''{ print \$2 }'' | awk -F '' '' ''{ print \$1 }''' " -U $gpusername -d $gpdatabase -h $gpserver -p $gpport 

t=`psql -A -t -c "SELECT SUM(foo)/COUNT(*) FROM os_installer_test" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport`
if [ $t -gt 0 ]; then
	echo "Network connectivity test passed!"
	echo ""
else
	echo "This host is not accessible by the segment hosts / data nodes."
	echo "Hostname: $osserver"
	echo "##############################################################################################"
	echo "Installation failed!"
	echo "##############################################################################################"
	exit 1
fi
psql -c "DROP EXTERNAL TABLE IF EXISTS os_installer_test" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
echo ""
echo "##############################################################################################"
echo "Install database components"
echo "##############################################################################################"
os_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'os'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)

if [ $os_exists = 1 ]; then
	echo "Notice: os schema already exists"
	os_type_target_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_type t JOIN pg_namespace n on t.typnamespace = n.oid WHERE t.typname = 'type_target' and n.nspname = 'os'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)
	if [ $os_type_target_exists -eq 1 ]; then
		os_create_tables=1
		echo "Notice: found version 3 of Outsourcer in os schema" 
		i=0
		while [ -z $os_backup_schema ]
		do
		i=`expr $i + 1`
		check=$(psql -t -A -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'os_backup_$i'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)

		if [ $check = 0 ]; then
			os_backup_schema=os_backup_$i
			echo "Notice: backup schema is $os_backup_schema"
			psql -t -A -c "ALTER SCHEMA OS RENAME TO $os_backup_schema" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport 
		fi
		done
	else 
		os_create_tables=0
	fi
else
	os_create_tables=1
	echo "Notice: new install of Outsourcer"
fi

ext_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'ext'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)

if [ $ext_exists = 0 ]; then
	echo "Notice: creating ext schema"
	for i in $( ls *.install_ext_check.sql ); do
		psql -f $i -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
	done
else
	echo "Notice: ext schema already exists"
fi

if [ $os_create_tables = 1 ]; then
	#install the sql files
	echo "Notice: creating tables in the os schema"
	for i in $( ls *.install.sql ); do
		psql -f $i -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
	done
else
	echo "Notice: os schema already exists so skipping table creation"
fi

echo "Notice: create external table function create/replace"
for i in $( ls *.variables.sql ); do
	psql -f $i -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
done

echo "Notice: creating or replacing objects" 
for i in $( ls *.replace.sql ); do
	psql -f $i -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
done

echo "Notice: creating or replacing external tables that use gpfdist"
for i in $( ls *.gpfdist.sql ); do
	c=`echo $i | awk -F '_' '{print $3}' | awk -F '.' '{print $1}'`
	psql -f $i -v LOCATION="'gpfdist://$osserver:$OSPORT/foo#transform=$c'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
done

if [ "$os_backup_schema" != "" ]; then

	os_schedule_desc_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid JOIN pg_attribute a on a.attrelid = c.oid WHERE n.nspname = '$os_backup_schema' AND c.relname = 'job' AND a.attname = 'schedule_desc'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)
	if [ $os_schedule_desc_exists = 1 ]; then
		echo "Notice: migrating jobs from 4.x"
		for i in $( ls *.new_job.upgrade.sql ); do
			psql -f $i -v os_backup=$os_backup_schema -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
		done
	else
		echo "Notice: migrating jobs from 3.x" 
		for i in $( ls *.old_job.upgrade.sql ); do
			psql -f $i -v os_backup=$os_backup_schema -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
		done
	fi

	echo "Notice: Migrating queue and ext_connection tables from $os_backup_schema to os"
	for i in $( ls *.remaining.upgrade.sql ); do
		psql -f $i -v os_backup=$os_backup_schema -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1
	done
fi

#New Outsourcer 5 custom objects
os_custom_sql_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid WHERE n.nspname = 'os' AND c.relname = 'ao_custom_sql'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)
if [ $os_custom_sql_exists -eq 0 ]; then
	echo "Notice: Installing Version 5 custom_sql table"
	for i in $( ls *.install_os5.sql ); do
		psql -f $i -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1 
	done
fi

#Make sure custom_sql has gpfdist_port
os_custom_sql_gpfdist_check=$(psql -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid JOIN pg_attribute a on c.oid = a.attrelid WHERE n.nspname = 'os' AND c.relname = 'ao_custom_sql' AND a.attname = 'gpfdist_port'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport)
if [ $os_custom_sql_gpfdist_check -eq 0 ]; then
	echo "Notice: Adding gpfdist_port column to custom_sql table"
	for i in $( ls *.fix_os5.sql ); do
		psql -f $i -v osport_custom_lower=$OSPORT_CUSTOM_LOWER -U $gpusername -d $gpdatabase -h $gpserver -p $gpport >> $installSQLLog 2>&1 
	done
fi

echo "Notice: making sure all sequences have a cache of 1"
psql -t -A -c "SELECT 'ALTER SEQUENCE ' || n.nspname || '.' || c.relname || ' CACHE 1;' FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = 'os' AND c.relkind = 'S'" -U $gpusername -d $gpdatabase -h $gpserver -p $gpport | psql -e -U $gpusername -d $gpdatabase -h $gpserver -p $gpport 

cd $OSHOME

echo ""
echo "##############################################################################################"
echo "Start Outsourcer"
echo "##############################################################################################"
echo "Run stop_all"
source $PWD/os_path.sh; stop_all
echo "Run start_all"
source $PWD/os_path.sh; start_all
echo ""
echo "##############################################################################################"
echo "Basic commands:"
echo "##############################################################################################"
echo "start_all / stop_all"
echo "Starts and stops all background processes for Outsourcer."
echo ""
echo "gpfdistart / gpfdiststop"
echo "Starts and stops the REQUIRED gpfdist process.  This is used for getting external data as well"
echo "as commands within the user interface.  This must be running at all times.  You can change the"
echo "port number gpfdist runs on in $PWD/os_path.sh"
echo "and then re-run this installer."
echo ""
echo "uistart / usstop"
echo "Starts and stops the Outsourcer User Interface."
echo "Note: gpfdist must be running for the User Interface to work."
echo ""
echo "osstart / osstop"
echo "Starts and stops the Queue Daemon."
echo "This is a background process that watches the os.queue table for jobs to execute."
echo "Note: gpfdist must be running to load data."
echo ""
echo "agentstart / agentstop"
echo "Starts and stops the Scheduler Daemon."
echo "This is a background process that will put jobs into the os.queue table to be executed based"
echo "on the schedule set for the job."
echo "Note: gpfdist must be running to load data."
echo ""
echo "Database configuration details in $configFile"
echo "Environment variables are stored in $PWD/os_path.sh"
echo "Note: You may manually edit these configuration files after installation but it requires"
echo "restarting the services."
echo "##############################################################################################"
echo ""
echo "##############################################################################################"
echo "http://$osserver:$UIPORT for Outsourcer"
echo "##############################################################################################"
echo ""
echo "Be sure to source your .bashrc file after installation or re-login."
echo ""
