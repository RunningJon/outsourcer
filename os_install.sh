#!/bin/bash
source ~/.bashrc
##################################################################
# Outsourcer Installer
##################################################################

##################################################################
# Make sure the greenplum_path has been sourced.
##################################################################
if [ -z $MASTER_DATA_DIRECTORY ]; then
	echo "Did you source the greenplum_path?  MASTER_DATA_DIRECTORY not found!"
	echo Exiting
	exit 0
fi

##################################################################
# Check to see if running a Mac or Linux
##################################################################
if [[ "$(uname)" = "Darwin" ]]; then
	os=mac
else
	os=linux
fi

##################################################################
# Get the IP address clients use
##################################################################
echo "Which IP address do EXTERNAL clients use?"
PS3="Type a number or any other key to exit:"
if [ $os = mac ]; then
	ip_list=$(/sbin/ifconfig | grep "inet " | awk '{print $2}' | grep -v 127.0.0.1)
else
	ip_list=$(/sbin/ifconfig | grep "inet addr" | cut -d: -f2 | awk '{print $1}' | grep -v 127.0.0.1)
fi
 
select ip in $ip_list; do
	break
done

##################################################################
# Set the PGPORT environment variable
##################################################################
echo "What is the port for Greenplum or HAWQ?  This will set the PGPORT value in your .bashrc file.  Default is [5432]"
read pgport
if [ "$pgport" = "" ]; then
	pgport=5432
fi

##################################################################
# Set the PGDATABASE environment variable
##################################################################
while [ -z $pgdatabase ]; do
	echo "Which database do you want to install Outsourcer?  This will set the PGDATABASE value in your .bashrc file."
	read pgdatabase
	if [ "$pgdatabase" = "" ]; then
        	echo "Please provide a value."
	fi
done

##################################################################
# Verfiy pgdatabase value
##################################################################
psql $pgdatabase -c "select version()" 2>&1 > /dev/null
if [ $? != 0 ]; then
	echo "PGDATABASE=$pgdatabase is not valid!"
	exit 1
fi

##################################################################
# Set the UIPORT environment variable
##################################################################
echo "What is an available port you want to use for the Outsourcer Web Interface? This will set the UIPORT value in your .bashrc file.  Default is [8080]"
read uiport
if [ "$uiport" = "" ]; then
	uiport=8080
fi

##################################################################
# Update the .bash_profile file
##################################################################
if [ -f ~/.bash_profile ]; then
	echo "Making new .bash_profile file"
	grep -v PGPORT ~/.bash_profile | grep -v os_path | grep -v PGDATABASE > ~/.bash_profile_os
	i=0
	while [ -z $bash_profile_backup ]; do
		i=`expr $i + 1`
		if [ ! -f ~/.bash_profile_old_$i ]; then
			bash_profile_backup=.bash_profile_old_$i
		fi
	done
	echo "Backing up old .bash_profile file to $bash_profile_backup"
	mv ~/.bash_profile ~/$bash_profile_backup
	echo "Updating the .bash_profile file"
	mv ~/.bash_profile_os ~/.bash_profile
fi

##################################################################
# Update the .bashrc file
##################################################################
echo "Making new .bashrc file"
grep -v PGPORT ~/.bashrc | grep -v os_path | grep -v PGDATABASE > ~/.bashrc_os
echo "Adding source to os_path.sh"
echo "source /usr/local/os/os_path.sh" >> ~/.bashrc_os
echo "Adding export PGDATABASE=$pgdatabase"
echo "export PGDATABASE=$pgdatabase" >> ~/.bashrc_os
echo "Adding export PGPORT=$pgport"
echo "export PGPORT=$pgport" >> ~/.bashrc_os
echo "Adding export AUTHSERVER=$ip"
echo "export AUTHSERVER=$ip" >> ~/.bashrc_os
echo "Adding export UIPORT=$uiport"
echo "export UIPORT=$uiport" >> ~/.bashrc_os
i=0
while [ -z $bashrc_backup ]; do
	i=`expr $i + 1`
	if [ ! -f ~/.bashrc_old_$i ]; then
		bashrc_backup=.bashrc_old_$i
	fi
done
echo "Backing up old .bashrc file to $bashrc_backup"
mv ~/.bashrc ~/$bashrc_backup
echo "Updating the .bashrc file"
mv ~/.bashrc_os ~/.bashrc

##################################################################
# Update the pg_hba.conf file
##################################################################
echo "Making new pg_hba.conf file"
echo "host $pgdatabase all $ip/32 md5" > $MASTER_DATA_DIRECTORY/pg_hba.conf_os
grep -v $ip $MASTER_DATA_DIRECTORY/pg_hba.conf >> $MASTER_DATA_DIRECTORY/pg_hba.conf_os
i=0
while [ -z $pg_hba_backup ]; do
	i=`expr $i + 1`
	if [ ! -f $MASTER_DATA_DIRECTORY/pg_hba.conf_old_$i ]; then
		pg_hba_backup=pg_hba.conf_old_$i
	fi
done
echo "Backing up pg_hba.conf file to $pg_hba_backup"
mv $MASTER_DATA_DIRECTORY/pg_hba.conf $MASTER_DATA_DIRECTORY/pg_hba.conf_old
echo "Updating the pg_hba.conf file"
mv $MASTER_DATA_DIRECTORY/pg_hba.conf_os $MASTER_DATA_DIRECTORY/pg_hba.conf

##################################################################
# Apply changes to pg_hba.conf file
##################################################################
echo "Applying changes to pg_hba.conf file"
gpstop -u

##################################################################
# Install database components
##################################################################
source ~/.bashrc

os_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'os'")

if [ $os_exists = 1 ]; then
	echo "Notice: os schema already exists"
	os_type_target_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_type t JOIN pg_namespace n on t.typnamespace = n.oid WHERE t.typname = 'type_target' and n.nspname = 'os'")
	if [ $os_type_target_exists == 1 ]; then
		os_create_tables=1
		echo "Notice: found earlier version of Outsourcer in os schema" 
		i=0
		while [ -z $os_backup_schema ]
		do
		i=`expr $i + 1`
		check=$(psql -t -A -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'os_backup_$i'")

		if [ $check = 0 ]; then
			os_backup_schema=os_backup_$i
			echo "Notice: backup schema is $os_backup_schema"
			psql -t -A -c "ALTER SCHEMA OS RENAME TO $os_backup_schema"
		fi
		done
	else
		os_create_tables=0
		echo "Notice: os schema doesn't have custom type so jobs will not be migrated but functions will be updated to the new installation"
	fi
else
	os_create_tables=1
	echo "Notice: new install of Outsourcer"
fi

ext_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'ext'")

if [ $ext_exists = 0 ]; then
	echo "Notice: creating ext schema"
	for i in $( ls /usr/local/os/sql/*.install_ext_check.sql ); do
		echo psql -f $i
		psql -f $i 
	done
else
	echo "Notice: ext schema already exists"
fi

if [ $os_create_tables = 1 ]; then
	#install the sql files
	echo "Notice: creating tables in the os schema"
	for i in $( ls /usr/local/os/sql/*.install.sql ); do
		echo psql -f $i 
		psql -f $i 
	done

else
	echo "Notice: os schema already exists so skipping table creation"
fi

echo "Notice: creating or replacing functions" 
for i in $( ls /usr/local/os/sql/*.replace.sql ); do
	echo psql -f $i
	psql -f $i 
done

if [ "$os_backup_schema" != "" ]; then

	os_schedule_desc_exists=$(psql -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid JOIN pg_attribute a on a.attrelid = c.oid WHERE n.nspname = '$os_backup_schema' AND c.relname = 'job' AND a.attname = 'schedule_desc'")
	if [ $os_schedule_desc_exists = 1 ]; then
		echo "Notice: migrating jobs from 4.x"
		for i in $( ls /usr/local/os/sql/*new_job.upgrade.sql ); do
			echo psql -f $i -v os_backup=$os_backup_schema
			psql -f $i -v os_backup=$os_backup_schema
		done
	else
		echo "Notice: migrating jobs from 3.x" 
		for i in $( ls /usr/local/os/sql/*old_job.upgrade.sql ); do
			echo psql -f $i -v os_backup=$os_backup_schema
			psql -f $i -v os_backup=$os_backup_schema 
		done
	fi

	echo "Notice: Migrating queue and ext_connection tables from $os_backup_schema to os"
	for i in $( ls /usr/local/os/sql/*remaining.upgrade.sql ); do
		echo psql -f $i -v os_backup=$os_backup_schema 
		psql -f $i -v os_backup=$os_backup_schema 
	done
fi

echo

if [ ! -f $MSJAR ]; then
	echo "##############################################################################################"
	echo "Microsoft SQL Server JDBC driver is missing."
	echo "Outsourcer is not allowed to redistribute this Microsoft driver."
	echo "Please download from http://www.microsoft.com/download/en/details.aspx?displaylang=en&id=21599"
	echo "Then place the sqljdbc4.jar file in /usr/local/os/jar/"
	echo "You can rename the jar file but be sure to set this value with MSJAR in your .bashrc file."
	echo "##############################################################################################"
	echo
fi

if [ ! -f $OJAR ]; then
	echo "##############################################################################################"
	echo "Oracle JDBC driver is missing."
	echo "Outsourcer is not allowed to redistribute this Oracle driver."
	echo "Please download from http://download.oracle.com/otn/utilities_drivers/jdbc/11203/ojdbc6.jar"
	echo "Then place the ojdbc6.jar file in /usr/local/os/jar/"
	echo "You can rename the jar file but be sure to set this value with OJAR in your .bashrc file"
	echo "##############################################################################################"
	echo
fi

echo "Stop request for Outsourcer UI"
uistop
sleep 1
echo "Start request for Outsourcer UI"
uistart

echo "##################################################################"
echo "Visit http://$AUTHSERVER:$UIPORT for Outsourcer"
echo "##################################################################"
echo

