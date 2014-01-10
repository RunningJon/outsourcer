# make sure executing as root
if [ "`whoami`" != root ]; then
        echo "You MUST run install as root"
	echo Exiting
	exit 1
fi

# get the version number
if [ -f version.sh ]; then
	source version.sh
else
        echo "version.sh not found!"
        echo Exiting
        exit 1
fi

# make sure zip file is found
if [ ! -f $os_version.zip ]; then
	echo "Zip file not found!"
	echo Exiting
	exit 1
fi

# remove this version if it already exists
rm -rf /usr/local/$os_version

# remove old symbolic link
rm /usr/local/os

# unzip new version
unzip os.zip -d /usr/local/$os_version/

# create new symbolic link
ln -s /usr/local/$os_version /usr/local/os 

# change ownership to gpadmin
chown -R gpadmin /usr/local/$os_version

# set permissions
chmod 755 /usr/local/os/bin/*
chmod 755 /usr/local/os/os_install.sh
chmod 755 /usr/local/os/os_path.sh

# Jar files
echo Installation of Outsourcer version $os_version is complete!
echo Next Steps
echo "1.  Download Oracle JDBC driver from http://download.oracle.com/otn/utilities_drivers/jdbc/11203/ojdbc6.jar"
echo
echo "2.  Download Microsoft SQL Server JDBC driver from http://www.microsoft.com/download/en/details.aspx?displaylang=en&id=21599"
echo
echo 3.  Put the ojdbc.jar file and the sqljdbc4.jar file in /usr/local/os/jar
echo
echo 4.  Change login to gpadmin
echo
echo 5.  Edit your .bashrc or .bash_profile and add:
echo
echo     source /usr/local/os/os_path.sh
echo
echo Note: Be sure to set PGPORT and PGDATABASE environment variables 
echo Example:
echo     export PGDATABASE=gpdb
echo     export PGPORT=5432
echo
echo Note 2: For the UI security, export AUTHSERVER for the IP Address/Name of the public facing network.
echo
echo Example:
echo     export AUTHSERVER=10.1.1.2
echo
echo 6.  Source the update .bashrc
echo     source ~/.bashrc
echo
echo 7. Install the database components
echo
echo     /usr/local/os/os_install.sh
echo
echo 8.  Start Outsourcer UI with uistart and stop Outsourcer UI, run uistop
echo 
echo 9.  Visit http://mdw:8080 for Outsourcer 
