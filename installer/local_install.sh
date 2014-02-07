set -e
source version.sh

sudo rm /usr/local/os 2> /dev/null
sudo rm -rf /usr/local/$os_version 2> /dev/null
sudo mkdir /usr/local/$os_version 2> /dev/null
sudo ln -s /usr/local/$os_version /usr/local/os 2> /dev/null
sudo chown gpadmin /usr/local/$os_version 2> /dev/null

mkdir /usr/local/os/bin
cp ../bin/* /usr/local/os/bin

mkdir /usr/local/os/jar
cp ../jar/* /usr/local/os/jar

cp ../jar_extra/* /usr/local/os/jar

mkdir /usr/local/os/log

mkdir /usr/local/os/sql
cp -R ../sql/* /usr/local/os/sql

cp ../LICENSE.txt /usr/local/os
cp ../README.txt /usr/local/os/
cp ../os_install.sh /usr/local/os/
cp ../os_path.sh /usr/local/os/

# set permissions
chmod 755 /usr/local/os/bin/*
chmod 755 /usr/local/os/os_install.sh
chmod 755 /usr/local/os/os_path.sh
