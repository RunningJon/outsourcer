################################################################################################
#Define the Greenplum environment for Outsourcer
################################################################################################
#Note: Outsourcer server name is set during installation.

#Outsourcer home
OSHOME=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export OSHOME

#Outsourcer UI Web Port
export UIPORT=8090

#gpfdist Ports
#If you change these ports after installation, re-run os_install.sh
export OSPORT=8999

#Range of ports used by Outsourcer jobs
#Note: Do not overlap with OSPORT or CUSTOM port ranges!!
export OSPORT_LOWER=21850
export OSPORT_UPPER=21900

#Range of ports used by Outsourcer custom tables
#Note: Do not overlap with OSPORT or JOB port ranges!!
export OSPORT_CUSTOM_LOWER=22000
export OSPORT_CUSTOM_UPPER=23000

#gpfdist max row length
#increase if you get "line too long" error message
GPFDISTMAXROW=32768

#job lock file
export JOBLOCK=/tmp/jobstart.lock

#gpfdist timeout
export GPFDISTTIMEOUT=30

#yml
export YML=$OSHOME/yml/outsourcer.yml

#Database configuration
export CONFIG=$OSHOME/config.properties

################################################################################################
#Logs
################################################################################################
#Outsourcer log file
export OSLOG=$OSHOME/log/Outsourcer.log

#Outsourcer UI log file
export UILOG=$OSHOME/log/OutsourcerUI.log

#Outsourcer Agent log file
export AGENTLOG=$OSHOME/log/OutsourcerAgent.log

#Outsourcer gpfdist log file
export GPFDISTLOG=$OSHOME/log/OutsourcerGpfdist.log

#Job gpfdist log file prefix
export JOBLOG=$OSHOME/log/job

#Custom gpfdist log file prefix
export CUSTOMLOG=$OSHOME/log/custom

#Outsourcer UI Sessions log file
export SESSIONS=$OSHOME/log/sessions.txt

################################################################################################
#Java Memory
################################################################################################
#Min memory for Outsourcer
export XMS=128m

#Max memory for Outsourcer
export XMX=256m

################################################################################################
#Jar files
################################################################################################
#Outsourcer Jar
OSJAR=$OSHOME/jar/Outsourcer.jar

#OutsourcerScheduler Jar
export OSAGENTJAR=$OSHOME/jar/OutsourcerScheduler.jar

#OutsourcerUI Jar
export OSUIJAR=$OSHOME/jar/OutsourcerUI.jar

#GPDB Jar
export GPDBJAR=$OSHOME/jar/gpdb.jar

#Microsoft Jar
export MSJAR=$OSHOME/jar/sqljdbc4.jar

#Oracle Jar
export OJAR=$OSHOME/jar/ojdbc6.jar

#Nano Jar
export NANOJAR=$OSHOME/jar/nanohttpd.jar

################################################################################################
#Paths
################################################################################################
#Classpath for Outsourcer osstart
export OSCLASSPATH=$OSJAR\:$MSJAR\:$OJAR\:$GPDBJAR

#Classpath for Outsourcer uistart
export OSUICLASSPATH=$OSJAR\:$OSUIJAR\:$MSJAR\:$OJAR\:$GPDBJAR\:$NANOJAR

#Classpath for Outsourcer agentstart
export OSAGENTCLASSPATH=$OSJAR\:$OSAGENTJAR\:$GPDBJAR

#set new path
export PATH=$OSHOME/bin:$PATH
