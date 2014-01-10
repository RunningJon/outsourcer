############################################################################################################################
#Define the Greenplum environment for Outsourcer
############################################################################################################################

#Outsourcer home
OSHOME=/usr/local/os
export OSHOME

#Outsourcer log file
OSLOG=$OSHOME/log/Outsourcer.log
export OSLOG

#Outsourcer UI log file
UILOG=$OSHOME/log/OutsourcerUI.log
export UILOG

#Outsourcer Agent log file
AGENTLOG=$OSHOME/log/OutsourcerAgent.log
export AGENTLOG

#Outsourcer UI Auth Server for Web UI
#Should be set to an entry that won't use TRUST but MD5 or LDAP
if [ -z $AUTHSERVER ]; then
	export AUTHSERVER=mdw
fi

#Outsourcer UI Web Port
if [ -z $UIPORT ]; then
	export UIPORT=8080
fi

#Min memory for Outsourcer
if [ -z $XMS ]; then
	XMS=128m
	export XMS
fi

#Max memory for Outsourcer
if [ -z $XMX ]; then
	XMX=256m
	export XMX
fi

#Outsourcer Jar
if [ -z $OSJAR ]; then
	OSJAR=$OSHOME/jar/Outsourcer.jar
	export OSJAR
fi

#OutsourcerScheduler Jar
if [ -z $OSAGENTJAR ]; then
	OSAGENTJAR=$OSHOME/jar/OutsourcerScheduler.jar
	export OSAGENTJAR
fi

#OutsourcerUI Jar
if [ -z $OSUIJAR ]; then
	OSUIJAR=$OSHOME/jar/OutsourcerUI.jar
	export OSUIJAR
fi

#GPDB Jar
if [ -z $GPDBJAR ]; then
	GPDBJAR=$OSHOME/jar/gpdb.jar
	export GPDBJAR
fi

#Microsoft Jar
if [ -z $MSJAR ]; then
	MSJAR=$OSHOME/jar/sqljdbc4.jar
	export MSJAR
fi

#Oracle Jar
if [ -z $OJAR ]; then
	OJAR=$OSHOME/jar/ojdbc6.jar
	export OJAR
fi

#Nano Jar
if [ -z $NANOJAR ]; then
	NANOJAR=$OSHOME/jar/nanohttpd.jar
	export NANOJAR
fi

#Classpath for Outsourcer osstart
OSCLASSPATH=$OSJAR\:$MSJAR\:$OJAR\:$GPDBJAR
export OSCLASSPATH

#Classpath for Outsourcer uistart
OSUICLASSPATH=$OSJAR\:$OSUIJAR\:$MSJAR\:$OJAR\:$GPDBJAR\:$NANOJAR
export OSCLASSPATH

#Classpath for Outsourcer agentstart
OSAGENTCLASSPATH=$OSJAR\:$OSAGENTJAR\:$GPDBJAR
export OSAGENTCLASSPATH

#set new path
PATH=$OSHOME/bin:$PATH
export PATH
