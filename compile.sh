#!/bin/bash
set -e

echo "Manifest-Version: 1.0" > manifest.txt
echo "Main-Class: ExternalData" >> manifest.txt
echo "Specification-Title: \"Outsourcer\"" >> manifest.txt
echo "Specification-Version: \"1.0\"" >> manifest.txt
echo "Created-By: 1.6.0_65-b14-462-11M4609" >> manifest.txt
d=`date`
echo "Build-Date: $d" >> manifest.txt

javac -cp .:jar/gpdb.jar *.java
jar cfm jar/Outsourcer.jar manifest.txt Logger.class CommonDB.class Oracle.class SQLServer.class ExternalData.class CustomSQL.class GP.class ExternalDataD.class ExternalDataThread.class OSProperties.class
jar cfm jar/OutsourcerUI.jar manifest.txt *Model.class *View.class *Control.class UI*.class ServerRunnerUI.class
jar cfm jar/OutsourcerScheduler.jar manifest.txt AgentD.class
stop_all
sleep 1
start_all
