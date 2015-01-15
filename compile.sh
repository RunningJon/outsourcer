javac -cp .:jar/gpdb.jar *.java
jar cfm Outsourcer.jar manifest.txt Logger.class CommonDB.class Oracle.class SQLServer.class ExternalData.class GP.class ExternalDataD.class ExternalDataThread.class 
jar cfm OutsourcerUI.jar manifest.txt *Model.class *View.class *Control.class UI*.class ServerRunnerUI.class
jar cfm OutsourcerScheduler.jar manifest.txt AgentD.class

mv Outsourcer.jar jar/
mv OutsourcerUI.jar jar/
mv OutsourcerScheduler.jar jar/
