import java.sql.*;
import java.net.*;
import java.io.*;

public class ExternalDataThread implements Runnable
{
	private static String myclass = "ExternalDataThread";
	public static boolean debug = true;

	private String gpServer;
	private int gpPort;
	private String gpDatabase;
	private String gpUserName;
	private int id;
	private String targetSchema;
	private String targetTable;
	private String sourceType;
	private String sourceServer;
	private String sourceInstance;
	private int sourcePort;
	private String sourceDatabase;
	private String sourceSchema;
	private String sourceTable;
	private String sourceUser;
	private String sourcePass;
	private String refreshType;
	private String columnName;
	private String sqlText;
	private boolean snapshot;
	private int queueId;
	private int numRows = 0;
	private int sqlTextNumRows = 0;
	private Connection conn;

	ExternalDataThread(String aGpServer, int aGpPort, String aGpDatabase, String aGpUserName, int aId, String aTargetSchema, String aTargetTable, String aSourceType, String aSourceServer, String aSourceInstance, int aSourcePort, String aSourceDatabase, String aSourceSchema, String aSourceTable, String aSourceUser, String aSourcePass, String aRefreshType, String aColumnName, String aSqlText, boolean aSnapshot, int aQueueId) throws Exception
	{

		gpServer = aGpServer;
		gpPort = aGpPort;
		gpDatabase = aGpDatabase;
		gpUserName = aGpUserName;
		id = aId;
		targetSchema = aTargetSchema;
		targetTable = aTargetTable;
		sourceType = aSourceType;
		sourceServer = aSourceServer;
		sourceInstance = aSourceInstance;
		sourcePort = aSourcePort;
		sourceDatabase = aSourceDatabase;
		sourceSchema = aSourceSchema;
		sourceTable = aSourceTable;
		sourceUser = aSourceUser;
		sourcePass = aSourcePass;
		refreshType = aRefreshType;
		columnName = aColumnName;
		sqlText = aSqlText;	
		snapshot = aSnapshot;
		queueId = aQueueId;

	}

	public void run() 
	{
		String method = "run";	
		int location = 1000;
		

		try
		{
			location = 2000;
			Connection conn = CommonDB.connectGP(gpServer, gpPort, gpDatabase, gpUserName);

			try 
			{
				location = 3000;
				if (debug)
					Logger.printMsg("QueueID: " + queueId + " refreshType: " + refreshType);

				location = 3100;
				boolean targetSchemaFound = false;
				boolean targetTableFound = false;
				boolean stageTableFound = false;
				boolean archTableFound = false;
				boolean sourceReplTablesFound = false;

				int maxGPId = -1;
				int maxSourceId = -1;

				location = 3200;
				if (!(refreshType.equals("transform")))
				{
					location = 3250;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check for source objects");
					CommonDB.checkSourceObjects(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, columnName, refreshType);
				}

				location = 3300;
				if (refreshType.equals("refresh"))
				{
					location = 4100;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check for target schema");
					//Create target schema if it doesn't exist
					targetSchemaFound = GP.createSchema(conn, targetSchema);

					location = 4200;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " create target table if needed");
					//Create target table if it doesn't exist based on DDL from external source
					targetTableFound = GP.createTargetTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass);

					location = 4250;
					//drop external web table
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " drop external web table if exists");
					GP.dropExternalWebTable(conn, targetSchema, targetTable);

					location = 4300;
					//Create the external table 
					//hard coded the column name and max id for refresh because it is ignored
					columnName = "column";
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " creating external table");
					GP.createExternalTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, refreshType, columnName, maxGPId, gpDatabase, gpPort, queueId);

					location = 4400;
					//Truncate the target table if refresh
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " truncate target table");
					GP.truncateTable(conn, targetSchema, targetTable);

					location = 4500;
					//Insert into target table, selecting from external table	
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " insert");
					numRows = GP.insertTargetTable(conn, targetSchema, targetTable);

					location = 4600;
					//drop external web table
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " drop external web table if exists");
					GP.dropExternalWebTable(conn, targetSchema, targetTable);

					location = 4700;
					//Analyze target table
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " analyze target table");
					GP.analyzeTargetTable(conn, targetSchema, targetTable);

					location = 4800;
					//execute transform sql if any
					sqlTextNumRows = GP.executeSQL(conn, sqlText);

				} else if (refreshType.equals("append"))
				{
					location = 5100;
					//Create target schema if it doesn't exist
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check for target schema");
					targetSchemaFound = GP.createSchema(conn, targetSchema);

					location = 5200;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " create target table");
					//Create target table if it doesn't exist based on DDL from external source
					targetTableFound = GP.createTargetTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass);

					location = 5300;
					//Get the Max ID from GP
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " get max id from GP");
					maxGPId = GP.getMax(conn, targetSchema, targetTable, columnName);

					location = 5400;
					//Get the Max ID from Source
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " get max id from source");
					maxSourceId = CommonDB.getMaxId(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, columnName);

					location = 5500;	
					if (maxGPId != maxSourceId)
					{
						location = 5600;
						if (maxGPId == -1)
						{
							location = 5700;	
							//GP has no data in it so change to a refresh
							refreshType = "refresh";
						}
					
						location = 5750;
						//drop external web table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " drop external table");
						GP.dropExternalWebTable(conn, targetSchema, targetTable);

						location = 5800;	
						//Create the external table 
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " create external table");
						GP.createExternalTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, refreshType, columnName, maxGPId, gpDatabase, gpPort, queueId);

						location = 5900;	
						//Insert into target table, selecting from external table	
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " insert");
						numRows = GP.insertTargetTable(conn, targetSchema, targetTable);

						location = 5950;
						//drop external web table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " drop external table");
						GP.dropExternalWebTable(conn, targetSchema, targetTable);

						location = 6000;
						if (refreshType.equals("refresh"))
						{
							location = 6100;
							//Analyze target table
							if (debug)
								Logger.printMsg("QueueID: " + queueId + " analyze table");
							GP.analyzeTargetTable(conn, targetSchema, targetTable);
						}
					}

					location = 6200;
					//execute transform sql if any
					sqlTextNumRows = GP.executeSQL(conn, sqlText);

				} else if (refreshType.equals("replication"))
				{
					location = 7100;
					//Create target schema if it doesn't exist
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check for target schema");
					targetSchemaFound = GP.createSchema(conn, targetSchema);

					location = 7200;
					//Create target table if it doesn't exist based on DDL from external source
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " create target table");
					targetTableFound = GP.createTargetTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass);

					location = 7300;	
					//check to see if stage table exists
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check stage table");
					stageTableFound = GP.checkStageTable(conn, targetSchema, targetTable);

					location = 7400;	
					//check to see if arch table exists
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check archive table");
					archTableFound = GP.checkArchTable(conn, targetSchema, targetTable);

					location = 7500;
					//check to see if source repl tables exist
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check source replication objects");
					sourceReplTablesFound = CommonDB.checkSourceReplObjects(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, columnName);
					
					if (debug)
					{
						Logger.printMsg("targetSchemaFound: " + targetSchemaFound);
						Logger.printMsg("targetTableFound: " + targetTableFound);
						Logger.printMsg("stageTableFound: " + stageTableFound);
						Logger.printMsg("archTableFound: " + archTableFound);
						Logger.printMsg("sourceReplTablesFound: " + sourceReplTablesFound);
						Logger.printMsg("snapshot: " + snapshot);
					}

					if ((!(targetSchemaFound)) || (!(targetTableFound)) || (!(stageTableFound)) || (!(archTableFound)) || (!(sourceReplTablesFound)) || (snapshot))
					{
						location = 7600;
						refreshType = "refresh";
				
						location = 7700;	
						//Create stage and archive tables for replication
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " create stage and archive tables");
						GP.setupReplicationTables(conn, targetSchema, targetTable, columnName);
				
						location = 7800;	
						//Create _OS table and triggers in the source database
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " create triggers and log table in source");
						CommonDB.configureReplication(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, columnName);

						location = 7900;
						//Truncate the target table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " truncate target table");
						GP.truncateTable(conn, targetSchema, targetTable);

						location = 7950;
						//drop external web table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " drop external table");
						GP.dropExternalWebTable(conn, targetSchema, targetTable);

						location = 8000;
						//Create the external table 
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " create external table");
						GP.createExternalTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, refreshType, columnName, maxGPId, gpDatabase, gpPort, queueId);

						location = 8100;
						//Insert into target table, selecting from external table	
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " insert");
						numRows = GP.insertTargetTable(conn, targetSchema, targetTable);

						location = 8150;
						//drop external web table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " drop external table");
						GP.dropExternalWebTable(conn, targetSchema, targetTable);

						location = 8200;
						//Analyze target table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " analyze table");
						GP.analyzeTargetTable(conn, targetSchema, targetTable);
					}
					else 
					//not a snapshot so get new data from trigger table and load it to GP to be applid by the function
					{
						location = 8300;
						//external table function knows about refresh and append options
						refreshType = "append";

						location = 8400;
						//Get the Max ID from GP archive table
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " get archive table max");
						maxGPId = GP.getArchMax(conn, targetSchema, targetTable, columnName);

						if (debug)
							Logger.printMsg("QueueID: " + queueId + " maxGPId: " + maxGPId);

						location = 8500;
						//Get the Max ID from Source
						if (debug)
							Logger.printMsg("QueueID: " + queueId + " get max id from source");
						maxSourceId = CommonDB.getReplMaxId(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, columnName);

						if (debug)
							Logger.printMsg("QueueID: " + queueId + " maxSourceId: " + maxSourceId);
						if ((maxGPId != maxSourceId) && maxSourceId != -1)
						{
							location = 8550;
							//drop repl external web table
							if (debug)
								Logger.printMsg("QueueID: " + queueId + " drop external table");
							GP.dropExternalReplWebTable(conn, sourceType, targetSchema, targetTable, sourceTable);

							location = 8600;
							//Create the external table for replication
							if (debug)
								Logger.printMsg("QueueID: " + queueId + " create external table");
							GP.createReplExternalTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, refreshType, columnName, maxGPId, gpDatabase, gpPort, queueId);

							location = 8700;
							//Insert into target table, selecting from external table
							//method also truncates stage table first
							if (debug)
								Logger.printMsg("QueueID: " + queueId + " insert");
							numRows = GP.insertReplTable(conn, targetSchema, targetTable);
						
							location = 8750;
							//drop repl external web table
							if (debug)
								Logger.printMsg("QueueID: " + queueId + " drop external table");
							GP.dropExternalReplWebTable(conn, sourceType, targetSchema, targetTable, sourceTable);

							//if any row inserted from triggers, apply the changes in GP with the function
							if (numRows > 0)
							{
								location = 8800;
								if (debug)
									Logger.printMsg("QueueID: " + queueId + " apply changes");
								GP.executeReplication(conn, targetSchema, targetTable, columnName);
							}
						}
					}	

				location = 8900;
				//execute transform sql if any
				sqlTextNumRows = GP.executeSQL(conn, sqlText);
	
				} else if (refreshType.equals("transform"))
				{
					location = 9000;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " Transform SQL:" + sqlText);

					numRows = GP.executeSQL(conn, sqlText);
				} else if (refreshType.equals("ddl"))
				{
					location = 10000;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " Transform SQL:" + sqlText);
					
					location = 10100;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " check for target schema");
					//Create target schema if it doesn't exist
					targetSchemaFound = GP.createSchema(conn, targetSchema);

					location = 10200;
					if (debug)
						Logger.printMsg("QueueID: " + queueId + " create target table if needed");
					//Create target table if it doesn't exist based on DDL from external source
					targetTableFound = GP.createTargetTable(conn, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass);
				
				} else  
					Logger.printMsg("QueueID: " + queueId + " Unknown refreshType:" + refreshType);

				location = 11000;
				//Update status to success	
				GP.updateStatus(conn, queueId, "success", numRows, "");

				Logger.printMsg("success....");

			}
			catch (SQLException exec)
			{
				Logger.printMsg("QueueID: " + queueId + " failed....");
				String errorMessage = exec.getMessage();
				errorMessage = GP.setErrorMessage(errorMessage);

				GP.updateStatus(conn, queueId, "failed", 0, myclass + ":" + method + ":" + location + ":" + errorMessage);
			}
			finally
			{
				conn.close();
			}
		}
		catch (Exception e)
		{
			Logger.printMsg("Thread died!");
			e.printStackTrace();
		}
	}
}
