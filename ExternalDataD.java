import java.sql.*;
import java.net.*;
import java.io.*;

public class ExternalDataD
{
	private static String myclass = "ExternalDataD";
	public static boolean debug = true;

	public static void main(String[] args) throws Exception
	{

		String method = "main";
		int location = 1000;
		String gpServer = args[0];
		int gpPort = Integer.parseInt(args[1]);
		String gpDatabase = args[2];
		String gpUserName = args[3];

		location = 2000;
		if (debug)
		{
			System.out.println("gpServer:" + gpServer);
			System.out.println("gpPort:" + gpPort);
			System.out.println("gpDatabase:" + gpDatabase);
			System.out.println("gpUsername:" + gpUserName);
		}
	
		try
		{
			location = 3000;	
			if (debug)	
				Logger.printMsg("Start to loop....");	
			loadLoop(gpServer, gpPort, gpDatabase, gpUserName);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
        }

	private static void loadLoop(String gpServer, int gpPort, String gpDatabase, String gpUserName) throws Exception
	{

		String method = "loadLoop";
		int location = 1000;

		boolean loop = true;

		while (loop)
		{
			try
			{
				location = 2000;				
				ResultSet rs; 
				Statement stmt;
				String strSQL = "";

				int id = 0;
				String targetSchema = "";
				String targetTable = "";
				String sourceType = "";
				String sourceServer = "";
				String sourceInstance = "";
				int sourcePort = 0;
				String sourceDatabase = "";
				String sourceSchema = "";
				String sourceTable = "";
				String sourceUser = "";
				String sourcePass = "";
				String refreshType = "";
				String columnName = "";
				String sqlText = "";
				boolean snapshot = false;
				int queueId = 0;

				location = 3000;
				Connection conn = CommonDB.connectGP(gpServer, gpPort, gpDatabase, gpUserName);

				location = 3100;
				strSQL = "SELECT id, LOWER((target).schema_name), LOWER((target).table_name), \n" + 
            				"	(source).type, (source).server_name, (source).instance_name, (source).port, (source).database_name, (source).schema_name, \n" +
            				"	(source).table_name, (source).user_name, (source).pass, refresh_type, column_name, \n" +
					"	sql_text, snapshot, queue_id \n" +
					"FROM os.fn_update_status()";

				location = 3200;
    				stmt = conn.createStatement();

				location = 3300;
    				rs = stmt.executeQuery(strSQL);
				
				//query only returns one record
				while (rs.next())                        
				{
					location = 4000;
					id  = rs.getInt(1);
					targetSchema = rs.getString(2);
					targetTable = rs.getString(3);
					sourceType = rs.getString(4);
					sourceServer = rs.getString(5);
					sourceInstance = rs.getString(6);
					sourcePort = rs.getInt(7);
					sourceDatabase = rs.getString(8);
					sourceSchema = rs.getString(9);
					sourceTable = rs.getString(10);
					sourceUser = rs.getString(11);
					sourcePass = rs.getString(12);
					refreshType = rs.getString(13);
					columnName = rs.getString(14);
					sqlText = rs.getString(15);
					snapshot = rs.getBoolean(16);
					queueId  = rs.getInt(17);
				}

				location = 4100;
				conn.close();

				location = 4200;
				if (id != 0)
				{
					location = 5000;
					if (debug)	
						Logger.printMsg("Thread working on ID: " + id);

					ExternalDataThread edt = new ExternalDataThread(gpServer, gpPort, gpDatabase, gpUserName, id, targetSchema, targetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, refreshType, columnName, sqlText, snapshot, queueId);

					location = 5100;
					if (debug)	
						Logger.printMsg("Thread is initialized");

					Thread t = new Thread(edt);

					location = 5200;
					if (debug)	
						Logger.printMsg("Thread started");
					t.start();
				}
				else
				{
					location = 6000;
					Thread.sleep(5000);
				}
				
			}
			catch (SQLException ex) 
			{
				Logger.printMsg("(" + method + ":" + location + ":" + ex.getMessage() + ")");
				Thread.sleep(5000);
			}
		}
   	
	}
}

