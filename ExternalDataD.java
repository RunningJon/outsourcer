import java.sql.*;
import java.net.*;
import java.io.*;

public class ExternalDataD
{
	private static String myclass = "ExternalDataD";
	public static boolean debug = true;
	public static String configFile = "";

	public static void main(String[] args) throws Exception
	{

		String method = "main";
		int location = 1000;
		int argsCount = args.length;

		configFile = args[0];
		String action = args[1];

		location = 2000;
		try
		{
			location = 3000;
			if (action.equals("start"))
			{
				Connection conn = CommonDB.connectGP(configFile);
				GP.cancelJobs(conn);
				conn.close();
				loadLoop();
			} 
			else if (action.equals("stop"))
			{
				Connection conn = CommonDB.connectGP(configFile);
				GP.failJobs(conn);
				GP.cancelJobs(conn);
				conn.close();
			}
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
        }

	private static void loadLoop() throws Exception
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

				int queueId = 0;
				Timestamp queueDate = new Timestamp((new java.util.Date()).getTime());
				Timestamp startDate = new Timestamp((new java.util.Date()).getTime());
				int numRows = 0;
				int id = 0;
				String refreshType = "";
				String targetSchema = "";
				String targetTable = "";
				boolean targetAppendOnly = false;
				boolean targetCompressed = false;
				boolean targetRowOrientation = true;
				String sourceType = "";
				String sourceServer = "";
				String sourceInstance = "";
				int sourcePort = 0;
				String sourceDatabase = "";
				String sourceSchema = "";
				String sourceTable = "";
				String sourceUser = "";
				String sourcePass = "";
				String columnName = "";
				String sqlText = "";
				boolean snapshot = false;

				location = 3000;
				Connection conn = CommonDB.connectGP(configFile);

				location = 3100;
				strSQL = "SELECT queue_id, queue_date, start_date,\n";
				strSQL += "	id, refresh_type, LOWER(target_schema_name) as target_schema_name, LOWER(target_table_name) as target_table_name,\n";
				strSQL += "	target_append_only, target_compressed, target_row_orientation,\n";
            			strSQL += "	source_type, source_server_name, source_instance_name, source_port, source_database_name, source_schema_name,\n";
            			strSQL += "	source_table_name, source_user_name, source_pass, column_name,\n";
				strSQL += "	sql_text, snapshot\n";
				strSQL += "FROM os.fn_update_status()";

				location = 3200;
    				stmt = conn.createStatement();

				location = 3300;
    				rs = stmt.executeQuery(strSQL);
				
				//query only returns one record
				while (rs.next())                        
				{
					location = 4000;
					queueId = rs.getInt(1);
					queueDate = rs.getTimestamp(2);
					startDate = rs.getTimestamp(3);
					id  = rs.getInt(4);
					refreshType = rs.getString(5);
					targetSchema = rs.getString(6);
					targetTable = rs.getString(7);
					targetAppendOnly = rs.getBoolean(8);
					targetCompressed = rs.getBoolean(9);
					targetRowOrientation = rs.getBoolean(10);
					sourceType = rs.getString(11);
					sourceServer = rs.getString(12);
					sourceInstance = rs.getString(13);
					sourcePort = rs.getInt(14);
					sourceDatabase = rs.getString(15);
					sourceSchema = rs.getString(16);
					sourceTable = rs.getString(17);
					sourceUser = rs.getString(18);
					sourcePass = rs.getString(19);
					columnName = rs.getString(20);
					sqlText = rs.getString(21);
					snapshot = rs.getBoolean(22);
				}

				location = 4100;
				conn.close();

				location = 4200;
				if (id != 0)
				{
					location = 5000;
					if (debug)	
						Logger.printMsg("Thread working on ID: " + id);

					ExternalDataThread edt = new ExternalDataThread(queueId, queueDate, startDate, id, refreshType, targetSchema, targetTable, targetAppendOnly, targetCompressed, targetRowOrientation, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, columnName, sqlText, snapshot);

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

