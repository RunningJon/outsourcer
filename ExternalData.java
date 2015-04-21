import java.sql.*;
import java.net.*;
import java.io.*;

public class ExternalData
{

	private static String myclass = "ExternalData";
	public static boolean debug = false;

	public static void main(String[] args) throws Exception
	{
		String method = "main";
		int location = 1000;
		int argsCount = args.length;

		String configFile = "";
		int queueId = 0;
		int appendColumnMax = 0;
		String refreshType = "";
		String sourceTable = "";
		int customSQLId = 0;

		location = 2000;

		if (argsCount == 5)
		{
			location = 3100;
			//this is an external table Outsourcer defines
			configFile = args[0];
			queueId = Integer.parseInt(args[1]);
			appendColumnMax = Integer.parseInt(args[2]);
			refreshType = args[3];
			sourceTable = args[4];
			executeOS(configFile, queueId, appendColumnMax, refreshType, sourceTable);
		}
		else if (argsCount == 2)
		{
			location = 3200;
			//this is an external table that a user defines
			configFile = args[0];
			customSQLId = Integer.parseInt(args[1]);
			executeExt(configFile, customSQLId);
		}
        }

	private static void executeOS(String configFile, int queueId, int appendColumnMax, String refreshType, String sourceTable) throws Exception 
	{
		String method = "executeOS";
		int location = 1000;

		String sourceType = "";
		String sourceServer = "";
		String sourceInstance = "";
		int sourcePort = 0;
		String sourceDatabase = "";
		String sourceSchema = "";
		String sourceUser = "";
		String sourcePass = "";
		String appendColumnName = "";
		String strSQL = "";
		Connection conn = null;
		Connection gpConn = null;

		try
		{
			location = 3000;

			ResultSet rs;
			Statement stmt;

			gpConn = CommonDB.connectGP(configFile);

			location = 3010;
			strSQL = GP.getQueueDetails(gpConn, queueId);

			location = 3020;
			stmt = gpConn.createStatement();

			location = 3040;
			rs = stmt.executeQuery(strSQL);

			while (rs.next())
			{
				//handled with a parameter so Outsourcer can do replications jobs
				//refreshType = rs.getString(1);
				sourceType = rs.getString(2);
				sourceServer = rs.getString(3);
				sourceInstance = rs.getString(4);
				sourcePort = rs.getInt(5);
				sourceDatabase = rs.getString(6);
				sourceSchema = rs.getString(7);
				//handled with a parameter so Outsourcer can do replications jobs
				//sourceTable = rs.getString(8);
				sourceUser = rs.getString(9);
				sourcePass = rs.getString(10);
				appendColumnName = rs.getString(11);
			}

			location = 3090;
			if (sourceType.equals("sqlserver"))
			{
				location = 3100;
				gpConn.close();

				location = 3150;
				conn = CommonDB.connectSQLServer(sourceServer, sourceInstance, sourceUser, sourcePass);

				location = 3200;
				//create SQL statement for selecting data
				strSQL = SQLServer.getSQLForData(conn, sourceDatabase, sourceSchema, sourceTable, refreshType, appendColumnName, appendColumnMax);
			
				location = 3300;	
				//execute the SQL Statement
				CommonDB.outputData(conn, strSQL);

				location = 3400;
				conn.close();

			}
			else if (sourceType.equals("oracle"))
			{
				location = 4000;
				int fetchSize = Integer.parseInt(GP.getVariable(gpConn, "oFetchSize"));
			
				location = 4050;
				gpConn.close();

				location = 4100;
				conn = CommonDB.connectOracle(sourceServer, sourceDatabase, sourcePort, sourceUser, sourcePass, fetchSize);

				location = 4200;
				//execute the SQL Statement
				strSQL = Oracle.getSQLForData(conn, sourceSchema, sourceTable, refreshType, appendColumnName, appendColumnMax);
				
				location = 4300;	
				//execute the SQL Statement
				CommonDB.outputData(conn, strSQL);

				location = 4400;
				conn.close();

			}

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
		finally
		{
			if (conn != null)
				conn.close();
			if (gpConn != null)
				gpConn.close();
		}
        }

	private static void executeExt(String configFile, int customSQLId) throws Exception
	{
		String method = "executeExt";
		int location = 1000;

		String sourceType = "";
		String sourceServer = "";
		String sourceInstance = "";
		int sourcePort = 0;
		String sourceDatabase = "";
		String sourceUser = "";
		String sourcePass = "";
		String selectSQL = "";
		String strSQL = "";
		Connection conn = null;
		Connection gpConn = null;

		try
		{
			location = 3000;
			gpConn = CommonDB.connectGP(configFile);

			ResultSet rs;
			Statement stmt;

			location = 3010;
			strSQL = GP.getCustomSQLDetails(gpConn, customSQLId);

			location = 3020;
			stmt = gpConn.createStatement();

			location = 3030;
			rs = stmt.executeQuery(strSQL);

			while (rs.next())
			{
				sourceType = rs.getString(1);
				sourceServer = rs.getString(2);
				sourceInstance = rs.getString(3);
				sourcePort = rs.getInt(4);
				sourceDatabase = rs.getString(5);
				sourceUser = rs.getString(6);
				sourcePass = rs.getString(7);
				selectSQL = rs.getString(8);
			}

			location = 3090;
			if (sourceType.equals("sqlserver"))
			{
			
				location = 3100;
				gpConn.close();

				location = 3200;
				conn = CommonDB.connectSQLServer(sourceServer, sourceInstance, sourceUser, sourcePass);

				location = 3300;	
				//execute the SQL Statement
				CommonDB.outputData(conn, selectSQL);

				location = 3400;
				conn.close();

			}
			else if (sourceType.equals("oracle"))
			{
				location = 4000;
				int fetchSize = Integer.parseInt(GP.getVariable(gpConn, "oFetchSize"));

				location = 4100;
				gpConn.close();
			
				location = 4200;
				conn = CommonDB.connectOracle(sourceServer, sourceDatabase, sourcePort, sourceUser, sourcePass, fetchSize);

				location = 4300;
				//execute the SQL Statement
				CommonDB.outputData(conn, selectSQL);

				location = 4400;
				conn.close();

			}

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
		finally
		{
			if (conn != null)
	 			conn.close();
			if (gpConn != null)
	 			gpConn.close();
		}
        }

}
