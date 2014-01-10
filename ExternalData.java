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

		String sourceType = "";
		String sourceServer = "";
		String sourceInstance = "";
		int sourcePort = 0;
		String sourceDatabase = "";
		String sourceSchema = "";
		String sourceTable = "";
		String refreshType = "";
		String appendColumnName = "";
		int appendColumnMax = 0;
		String gpDatabase = "";
		int gpPort = 0;
		int queueId = 0;
		int connectionId = 0;
		String selectSQL = "";

		//this is an external table that a user defines
		if (argsCount == 4) 
		{
			gpDatabase = args[0];
			gpPort = Integer.parseInt(args[1]);
			connectionId = Integer.parseInt(args[2]);
			selectSQL = args[3];
		} 
		//this is an extrenal table Outsourcer defines
		else if (argsCount == 13)
		{
			sourceType = args[0];
			sourceServer = args[1];
			sourceInstance = args[2];
			sourcePort = Integer.parseInt(args[3]);
			sourceDatabase = args[4];
			sourceSchema = args[5];
			sourceTable = args[6];
			refreshType = args[7];
			appendColumnName = args[8];
			appendColumnMax = Integer.parseInt(args[9]);
			gpDatabase = args[10];
			gpPort = Integer.parseInt(args[11]);
			queueId = Integer.parseInt(args[12]);
		}

		String gpServer = "localhost";
		String gpUserName = System.getProperty("user.name");
		String sourceUser = "";
		String sourcePass = "";
		Connection gpConn = null;

		try
		{
			location = 3000;
			gpConn = CommonDB.connectGP(gpServer, gpPort, gpDatabase, gpUserName);

			if (argsCount == 13)
			{
				location = 3100;
				executeOS(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, refreshType, appendColumnName, appendColumnMax, gpConn, queueId);
			}

			else if (argsCount == 4)
			{
				location = 3200;
				executeExt(gpConn, connectionId, selectSQL);
			}

			location = 4000;
			gpConn.close();
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
		finally
		{
			if (gpConn != null)
				gpConn.close();
		}
        }

	private static void executeOS(String sourceType, String sourceServer, String sourceInstance, int sourcePort, String sourceDatabase, String sourceSchema, String sourceTable, String refreshType, String appendColumnName, int appendColumnMax, Connection gpConn, int queueId) throws Exception
	{
		String method = "executeOS";
		int location = 1000;

		String sourceUser = "";
		String sourcePass = "";
		String strSQL = "";
		int fetchSize = 10;
		Connection conn = null;

		try
		{
			location = 3000;

			ResultSet rs;
			Statement stmt;

			location = 3010;
			strSQL = GP.getQueueDetails(gpConn, queueId);

			location = 3020;
			stmt = gpConn.createStatement();

			location = 3040;
			rs = stmt.executeQuery(strSQL);

			while (rs.next())
			{
				sourceUser = rs.getString(1);
				sourcePass = rs.getString(2);	
			}

			location = 3090;
			if (sourceType.equals("sqlserver"))
			{
				location = 3100;
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
				fetchSize = Integer.parseInt(GP.getVariable(gpConn, "oFetchSize"));
			
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
		}
        }

	private static void executeExt(Connection gpConn, int connectionId, String selectSQL) throws Exception
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
		String strSQL = "";
		Connection conn = null;
		int fetchSize = 10;

		try
		{
			location = 3000;

			ResultSet rs;
			Statement stmt;

			location = 3010;
			strSQL = GP.getExtConnectionDetails(gpConn, connectionId);

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
			}

			location = 3090;
			if (sourceType.equals("sqlserver"))
			{
				location = 3100;
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
				fetchSize = Integer.parseInt(GP.getVariable(gpConn, "oFetchSize"));
			
				location = 4100;
				conn = CommonDB.connectOracle(sourceServer, sourceDatabase, sourcePort, sourceUser, sourcePass, fetchSize);

				location = 4200;
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
		}
        }

}
