import java.sql.*;
import java.net.*;
import java.io.*;

public class GP
{

	private static String myclass = "GP";
	public static boolean debug = false;
	public static String externalSchema = "ext";

	public static int executeSQL(Connection conn, String strSQL) throws SQLException
	{
		String method = "executeSQL";
		int location = 1000;

		if (strSQL == null)
			strSQL = "";

		try
		{
			location = 2000;
			int numRows = 0;
			
			location = 2100;
			String SQL = "";

			location = 2150;
			strSQL = strSQL.trim();

			location = 2200;
			int endPosition = strSQL.indexOf(";");

			location = 2300;
			Statement stmt = conn.createStatement();

			location = 2400;
			if (endPosition > -1) 
			{
				location = 2500;
				while (endPosition > - 1) 
				{

					location = 2600;
					SQL = strSQL.substring(0, endPosition);

					if (debug)
						Logger.printMsg("Executing sql: " + SQL);

					//if select, execute query else execute update
					if ( (strSQL.toUpperCase()).startsWith("SELECT") )
					{
						location = 2700;
						stmt.execute(SQL);
					} 
					else
					{
						location = 2900;	
						numRows = numRows + stmt.executeUpdate(SQL);
					}
			
					location = 3100;
					strSQL = strSQL.substring(endPosition + 1).trim();

					location = 3200;
					endPosition = strSQL.indexOf(";");


				}

			}
			else if (strSQL.length() > 0)
			{
				location = 4000;	
				if (debug)
					Logger.printMsg("Executing sql: " + SQL);
				//if select, execute query else execute update
				if ( (strSQL.toUpperCase()).startsWith("SELECT") )
				{
					location = 4200;
					stmt.execute(strSQL);
					//discard results
				} 
				else
				{
					location = 4300;	
					numRows = stmt.executeUpdate(strSQL);
				}
			}


			location = 5000;
			return numRows;

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static String getVariable(Connection conn, String name) throws SQLException 
	{
		String method = "getVariable";
		int location = 1000;
        	try
		{
			location = 2000;
			String value = "";

			location = 2100;
			Statement stmt = conn.createStatement();
			String strSQL = "SELECT os.fn_get_variable('" + name + "')";

			if (debug)
				Logger.printMsg("Getting Variable: " + strSQL);
		
			location = 2200;	
			ResultSet rs = stmt.executeQuery(strSQL);

			while (rs.next())
			{
				value = rs.getString(1);
			}

			location = 2300;
			return value;

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static void executeReplication(Connection conn, String targetSchema, String targetTable, String appendColumnName) throws SQLException
	{
		String method = "executeReplication";
		int location = 1000;
        	try
		{
			location = 2000;
			Statement stmt = conn.createStatement();

			location = 2100;
			String externalTable = getExternalTableName(targetSchema, targetTable);

			location = 2200;
			String stageTable = getStageTableName(targetSchema, targetTable);
		
			location = 2301;	
			String strSQL = "SELECT os.fn_replication('" + targetSchema + "', '" + targetTable + "', '" + 
					externalSchema + "', '" + stageTable + "', '" + 
					appendColumnName + "');";

			if (debug)
				Logger.printMsg("Executing function: " + strSQL);
		
			location = 2400;	
			stmt.executeQuery(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String setErrorMessage(String errorMessage) throws SQLException
	{
	
		String method = "setErrorMessage";
		int location = 1000;
        	try
		{
			location = 2000;
			errorMessage = errorMessage.replace("\"", "\\\"");

			location = 2200;
			errorMessage = errorMessage.replace("'", "\\'");

			location = 2302;
			errorMessage = errorMessage.replace("\n", " ");

			location = 3000;
			return errorMessage;

		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void updateStatus(Connection conn, int queueId, String status, int numRows, String errorMessage) throws SQLException 
	{
		String method = "updateStatus";
		int location = 1000;
        	try
		{
			location = 2000;
			Statement stmt = conn.createStatement();

			location = 2400;
			String strSQL = "SELECT os.fn_update_status(" + queueId + ", '" + status + "', " + numRows + ", '" + errorMessage + "')";

			if (debug)
				Logger.printMsg("Updating Status: " + strSQL);
		
			location = 2500;	
			stmt.executeQuery(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static void analyzeTargetTable(Connection conn, String targetSchema, String targetTable) throws SQLException 
	{
		String method = "analyzeTargetTable";
		int location = 1000;
         	try
		{
			location = 2000;
			Statement stmt = conn.createStatement();

			location = 2100;
			String strSQL = "ANALYZE \"" + targetSchema + "\".\"" + targetTable + "\"";
			
			if (debug)
				Logger.printMsg("Analyzing target table: " + strSQL);
		
			location = 2200;	
			stmt.executeUpdate(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void dropExternalReplWebTable(Connection conn, String sourceType, String targetSchema, String targetTable, String sourceTable) throws SQLException  
	{
		String method = "dropExternalReplWebTable";
		int location = 1000;
		try
		{
			location = 2000;
			String replTargetSchema = externalSchema;

			location = 2100;
			String replTargetTable = getStageTableName(targetSchema, targetTable);

			location = 2200;
			String replSourceTable = getReplTableName(sourceType, sourceTable);

			location = 2315;
			dropExternalWebTable(conn, replTargetSchema, replTargetTable);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void dropExternalWebTable(Connection conn, String targetSchema, String targetTable) throws SQLException  
	{
		String method = "dropExternalWebTable";
		int location = 1000;
         	try
		{
			location = 2000;
			String externalTable = getExternalTableName(targetSchema, targetTable);

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "DROP EXTERNAL WEB TABLE IF EXISTS \"" + externalSchema + "\".\"" + externalTable + "\"";
			if (debug)
				Logger.printMsg("Dropping External Web Table (if exists): " + strSQL);
		
			location = 2303;	
			stmt.executeUpdate(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static int insertTargetTable(Connection conn, String targetSchema, String targetTable) throws SQLException 
	{
		String method = "insertTargetTable";
		int location = 1000;

		int numRows = 0;
		try
		{
			location = 2000;
			String externalTable = getExternalTableName(targetSchema, targetTable);

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "INSERT INTO \"" + targetSchema + "\".\"" + targetTable + "\" \n" +
					"SELECT * FROM \"" + externalSchema + "\".\"" + externalTable + "\"";
			if (debug)
				Logger.printMsg("Executing SQL: " + strSQL);

			location = 2304;
			numRows = stmt.executeUpdate(strSQL);

			location = 2400;
			return numRows;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static int insertReplTable(Connection conn, String targetSchema, String targetTable) throws SQLException 
	{
		String method = "insertReplTable";
		int location = 1000;

		int numRows = 0;
		try
		{
			location = 2000;
			String replTargetSchema = externalSchema;

			location = 2100;
			String replTargetTable = getStageTableName(targetSchema, targetTable);

			location = 2200;
			String externalTable = getExternalTableName(replTargetSchema, replTargetTable);

			location = 2305;
			//truncate the stage table before loading
			truncateTable(conn, replTargetSchema, replTargetTable);

			location = 2400;
			Statement stmt = conn.createStatement();

			location = 2500;
			String strSQL = "INSERT INTO \"" + replTargetSchema + "\".\"" + replTargetTable + "\" \n" +
					"SELECT * FROM \"" + externalSchema + "\".\"" + externalTable + "\"";
			if (debug)
				Logger.printMsg("Executing SQL: " + strSQL);

			location = 2600;
			numRows = stmt.executeUpdate(strSQL);

			location = 2700;
			return numRows;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void truncateTable(Connection conn, String schema, String table) throws SQLException 
	{
		String method = "truncateTable";
		int location = 1000;
         	try
		{
			location = 2000;
			Statement stmt = conn.createStatement();

			location = 2100;
			String strSQL = "truncate table \"" + schema + "\".\"" + table + "\"";
		
			if (debug)
				Logger.printMsg("Truncating table: " + strSQL);
	
			location = 2200;	
			stmt.executeUpdate(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static boolean createSchema(Connection conn, String schema) throws SQLException
	{
		String method = "createSchema";
		int location = 1000;
		try
                {
                        location = 2000;
                        boolean found = false;

                        String strSQL = "SELECT COUNT(*) \n" +
                                        "FROM INFORMATION_SCHEMA.SCHEMATA \n" +
                                        "WHERE SCHEMA_NAME = '" + schema + "'";

                        location = 2100;
                        Statement stmt = conn.createStatement();

                        location = 2200;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        location = 2306;
                        while (rs.next())
                        {
                                if (rs.getInt(1) > 0)
                                        found = true;
                        }

                        location = 2400;
                        if (!(found))
                        {
                                location = 2500;
                                String schemaDDL = "CREATE SCHEMA \"" + schema + "\"";;
                                if (debug)                                        
					Logger.printMsg("Schema DDL: " + schemaDDL);

                                location = 2600;
                                stmt.executeUpdate(schemaDDL);
                        }

                        location = 2700;
                        return found;

                }
		catch (SQLException ex)
		{
			return true;
		}
	}

	public static boolean createTargetTable(Connection conn, String targetSchema, String targetTable, String sourceType, String sourceServer, String sourceInstance, int sourcePort, String sourceDatabase, String sourceSchema, String sourceTable, String sourceUser, String sourcePass) throws Exception 
	{
		String method = "createTargetTable";
		int location = 1000;

		try 
		{
			location = 2000;
			boolean found = false;

			String strSQL = "SELECT COUNT(*) \n" +
					"FROM INFORMATION_SCHEMA.TABLES \n" + 
					"WHERE TABLE_SCHEMA = '" + targetSchema + "' \n" + 
					"	AND TABLE_NAME = '" + targetTable + "'";
	
			location = 2100;	
			Statement stmt = conn.createStatement();

			location = 2200;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2307;
			while (rs.next())
			{
				if (rs.getInt(1) > 0)
					found = true;
			}

			location = 2400;
			if (!(found)) 
			{
				String tableDDL = CommonDB.getGPTableDDL(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, targetSchema, targetTable); 

				if (debug)
					Logger.printMsg("Table DDL: " + tableDDL);

				location = 2800;
				stmt.executeUpdate(tableDDL);
			}

			location = 3000;
			return found;

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getReplTableName(String sourceType, String sourceTable) throws SQLException
	{
		String method = "getReplTableName";
		int location = 1000;
		try
		{
			location = 2000;
			String tableSuffix = "_OS";
			int suffixLength = tableSuffix.length();
			int maxLength = 0;

			String replSourceTable = sourceTable + tableSuffix;
			if (sourceType.equals("oracle"))
			{
				//max length for Oracle is 30 bytes
				//triggers created are t_<tablename><suffix>_aiud which add 7 characters
				//make the max length 23 + suffix so that this output can be used for triggers too
				maxLength = 23 - suffixLength;
			} else if (sourceType.equals("sqlserver"))
			{
				//max length for SQL Server is 128 characters
				//triggers created are t_<tablename><suffix>_i which add 4 characters
				//make the max length 124 + suffix so that this output can be used for triggers too
				maxLength = 124 - suffixLength;
			}
			if (sourceTable.length() > maxLength) 
			{	
				replSourceTable = sourceTable.substring(0, maxLength) + tableSuffix;
			} 
			
			return replSourceTable;

		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void createReplExternalTable(Connection conn, String targetSchema, String targetTable, String sourceType, String sourceServer, String sourceInstance, int sourcePort, String sourceDatabase, String sourceSchema, String sourceTable, String refreshType, String columnName, int maxId, String gpDatabase, int gpPort, int queueId) throws SQLException 
	{
		String method = "createReplExternalTable";
		int location = 1000;
		try
		{
			location = 2000;
			String replTargetSchema = externalSchema;

			location = 2100;
			String replTargetTable = getStageTableName(targetSchema, targetTable);

			location = 2200;
			String replSourceTable = getReplTableName(sourceType, sourceTable);

			location = 2308;
			createExternalTable(conn, replTargetSchema, replTargetTable, sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, replSourceTable, refreshType, columnName, maxId, gpDatabase, gpPort, queueId);

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}	

	public static void createExternalTable(Connection conn, String targetSchema, String targetTable, String sourceType, String sourceServer, String sourceInstance, int sourcePort, String sourceDatabase, String sourceSchema, String sourceTable, String refreshType, String columnName, int maxId, String gpDatabase, int gpPort, int queueId) throws SQLException 
	{
		String method = "createExternalTable";
		int location = 1000;

		try
		{ 
			location = 2000;
			String externalTable = getExternalTableName(targetSchema, targetTable);

			location = 2010;
			String classPath = getVariable(conn, "osJar");

			location = 2020;
			classPath = classPath + ":" + getVariable(conn, "gpdbJar");

			location = 2025;
			classPath = classPath + ":" + getVariable(conn, "oJar");

			location = 2030;
			classPath = classPath + ":" + getVariable(conn, "msJar");

			location = 2035;
			classPath = classPath + " -Xms" + getVariable(conn, "Xms");

			location = 2037;
			classPath = classPath + " -Xmx" + getVariable(conn, "Xmx");

			location = 2039;
			classPath = classPath + " -Djava.security.egd=file:///dev/urandom";

			location = 2040;
			classPath = classPath + " ExternalData";

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
			String createSQL = "CREATE EXTERNAL WEB TABLE \"" + externalSchema + "\".\"" + externalTable + "\" \n (";

			location = 2309;
			String strSQL = "SELECT c.column_name, \n" +
				"	CASE WHEN c.data_type = 'character' THEN c.data_type || '(' || c.character_maximum_length || ')' ELSE c.data_type END AS data_type \n" +
                      		"FROM INFORMATION_SCHEMA.COLUMNS c \n" + 
                      		"WHERE table_schema = '" + targetSchema + "' \n" + 
                      		"	AND table_name = '" + targetTable + "' \n" + 
                      		"ORDER BY ordinal_position";
		
			location = 2400;	
			ResultSet rs = stmt.executeQuery(strSQL);
		
			location = 2500;	
			while (rs.next())
			{
				location = 2600;
				if (rs.getRow() == 1)
				{
					location = 2700;
					createSQL = createSQL + "\"" + rs.getString(1) + "\" " + rs.getString(2);
				}
				else
				{
					location = 2800;
					createSQL = createSQL + ", \n \"" + rs.getString(1) + "\" " + rs.getString(2);
				}
			}
		
			location = 2900;	
			createSQL = createSQL + ") \n";

			////////////////////////////////////////////
			//Create Java command for External Web Table
			////////////////////////////////////////////
			location = 3000;
			String javaExecute = "EXECUTE E'java -classpath " + classPath + " " + sourceType + " " + sourceServer + " ";
			
			location = 3050;
			sourceDatabase = sourceDatabase.replaceAll("\\$", "\\\\\\\\\\$");
			sourceSchema = sourceSchema.replaceAll("\\$", "\\\\\\\\\\$");
			sourceTable = sourceTable.replaceAll("\\$", "\\\\\\\\\\$");

			if (sourceInstance != null)
			{
				location = 3100;
				sourceInstance = sourceInstance.replaceAll("\\$", "\\\\\\\\\\$");
				javaExecute = javaExecute + "\"" + sourceInstance + "\" " + sourcePort + " \"" + sourceDatabase + "\" ";
			}
			else
			{
				location = 3200;
				javaExecute = javaExecute + "\"\" " + sourcePort + " \"" + sourceDatabase + "\" ";
			}

			location = 3300;
			javaExecute = javaExecute + "\"" + sourceSchema + "\" \"" + sourceTable + "\" " + refreshType + " " + columnName + " " + maxId + " " + gpDatabase + " "  + gpPort + " " + queueId + "'";

			location = 3400;
			javaExecute = javaExecute + "\n" + "ON MASTER FORMAT 'TEXT' (delimiter '|' null 'null' escape '\\\\')";

			////////////////////////////////////////////
			//Add createSQL with Java Command to exec.
			////////////////////////////////////////////
			location = 3500;
			createSQL = createSQL + javaExecute;
			
			////////////////////////////////////////////
			//Create new external web table
			////////////////////////////////////////////
			location = 4000;
			if (debug)
				Logger.printMsg("Creating External Web Table: " + createSQL);

			stmt.executeUpdate(createSQL);
			
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}	

	private static String getExternalTableName(String targetSchema, String targetTable) throws SQLException
	{
		String method = "getExternalTableName";
		int location = 1000;

		try
		{	
			location = 2000;
			String returnString = targetSchema + "_" + targetTable;
			return returnString;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}

	}

	private static String getArchTableName(String targetSchema, String targetTable) throws SQLException
	{
		String method = "getArchTableName";
		int location = 1000;

		try
		{
			location = 2000;
			String returnString = targetSchema + "_" + targetTable + "_arch";;
			return returnString;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}

	}

	private static String getStageTableName(String targetSchema, String targetTable) throws SQLException
	{
		String method = "getStageTableName";
		int location = 1000;

		try
		{
			location = 2000;
			String returnString = targetSchema + "_" + targetTable + "_stage";;
			return returnString;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}

	}

	public static boolean checkStageTable(Connection conn, String targetSchema, String targetTable) throws SQLException
	{
		String method = "checkStageTable";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			String stageTable = getStageTableName(targetSchema, targetTable);

			location = 2100;
			String strSQL = "SELECT NULL \n" + 
					"FROM INFORMATION_SCHEMA.TABLES \n" +
					"WHERE table_schema = '" + externalSchema + "' \n" +
					"	AND table_name = '" + stageTable + "'";

			if (debug)
				Logger.printMsg("Executing sql: " + strSQL);
		
			location = 2200;	
			Statement stmt = conn.createStatement();

			location = 2310;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				found = true;
			}

			location = 2500;
			return found;

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static boolean checkArchTable(Connection conn, String targetSchema, String targetTable) throws SQLException
	{
		String method = "checkArchTable";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			String archTable = getArchTableName(targetSchema, targetTable);

			location = 2100;
			String strSQL = "SELECT NULL \n" + 
					"FROM INFORMATION_SCHEMA.TABLES \n" +
					"WHERE table_schema = '" + externalSchema + "' \n" +
					"	AND table_name = '" + archTable + "'";

			if (debug)
				Logger.printMsg("Executing sql: " + strSQL);
		
			location = 2200;	
			Statement stmt = conn.createStatement();

			location = 2311;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				found = true;
			}

			location = 2500;
			return found;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void setupReplicationTables(Connection conn, String targetSchema, String targetTable, String columnName) throws SQLException
	{
		String method = "setupReplicationTables";
		int location = 1000;

		try
		{
			location = 2000;
			String archTable = getArchTableName(targetSchema, targetTable);

			location = 2100;
			String stageTable = getStageTableName(targetSchema, targetTable);

			location = 2200;
			String strSQL = "SELECT os.fn_replication_setup('" + targetSchema + "', '" + targetTable + "', '" +
					externalSchema + "', '" + stageTable + "', '" + archTable + "', '" + columnName + "')";

			location = 2312;
			Statement stmt = conn.createStatement();

			location = 2400;
			stmt.executeQuery(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static int getMax(Connection conn, String schema, String table, String columnName) throws SQLException
	{
		String method = "getMax";
		int location = 1000;

		try
		{
			location = 2000;
			String strSQL = "SELECT MAX(\"" + columnName.toLowerCase() + "\") \n" +
					"FROM \"" + schema + "\".\"" + table + "\"";

			if (debug)
				Logger.printMsg("Executing sql: " + strSQL);

			int max = -1;

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        while (rs.next())
                        {
				max = rs.getInt(1);	
                        }
		
			location = 2313;	
			return max;

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static int getArchMax(Connection conn, String targetSchema, String targetTable, String columnName) throws SQLException
	{
		String method = "getArchMax";
		int location = 1000;

		try
		{
			location = 2000;
			String archTable = getArchTableName(targetSchema, targetTable);

			location = 2100;
			int max = getMax(conn, externalSchema, archTable, columnName);

			location = 2200;
			return max;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	public static String getQueueDetails(Connection conn, int queueId) throws SQLException
	{
		String method = "getQueueDetails";
		int location = 1000;

		String strSQL = "";

		try
		{
			location = 2000;
			strSQL = "SELECT (source).user_name, (source).pass \n" +
				"FROM os.queue \n" +
				"WHERE queue_id = " + queueId;

			location = 3000;
			return strSQL;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getExtConnectionDetails(Connection conn, int connectionId) throws SQLException
	{
		String method = "getExtConnectionDetails";
		int location = 1000;

		String strSQL = "";

		try
		{
			location = 2000;

			strSQL = "SELECT type, server_name, instance_name, port, \n" +
				"      database_name, user_name, pass \n" +
				"FROM os.ext_connection \n " +
				"WHERE id = " + connectionId;

			location = 3000;
			return strSQL;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
}
