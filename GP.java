import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.*;

public class GP
{
	private static String myclass = "GP";
	public static boolean debug = false;
	public static String externalSchema = "ext";

	public static String getVersion(Connection conn) throws SQLException 
	{
		String method = "getVersion";
		int location = 1000;
		try
		{
			location = 2000;
			String value = "";

			location = 2100;
			Statement stmt = conn.createStatement();
			String strSQL = "SELECT CASE " +
					"WHEN POSITION ('HAWQ' in version) > 0 THEN 'HAWQ' " + 
					"WHEN POSITION ('HAWQ' in version) = 0 AND POSITION ('Greenplum Database' IN version) > 0 THEN 'GPDB' " +
					"ELSE 'OTHER' END " +
					"FROM version()";
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
	
	public static void customStartAll(Connection conn) throws SQLException
	{
		String method = "customStartAll";
		int location = 1000;
		try
		{
			Statement stmt = conn.createStatement();

			int id = 0;
			int gpfdistPort = 0;
			String strSQL = "SELECT id\n";
			strSQL += "FROM os.custom_sql";

			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				id = rs.getInt(1);
				gpfdistPort = GpfdistRunner.customStart(OSProperties.osHome);

				strSQL = "INSERT INTO os.ao_custom_sql\n";
				strSQL += "(id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, gpfdist_port)\n";
				strSQL += "SELECT id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, " + gpfdistPort + "\n";
				strSQL += "FROM os.custom_sql\n";
				strSQL += "WHERE id = " + id;

				stmt.executeUpdate(strSQL);
			}
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void failJobs(Connection conn) throws SQLException
	{
		String method = "failJobs";
		int location = 1000;
		try
		{
			Statement stmt = conn.createStatement();

			String strSQL = "INSERT INTO os.ao_queue (queue_id, status, queue_date, start_date, end_date, " +
				"error_message, num_rows, id, refresh_type, target_schema_name, target_table_name, target_append_only, " +
				"target_compressed, target_row_orientation, source_type, source_server_name, source_instance_name, " +
				"source_port, source_database_name, source_schema_name, source_table_name, source_user_name, " +
				"source_pass, column_name, sql_text, snapshot) " +
				"SELECT queue_id, 'failed' as status, queue_date, start_date, now() as end_date, " +
				"'Outsourcer stop requested' as error_message, num_rows, id, refresh_type, target_schema_name, " +
				"target_table_name, target_append_only, target_compressed, target_row_orientation, source_type, " +
				"source_server_name, source_instance_name, source_port, source_database_name, source_schema_name, " +
				"source_table_name, source_user_name, source_pass, column_name, sql_text, snapshot " +
				"FROM os.queue WHERE status = 'queued'";

			stmt.executeUpdate(strSQL);

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void emailAlert(Connection conn, String errorMsg) throws SQLException
	{
		String method = "emailAlert";
		int location = 1000;
		try
		{
			Statement stmt = conn.createStatement();

			String strSQL = "SELECT gp_elog('" + errorMsg + "',true)";
			ResultSet rs = stmt.executeQuery(strSQL);

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void cancelJobs(Connection conn) throws SQLException
	{
		String method = "cancelJobs";
		int location = 1000;
		try
		{
			List<String> jobIdList = new ArrayList<String>();
			Statement stmt = conn.createStatement();

			String strSQL = "SELECT id FROM os.queue WHERE status = 'processing'";
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				jobIdList.add(rs.getString(1));
			}

			for (String jobId : jobIdList)
			{
				strSQL = "SELECT os.fn_cancel_job(" + jobId + ")";
				rs = stmt.executeQuery(strSQL);
			}	
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

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
			errorMessage = errorMessage.replace("\"", "");

			location = 2200;
			errorMessage = errorMessage.replace("'", "");

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

	private static String setSQLString(boolean columnValue) 
	{
		String strColumnValue = "";

		if (columnValue)
			strColumnValue = "true";
		else
			strColumnValue = "false";

		return strColumnValue;
	}

	private static String setSQLString(int columnValue) 
	{
		String strColumnValue = Integer.toString(columnValue);

		return strColumnValue;
	}

	private static String setSQLString(String columnValue) 
	{
		if (columnValue != null)
		{	
			if (columnValue.equals(""))
				columnValue = "null::text";
			else
			{
				columnValue = columnValue.replace("'", "''");
				columnValue = "'" + columnValue + "'";
			}
		}
		else
			columnValue = "null";

		return columnValue;
	}

	public static String setSQLString(Timestamp columnValue)
	{
	
		String strColumnValue = "";
		if (columnValue != null)
			strColumnValue = "'" + columnValue.toString() + "'::timestamp";
		else
			strColumnValue = "null::timestamp";

		return strColumnValue;
	}

	public static void updateStatus(Connection conn, int queueId, String status, Timestamp queueDate, Timestamp startDate, String errorMessage, int numRows, int id, String refreshType, String targetSchema, String targetTable, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String sourceType, String sourceServer, String sourceInstance, int sourcePort, String sourceDatabase, String sourceSchema, String sourceTable, String sourceUser, String sourcePass, String columnName, String sqlText, boolean snapshot) throws SQLException
	{
		String method = "updateStatus";
		int location = 1000;

		String strQueueId = setSQLString(queueId);
		status = setSQLString(status);
		String strQueueDate = setSQLString(queueDate);
		String strStartDate = setSQLString(startDate);
		errorMessage = setSQLString(errorMessage);
		String strNumRows = setSQLString(numRows);
		String strId = setSQLString(id);
		refreshType = setSQLString(refreshType);
		targetSchema = setSQLString(targetSchema);
		targetTable = setSQLString(targetTable);
		String strTargetAppendOnly = setSQLString(targetAppendOnly);
		String strTargetCompressed = setSQLString(targetCompressed);
		String strTargetRowOrientation = setSQLString(targetRowOrientation);
		sourceType = setSQLString(sourceType);
		sourceServer = setSQLString(sourceServer);
		sourceInstance = setSQLString(sourceInstance);
		String strSourcePort = setSQLString(sourcePort);
		sourceDatabase = setSQLString(sourceDatabase);
		sourceSchema = setSQLString(sourceSchema);
		sourceTable = setSQLString(sourceTable);
		sourceUser = setSQLString(sourceUser);
		sourcePass = setSQLString(sourcePass);
		columnName = setSQLString(columnName);
		sqlText = setSQLString(sqlText);
		String strSnapshot = setSQLString(snapshot);
		
		try
		{
			location = 2000;
			Statement stmt = conn.createStatement();

			location = 2400;
			String strSQL = "SELECT os.fn_update_status(" + strQueueId + ", " + status + ", " + strQueueDate + ", " + strStartDate + ", ";
			strSQL += errorMessage + ", " + strNumRows + ", " + strId + ", " + refreshType + ", " + targetSchema + ", " + targetTable + ", ";
			strSQL += strTargetAppendOnly + ", " + strTargetCompressed + ", " + strTargetRowOrientation + ", " + sourceType + ", ";
			strSQL += sourceServer + ", " + sourceInstance + ", " + strSourcePort + ", " + sourceDatabase + ", " + sourceSchema + ", ";
			strSQL += sourceTable + ", " + sourceUser + ", " + sourcePass + ", " + columnName + ", " + sqlText + ", " + strSnapshot + ")";

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
	
	public static void dropExternalReplTable(Connection conn, String sourceType, String targetSchema, String targetTable, String sourceTable) throws SQLException  
	{
		String method = "dropExternalReplTable";
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
			dropExternalTable(conn, replTargetSchema, replTargetTable);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void dropExternalTable(Connection conn, String targetSchema, String targetTable) throws SQLException  
	{
		String method = "dropExternalTable";
		int location = 1000;
	 	try
		{
			location = 2000;
			String externalTable = getExternalTableName(targetSchema, targetTable);

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "DROP EXTERNAL TABLE IF EXISTS \"" + externalSchema + "\".\"" + externalTable + "\"";
			if (debug)
				Logger.printMsg("Dropping External Table (if exists): " + strSQL);

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

	public static boolean createTargetTable(Connection conn, String targetSchema, String targetTable, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String sourceType, String sourceServer, String sourceInstance, int sourcePort, String sourceDatabase, String sourceSchema, String sourceTable, String sourceUser, String sourcePass) throws Exception 
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
				String tableDDL = CommonDB.getGPTableDDL(sourceType, sourceServer, sourceInstance, sourcePort, sourceDatabase, sourceSchema, sourceTable, sourceUser, sourcePass, targetSchema, targetTable, targetAppendOnly, targetCompressed, targetRowOrientation); 

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

	public static void createReplExternalTable(Connection conn, String osServer, String refreshType, String targetSchema, String targetTable, String sourceType, String sourceTable, String maxId, int queueId, int jobPort) throws SQLException 
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
			createExternalTable(conn, osServer, refreshType, replSourceTable, replTargetSchema, replTargetTable, maxId, queueId, jobPort);

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void createExternalTable(Connection conn, String osServer, String refreshType, String sourceTable, String targetSchema, String targetTable, String maxId, int queueId, int jobPort) throws SQLException 
	{
		String method = "createExternalTable";
		int location = 1000;

		try
		{ 
			location = 2000;
			String externalTable = getExternalTableName(targetSchema, targetTable);

			location = 2100;
			Statement stmt = conn.createStatement();

			String createSQL = "CREATE EXTERNAL TABLE \"" + externalSchema + "\".\"" + externalTable + "\" \n (";

			location = 2309;
			String strSQL = "SELECT c.column_name, \n" +
				"       CASE WHEN c.data_type = 'character' THEN c.data_type || '(' || c.character_maximum_length || ')' ELSE c.data_type END AS data_type \n" +
				"FROM INFORMATION_SCHEMA.COLUMNS c \n" +
				"WHERE table_schema = '" + targetSchema + "' \n" +
				"       AND table_name = '" + targetTable + "' \n" +
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
			//Create location for External Table
			////////////////////////////////////////////
			location = 3000;
			//replace space in the maxId because this could now be a date
			maxId = maxId.replace(" ", "SPACE");

			location = 3100;
			String extLocation =    "LOCATION ('gpfdist://" + osServer + ":" + jobPort +
						"/config.properties+" + queueId + "+" + maxId + "+" + refreshType + "+" + sourceTable + "#transform=externaldata" + "')";
			location = 3400;
			extLocation = extLocation + "\n" + "FORMAT 'TEXT' (delimiter '|' null 'null')";

			////////////////////////////////////////////
			//Add createSQL with Java Command to exec.
			////////////////////////////////////////////
			location = 3500;
			createSQL = createSQL + extLocation;

			////////////////////////////////////////////
			//Create new external web table
			////////////////////////////////////////////
			location = 4000;
			if (debug)
				Logger.printMsg("Creating External Table: " + createSQL);

			stmt.executeUpdate(createSQL);
			
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
		catch (Exception e)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + e.getMessage() + ")");
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
	
	public static String getMax(Connection conn, String schema, String table, String columnName) throws SQLException
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

			String max = "-1";

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
			ResultSet rs = stmt.executeQuery(strSQL);

			while (rs.next())
			{
				max = rs.getString(1);	
			}
		
			location = 2313;
			if (max == null)
			{
				max = "-1";
			}
			return max;

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static String getArchMax(Connection conn, String targetSchema, String targetTable, String columnName) throws SQLException
	{
		String method = "getArchMax";
		int location = 1000;

		try
		{
			location = 2000;
			String archTable = getArchTableName(targetSchema, targetTable);

			location = 2100;
			String max = getMax(conn, externalSchema, archTable, columnName);

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
			strSQL = "SELECT refresh_type, source_type, source_server_name, source_instance_name, source_port, source_database_name, " +
				"source_schema_name, source_table_name, source_user_name, source_pass, column_name \n" +
				"FROM os.queue \n" +
				"WHERE queue_id = " + queueId + "\n" +
				"AND status = 'processing'";
				//added processing check

			location = 3000;
			return strSQL;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getCustomSQLDetails(Connection conn, int customSQLId) throws SQLException
	{
		String method = "getCustomSQLDetails";
		int location = 1000;

		String strSQL = "";

		try
		{
			location = 2000;

			strSQL = "SELECT source_type, source_server_name, source_instance_name, source_port, \n" +
				"	source_database_name, source_user_name, source_pass, sql_text \n" +
				"FROM os.custom_sql \n" +
				"WHERE id = " + customSQLId;

			location = 3000;
			return strSQL;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
}
