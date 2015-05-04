import java.sql.*;
import java.net.*;
import java.io.*;

public class CustomSQL
{
	private static String myclass = "CustomSQL";
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
				startAll(conn);
				conn.close();
			} 
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	private static void startAll(Connection conn) throws SQLException
	{
		String id = "";
		String tableName = "";
		String columns = "";
		String columnDataTypes = "";
		String sqlText = "";
		String sourceType = "";
		String sourceServerName = "";
		String sourceInstanceName = "";
		String sourcePort = "";
		String sourceDatabaseName = "";
		String sourceUserName = "";
		String sourcePass = "";
		int gpfdistPort = 0;
		int columnCount = 0;

		try
		{
			String strSQL = "SELECT id, table_name, columns, column_datatypes, sql_text,\n";
			strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass,\n";
			strSQL += "array_upper(columns, 1) as column_count\n";
			strSQL += "FROM os.custom_sql";

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				id = rs.getString(1);
				tableName = rs.getString(2);
				columns = rs.getString(3);
				columnDataTypes = rs.getString(4);
				sqlText = rs.getString(5);
				sourceType = rs.getString(6);
				sourceServerName = rs.getString(7);
				sourceInstanceName = rs.getString(8);
				sourcePort = rs.getString(9);
				sourceDatabaseName = rs.getString(10);
				sourceUserName = rs.getString(11);
				sourcePass = rs.getString(12);
				columnCount = rs.getInt(13);

				if (sourceType == null)
					sourceType = "";

				if (sourceInstanceName == null)
					sourceInstanceName = "";

				if (sourcePort == null)
					sourcePort = "";

				if (sourceDatabaseName == null)
					sourceDatabaseName = "";

				updateTable(conn, id, tableName, columns, columnDataTypes, sqlText, sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceUserName, sourcePass);
			}

		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

		private static void updateTable(Connection conn, String id, String tableName, String columns, String columnDataTypes, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) throws SQLException
	{
		columns = "'" + columns + "'";
		columnDataTypes = "'" + columnDataTypes + "'";
		sqlText = setSQLString(sqlText);
		sourceType = setSQLString(sourceType);
		sourceServerName = setSQLString(sourceServerName);
		sourceInstanceName = setSQLString(sourceInstanceName);
		sourcePort = setSQLInt(sourcePort);
		sourceDatabaseName = setSQLString(sourceDatabaseName);
		sourceUserName = setSQLString(sourceUserName);
		sourcePass = setSQLString(sourcePass);

		try
		{
			//stop the gpfdist process for this job
			customStop(conn, id);

			//drop the old version of the table
			tableName = tableName.toLowerCase();
			dropExtTable(conn, tableName);

			//add the quotes for the table name so the insert works properly
			tableName = setSQLString(tableName);

			//dynamically get the gpfdist port for the custom table
			int gpfdistPort = GpfdistRunner.customStart(OSProperties.osHome);
			System.out.println("Starting gpfdist on port " + gpfdistPort);

			//build the insert statement
			String strSQL = "INSERT INTO os.ao_custom_sql\n";
			strSQL += "(id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, gpfdist_port)\n";
			strSQL += "VALUES\n";
			strSQL += "     (" + id + ", \n";
			strSQL += "     " + tableName + ", \n";
			strSQL += "     " + columns + ", \n";
			strSQL += "     " + columnDataTypes + ", \n";
			strSQL += "     " + sqlText + ", \n";
			strSQL += "     " + sourceType + ", \n";
			strSQL += "     " + sourceServerName + ", \n";
			strSQL += "     " + sourceInstanceName + ", \n";
			strSQL += "     " + sourcePort + ", \n";
			strSQL += "     " + sourceDatabaseName + ", \n";
			strSQL += "     " + sourceUserName + ", \n";
			strSQL += "     " + sourcePass + ", \n";
			strSQL += "     " + gpfdistPort + ")\n";
			updateTable(conn, strSQL);

			//create the external table
			createExtTable(conn, id);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	private static String setSQLString(String columnValue)
	{
		columnValue = columnValue.replace("'", "\\'");

		if (columnValue.equals(""))
			columnValue = "null";
		else
			columnValue = "'" + columnValue + "'";

		return columnValue;
	}

	private static String setSQLInt(String columnValue)
	{
		if (columnValue.equals(""))
			columnValue = "null";

		return columnValue;
	}

	private static void updateTable(Connection conn, String strSQL) throws SQLException
	{
		try
		{
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}	

	private static void customStop(Connection conn, String id) throws SQLException
	{
		int myGpfdistPort = 0;
		try
		{
			//get the existing gpfdist_port value
			//but only if the port isn't in use by another
			//external table.
			//two tables should never use the same port
			//unless there is a problem
			String strSQL = "SELECT c1.gpfdist_port\n";
			strSQL += "FROM os.custom_sql AS c1\n";
			strSQL += "WHERE c1.id = " + id + "\n";
			strSQL += "AND NOT EXISTS       (SELECT NULL\n";
			strSQL += "		     FROM os.custom_sql AS c2\n";
			strSQL += "		     WHERE c1.id <> c2.id\n";
			strSQL += "		     AND c1.gpfdist_port = c2.gpfdist_port)";

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				myGpfdistPort  = rs.getInt(1);
				GpfdistRunner.customStop(OSProperties.osHome, myGpfdistPort);
			}

		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	private static void dropExtTable(Connection conn, String tableName) throws SQLException
	{
		try
		{
			String strSQL = "DROP EXTERNAL TABLE IF EXISTS " + tableName;
			updateTable(conn, strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	private static void createExtTable(Connection conn, String id) throws SQLException
	{
		try
		{
			String strSQL = "SELECT LOWER(table_name) as table_name,\n";
			strSQL += "unnest(columns) AS column_name,\n";
			strSQL += "unnest(column_datatypes) AS data_type,\n";
			strSQL += "gpfdist_port\n";
			strSQL += "FROM os.custom_sql\n";
			strSQL += "WHERE id = " + id;

			String strSQLCreateTable = "";
			int i = 0;
			int gpfdistPort = 0;

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				i++;
				if (i == 1)
				{
					gpfdistPort = rs.getInt(4);
					strSQLCreateTable = "CREATE EXTERNAL TABLE " + rs.getString(1) + "\n";
					strSQLCreateTable += "(" + rs.getString(2) + " " + rs.getString(3);
				}
				else
				{
					strSQLCreateTable += ",\n" + "\"" + rs.getString(2) + "\" " + rs.getString(3);
				}
			}

			strSQLCreateTable += ")\n";

			strSQLCreateTable += "LOCATION ('gpfdist://" + OSProperties.osServer + ":" + gpfdistPort + "/config.properties+" + id + "#transform=externaldata')\n";
			strSQLCreateTable += "FORMAT 'TEXT' (delimiter '|' null 'null')";

			updateTable(conn, strSQLCreateTable);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

}
