import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class CustomSQLModel
{
	String id;
	String tableName;
	ArrayList<String> columns;
	ArrayList<String> columnDataTypes;
	String sqlText;
	String sourceType;
	String sourceServerName;
	String sourceInstanceName;
	String sourcePort;
	String sourceDatabaseName;
	String sourceUserName;
	String sourcePass;
	String gpfdistPort;
	public static int maxColumns = 50;

	public static void startAll() throws SQLException
	{
		String id = "";
		String tableName = "";
		String strColumn = "";
		String strColumnDataType = "";
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> columnDataTypes = new ArrayList<String>();
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
			strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass\n";
			strSQL += "array_upper(columns, 1) as column_count\n";
			strSQL += "FROM os.custom_sql";

			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = rs.getString(1);
				tableName = rs.getString(2);
				strColumn = rs.getString(3);
				strColumnDataType = rs.getString(4);
				sqlText = rs.getString(5);
				sourceType = rs.getString(6);
				sourceServerName = rs.getString(7);
				sourceInstanceName = rs.getString(8);
				sourcePort = rs.getString(9);
				sourceDatabaseName = rs.getString(10);
				sourceUserName = rs.getString(11);
				sourcePass = rs.getString(12);
				columnCount = rs.getInt(13);

				for (int i=0; i <= columnCount; i++)
				{
					if (strColumn != null && !strColumn.equals(""))
					{
						columns.add(strColumn);
						columnDataTypes.add(strColumnDataType);
					}
				}
	
				updateTable(id, tableName, columns, columnDataTypes, sqlText, sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceUserName, sourcePass);
			}

		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort) throws SQLException
	{
		String strSQL = "SELECT '<button onclick=\"updateCustomSQL(''' || id || ''', ''update'')\">Update</button>' ||\n";
		strSQL += "'&nbsp;<button onclick=\"updateCustomSQL(''' || id || ''', ''delete'')\">Delete</button>' AS manage,\n";
		strSQL += "id,\n";
		strSQL += "LOWER(table_name) AS table_name, initcap(source_type) as source_type\n";
		strSQL += "FROM os.custom_sql\n";
		
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
		}
		sortBy = sortBy.toLowerCase();
		if (sortBy.equals("table_name") || sortBy.equals("source_type"))
			strSQL += "ORDER BY " + sortBy + " " + sort + "\n";
		else
			strSQL += "ORDER BY LOWER(table_name) ASC\n";

		if (!limit.equals(""))
			strSQL += "LIMIT " + limit + " ";

		if (!offset.equals(""))
			strSQL += "OFFSET " + offset;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			return rs;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	private static void customStop(String id) throws SQLException
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
			strSQL += "AND NOT EXISTS 	(SELECT NULL\n";
			strSQL += "			FROM os.custom_sql AS c2\n";
			strSQL += "			WHERE c1.id <> c2.id\n";
			strSQL += "			AND c1.gpfdist_port = c2.gpfdist_port)";

			ResultSet rs = OutsourcerModel.getResults(strSQL);
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

	public static void updateTable(String id, String tableName, ArrayList<String> columns, ArrayList<String> columnDataTypes, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) throws SQLException
	{
		String strColumns = OutsourcerModel.javaArrayToString(columns);
		String strColumnDataTypes = OutsourcerModel.javaArrayToString(columnDataTypes);
		sqlText = OutsourcerModel.setSQLString(sqlText);
		sourceType = OutsourcerModel.setSQLString(sourceType);
		sourceServerName = OutsourcerModel.setSQLString(sourceServerName);
		sourceInstanceName = OutsourcerModel.setSQLString(sourceInstanceName);
		sourcePort = OutsourcerModel.setSQLInt(sourcePort);
		sourceDatabaseName = OutsourcerModel.setSQLString(sourceDatabaseName);
		sourceUserName = OutsourcerModel.setSQLString(sourceUserName);
		sourcePass = OutsourcerModel.setSQLString(sourcePass);

		try
		{
			//stop the gpfdist process for this job
			customStop(id);

			//drop the old version of the table
			tableName = tableName.toLowerCase();
			dropExtTable(tableName);

			//add the quotes for the table name so the insert works properly
			tableName = OutsourcerModel.setSQLString(tableName);

			//dynamically get the gpfdist port for the custom table
			int gpfdistPort = GpfdistRunner.customStart(OSProperties.osHome);

			//build the insert statement
			String strSQL = "INSERT INTO os.ao_custom_sql\n";
			strSQL += "(id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, gpfdist_port)\n";
			strSQL += "VALUES\n";
			strSQL += "	(" + id + ", \n";
			strSQL += "     " + tableName + ", \n";
			strSQL += "     " + strColumns + ", \n";
			strSQL += "     " + strColumnDataTypes + ", \n";
			strSQL += "     " + sqlText + ", \n";
			strSQL += "     " + sourceType + ", \n";
			strSQL += "     " + sourceServerName + ", \n";
			strSQL += "     " + sourceInstanceName + ", \n";
			strSQL += "     " + sourcePort + ", \n";
			strSQL += "     " + sourceDatabaseName + ", \n";
			strSQL += "     " + sourceUserName + ", \n";
			strSQL += "     " + sourcePass + ", \n";
			strSQL += "     " + gpfdistPort + ")\n";
			OutsourcerModel.updateTable(strSQL);

			//create the external table
			createExtTable(id);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable(String id, String tableName) throws SQLException
	{

		String strSQL = "INSERT INTO os.ao_custom_sql\n";
		strSQL += "(id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, gpfdist_port, deleted)\n";
		strSQL += "SELECT id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, gpfdist_port, TRUE AS deleted\n";
		strSQL += "FROM os.custom_sql\n";
		strSQL += "WHERE id = " + id;
		try
		{
			//stop the gpfdist process for this job
			customStop(id);

			tableName = tableName.toLowerCase();
			dropExtTable(tableName);
			OutsourcerModel.updateTable(strSQL);
		}

		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
	
	public static void insertTable(String tableName, ArrayList<String> columns, ArrayList<String> columnDataTypes, String sqlText, String extConnectionId) throws SQLException
	{
		tableName = OutsourcerModel.setSQLString(tableName.toLowerCase());
		String strColumns = OutsourcerModel.javaArrayToString(columns);
		String strColumnDataTypes = OutsourcerModel.javaArrayToString(columnDataTypes);
		sqlText = OutsourcerModel.setSQLString(sqlText);

		try
		{
			String id = nextVal();

			//dynamically get the gpfdist port for the custom table
			int gpfdistPort = GpfdistRunner.customStart(OSProperties.osHome);

			String strSQL = "INSERT INTO os.ao_custom_sql\n";
			strSQL += "(id, table_name, columns, column_datatypes, sql_text, gpfdist_port,\n";
			strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass)\n";
			strSQL += "SELECT " + id + ", " + tableName + ", " + strColumns + ", " + strColumnDataTypes + ", " + sqlText + ", " + gpfdistPort + ",\n";
			strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass\n";
			strSQL += "FROM os.ext_connection\n";
			strSQL += "WHERE id = " + extConnectionId;
			OutsourcerModel.updateTable(strSQL);

			createExtTable(id);
			
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static ArrayList<String> getDataTypes()
	{
		ArrayList<String> dataTypes = new ArrayList<String>();
		dataTypes.add("bigint");
		dataTypes.add("boolean");
		dataTypes.add("float8");
		dataTypes.add("int");
		dataTypes.add("numeric");
		dataTypes.add("smallint");
		dataTypes.add("text");
		dataTypes.add("timestamp");
		dataTypes.add("timestamptz");
		dataTypes.add("varchar");
		return dataTypes;
	}

	private static String nextVal() throws SQLException
	{
		try
		{
			String id = "";
			String strSQL = "SELECT nextval('os.ao_custom_sql_id_seq')";
			OutsourcerModel.getResults(strSQL);

			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = Integer.toString(rs.getInt(1));
			}

			return id;
	
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	private static void createExtTable(String id) throws SQLException
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

			ResultSet rs = OutsourcerModel.getResults(strSQL);
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

			OutsourcerModel.updateTable(strSQLCreateTable);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	private static void dropExtTable(String tableName) throws SQLException
	{
		try
		{
			String strSQL = "DROP EXTERNAL TABLE IF EXISTS " + tableName;
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public CustomSQLModel (String aId) throws SQLException
	{
		String strSQL = "SELECT id, table_name, columns, column_datatypes, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass\n";
		strSQL += "FROM os.custom_sql\n";
		strSQL += "WHERE id = " + aId;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = Integer.toString(rs.getInt(1));
				tableName = rs.getString(2);
				columns = OutsourcerModel.gpArrayToJavaArray(rs.getString(3));
				columnDataTypes = OutsourcerModel.gpArrayToJavaArray(rs.getString(4));
				sqlText = rs.getString(5);
				sourceType = rs.getString(6);
				sourceServerName = rs.getString(7);
				sourceInstanceName = rs.getString(8);
				sourcePort = rs.getString(9);
				sourceDatabaseName = rs.getString(10);
				sourceUserName = rs.getString(11);
				sourcePass = rs.getString(12);
			}
		}
		catch (SQLException ex)
		{
			//do something??
		}

	}

	public static CustomSQLModel getModel(String id)
	{
		try
		{
			return new CustomSQLModel(id);
		}
		catch (Exception ex)
		{
			return null;
		}
	}
}
