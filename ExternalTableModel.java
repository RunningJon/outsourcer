import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class ExternalTableModel
{
	String id;
	String sourceType;
	String sourceServerName;
	String sourceInstanceName;
	String sourcePort;
	String sourceDatabaseName;
	String sourceUserName;
	String sourcePass;

	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort) throws SQLException
	{
		String strSQL = "SELECT '<button onclick=\"updateExternalTable(' || id || ', ''update'')\">Update</button>' ||\n";
		strSQL += "	'&nbsp;<button onclick=\"updateExternalTable(' || id || ', ''delete'')\">Delete</button>' ||\n";
		strSQL += "	'&nbsp;<button onclick=\"createJobs(' || id || ')\">Create Jobs</button>' as update_text,\n";
		strSQL += "id, initcap(source_type) as source_type, source_server_name, coalesce(source_instance_name, '') as source_instance_name, coalesce(source_port::text, '') as source_port,\n";
		strSQL += "coalesce(source_database_name, '') as source_database_name, source_user_name, '<i>password</i>' as source_pass\n ";
		strSQL += "FROM os.ext_connection\n";
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(source_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_server_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_instance_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_port) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_database_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_user_name) LIKE '%' || LOWER('" + search +"') || '%'\n";	
		}
		sortBy = sortBy.toLowerCase();

		if (sortBy.equals("id") || sortBy.equals("source_type") || sortBy.equals("source_server_name") || sortBy.equals("source_instance_name") || sortBy.equals("source_port") || sortBy.equals("source_database_name") || sortBy.equals("source_user_name") || sortBy.equals("source_pass"))
			strSQL += "ORDER BY " + sortBy + " " + sort + "\n";
		else
			strSQL += "ORDER BY id asc\n";

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

	public static void insertTable(String id, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) throws SQLException
	{
		sourceType = OutsourcerModel.setSQLString(sourceType);
		sourceServerName = OutsourcerModel.setSQLString(sourceServerName);
		sourceInstanceName = OutsourcerModel.setSQLString(sourceInstanceName);
		sourcePort = OutsourcerModel.setSQLInt(sourcePort);
		sourceDatabaseName = OutsourcerModel.setSQLString(sourceDatabaseName);
		sourceUserName = OutsourcerModel.setSQLString(sourceUserName);
		sourcePass = OutsourcerModel.setSQLString(sourcePass);

		String strSQL = "INSERT INTO os.ao_ext_connection\n ";

		if (id.equals(""))
			strSQL += "(";
		else
			strSQL += "(id, ";
		strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass)\n ";
		strSQL += "VALUES";

		if (id.equals(""))
			strSQL += "(";
		else
			strSQL += "(" + id + ", ";		
 		strSQL += "	" + sourceType + ", \n";
		strSQL += "	" + sourceServerName + ", \n";
		strSQL += "	" + sourceInstanceName + ", \n";
		strSQL += "	" + sourcePort + ", \n";
		strSQL += "	" + sourceDatabaseName + ", \n";
		strSQL += "	" + sourceUserName + ", \n";
		strSQL += "	" + sourcePass + ")\n";

		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable(String id) throws SQLException
	{
		String strSQL = "INSERT INTO os.ao_ext_connection\n";
		strSQL += "(id, source_type, source_server_name, source_instance_name, source_port,\n";
		strSQL += "source_database_name, source_user_name, source_pass, deleted)\n";
		strSQL += "SELECT id, source_type, source_server_name, source_instance_name, source_port,\n";
		strSQL += "source_database_name, source_user_name, source_pass, TRUE AS deleted\n";
		strSQL += "FROM os.ext_connection\n";
		strSQL += "WHERE id = " + id;

		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
	
	public ExternalTableModel(String aId) throws SQLException
	{
		String strSQL = "SELECT id, source_type, source_server_name, source_instance_name, source_port, source_database_name,\n";
		strSQL += "source_user_name, source_pass\n";
		strSQL += "FROM os.ext_connection\n";
		strSQL += "WHERE id = " + aId;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = Integer.toString(rs.getInt(1));
				sourceType = rs.getString(2);
				sourceServerName = rs.getString(3);
				sourceInstanceName = rs.getString(4);
				sourcePort = rs.getString(5);
				sourceDatabaseName = rs.getString(6);
				sourceUserName = rs.getString(7);
				sourcePass = rs.getString(8);
			}
		}
		catch (SQLException ex)
		{
			//do something??
		}

	}

	public static ExternalTableModel getModel(String id)
	{
		try
		{
			return new ExternalTableModel(id);
		}
		catch (Exception ex)
		{
			return null;
		}
	}

	//validate the connection to the source
	public static String validate(String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass)
	{
		String strSQL = "";
		String msg = "";
		try
		{
			Connection conn = null;

			if (sourceType.equals("sqlserver"))
			{
				conn = CommonDB.connectSQLServer(sourceServerName, sourceInstanceName, sourceUserName, sourcePass);
				msg = SQLServer.validate(conn);
				conn.close();
			}
			else if (sourceType.equals("oracle"))
			{
				int intSourcePort = Integer.parseInt(sourcePort);
				conn = CommonDB.connectOracle(sourceServerName, sourceDatabaseName, intSourcePort, sourceUserName, sourcePass, 10);
				msg = Oracle.validate(conn);
				conn.close();
			}
		}
		catch (Exception e)
		{
			msg = e.getMessage();
		}
		return msg;
	}

	public static int createJobs(String id, String sourceDatabaseName, String sourceSchema, String targetSchema, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String refreshType, String scheduleDesc) throws SQLException
	{
		try 
		{
			ExternalTableModel e = getModel(id);
			String aRefreshType = "'" + refreshType + "'";
			String aTargetSchema = "'" + targetSchema + "'";
			String aTargetTable = "";
			String aTargetAppendOnly = String.valueOf(targetAppendOnly);
			String aTargetCompressed = String.valueOf(targetCompressed);
			String aTargetRowOrientation = String.valueOf(targetRowOrientation);
			String aSourceType = "'" + e.sourceType + "'";
			String aSourceServerName = "'" + e.sourceServerName + "'";
			String aSourceInstanceName = "null";
			if (!(e.sourceInstanceName==null))
				aSourceInstanceName = "'" + e.sourceInstanceName + "'";
			String aSourcePort = e.sourcePort;
			if (e.sourceType.equals("oracle"))
				sourceDatabaseName = e.sourceDatabaseName;
			String aSourceDatabaseName = "'" + sourceDatabaseName + "'";
			String aSourceSchemaName = "'" + sourceSchema + "'";
			String aSourceTableName = "";
			String aSourceUserName = "'" + e.sourceUserName + "'";
			String aSourcePassword = "'" + e.sourcePass + "'";

			scheduleDesc = OutsourcerModel.setSQLString(scheduleDesc);

			Connection conn = null;
			ResultSet rs = null;
			int i = 0;
			String strSQL = "";
			String strSQLStart = "INSERT INTO os.ao_job(refresh_type,\n";
			strSQLStart += "target_schema_name, target_table_name, target_append_only, target_compressed, target_row_orientation,\n";
			strSQLStart += "source_type, source_server_name, source_instance_name, source_port, source_database_name,\n";
			strSQLStart += "source_schema_name, source_table_name, source_user_name, source_pass,\n";
			strSQLStart += "schedule_desc, snapshot)\n";
			strSQLStart += "VALUES (" + aRefreshType + ",\n";
			strSQLStart += aTargetSchema + ", ";

			if (e.sourceType.equals("sqlserver"))
			{	
				conn = CommonDB.connectSQLServer(e.sourceServerName, e.sourceInstanceName, e.sourceUserName, e.sourcePass);
				Statement stmt = conn.createStatement();
				rs = SQLServer.getTableList(conn, sourceDatabaseName, sourceSchema);
			}
			else if (e.sourceType.equals("oracle"))
			{
				int intSourcePort = Integer.parseInt(e.sourcePort);
				conn = CommonDB.connectOracle(e.sourceServerName, e.sourceDatabaseName, intSourcePort, e.sourceUserName, e.sourcePass, 10);
				rs = Oracle.getTableList(conn, sourceSchema);
			}

			while (rs.next())
			{
				i++;
				aTargetTable = "'" + rs.getString(1).toLowerCase() + "'";
				aSourceTableName = "'" + rs.getString(1) + "'";

				strSQL = strSQLStart + aTargetTable + ",\n";
				strSQL += aTargetAppendOnly + ", " + aTargetCompressed + ", " + aTargetRowOrientation + ",\n";
				strSQL += aSourceType + ", " + aSourceServerName + ", " + aSourceInstanceName + ", " + aSourcePort + ", " + aSourceDatabaseName + ",\n";
				strSQL += aSourceSchemaName + ", " + aSourceTableName + ", " + aSourceUserName + ", " + aSourcePassword + ", " + scheduleDesc + ", false)";

				OutsourcerModel.updateTable(strSQL);
			}

			if (!(conn==null))
				conn.close();

			return i;
		}
		catch (Exception e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	public static ArrayList<String> getExtConnectionIds() throws SQLException
	{
		String strSQL = "SELECT id, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name\n";
		strSQL += "FROM os.ext_connection\n";
		strSQL += "ORDER BY 1";
		
		ArrayList<String> extConnectionIds = new ArrayList<String>();

		try
		{
			extConnectionIds = OutsourcerModel.getStringArray(strSQL);
			return extConnectionIds;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
}
