import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class JobModel
{
	String id;
	String refreshType;
	String targetSchemaName;
	String targetTableName;
	String type;
	String serverName;
	String instanceName;
	String port;
	String databaseName;
	String schemaName;
	String tableName;
	String userName;
	String pass;
	String columnName;
	String sqlText;
	String snapshot;  //not sure if I should use boolean or not.
	String scheduleDesc;
	String scheduleNext;

	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort) throws SQLException
	{
		String strSQL = "SELECT '<button onclick=\"updateJob(' || j.id || ', ''update'')\">Update</button>' ||\n";
		strSQL += "'&nbsp;<button onclick=\"updateJob(' || j.id || ', ''delete'')\">Delete</button>&nbsp;' || \n";
		strSQL += "CASE WHEN q.status = 'processing' then '<button style=\"background-color:#707070\" onclick=\"void()\">Processing</button>'\n";
		strSQL += "	WHEN q.status = 'queued' then '<button style=\"background-color:#707070\" onclick=\"void()\">Queued</button>'\n";
		strSQL += "ELSE\n";
		strSQL += "	'<button onclick=\"updateQueue(' || j.id || ', ''insert'')\">Queue</button>' END AS manage,\n";
		strSQL += "j.id, initcap(j.refresh_type) AS refresh_type,\n";
		strSQL += "CASE WHEN j.refresh_type = 'transform' THEN 'Transform' ELSE initcap((j.source).type) END AS source_info,\n";
		strSQL += "coalesce(((j.target).schema_name || '.' || (j.target).table_name), '') AS target_table_name,\n";
		strSQL += "coalesce(j.schedule_desc, '') AS schedule_desc,\n";
		strSQL += "coalesce(schedule_next::text, '') AS schedule_next\n";
 		strSQL += "FROM os.job j\n";
		strSQL += "LEFT OUTER JOIN (\n";
		strSQL += "SELECT id, status FROM (\n";
		strSQL += "SELECT id, status, row_number() over (partition by id order by queue_id desc) as rownum\n";
		strSQL += "FROM os.queue) AS sub WHERE rownum = 1) AS q on j.id = q.id\n";
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(j.refresh_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.target).schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.target).table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).server_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).instance_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).port) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).database_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((j.source).user_name) LIKE '%' || LOWER('" + search +"') || '%'\n";
			strSQL += "OR LOWER(j.column_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.sql_text) LIKE '%' || LOWER('" + search + "') || '%'\n";
		}
		sortBy = sortBy.toLowerCase();
		if (sortBy.equals("id") || sortBy.equals("refresh_type") || sortBy.equals("source_info") || sortBy.equals("target_table_name") || sortBy.equals("schedule_desc") || sortBy.equals("schedule_next"))
			strSQL += "ORDER BY " + sortBy + " " + sort + "\n";
		else
			strSQL += "ORDER BY j.id ASC\n";

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

	public static void updateTable(String id, String refreshType, String targetSchemaName, String targetTableName, String type, String serverName, String instanceName, String port, String databaseName, String schemaName, String tableName, String userName, String pass, String columnName, String sqlText, String snapshot, String scheduleDesc) throws SQLException
	{
		refreshType = OutsourcerModel.setSQLString(refreshType);
		targetSchemaName = OutsourcerModel.setSQLString(targetSchemaName);
		targetTableName = OutsourcerModel.setSQLString(targetTableName);
		type = OutsourcerModel.setSQLString(type);
		serverName = OutsourcerModel.setSQLString(serverName);
		instanceName = OutsourcerModel.setSQLString(instanceName);
		port = OutsourcerModel.setSQLInt(port);
		databaseName = OutsourcerModel.setSQLString(databaseName);
		schemaName = OutsourcerModel.setSQLString(schemaName);
		tableName = OutsourcerModel.setSQLString(tableName);
		userName = OutsourcerModel.setSQLString(userName);
		pass = OutsourcerModel.setSQLString(pass);
		columnName = OutsourcerModel.setSQLString(columnName);
		sqlText = OutsourcerModel.setSQLString(sqlText);
		snapshot = OutsourcerModel.setSQLString(snapshot);
		scheduleDesc = OutsourcerModel.setSQLString(scheduleDesc);

		String strSQL = "UPDATE os.job\n";
		strSQL += "SET refresh_type = " + refreshType + ",\n";
		strSQL += "	target = (" + targetSchemaName + ", " + targetTableName + ")::os.type_target,\n";
		strSQL += "	source = (" + type + ", " + serverName + ", " + instanceName + ", " + port + ", " + databaseName + ", ";
		strSQL += schemaName + ", " + tableName + ", " + userName + ", " + pass + ")::os.type_source,\n";
		strSQL += "	column_name  = " + columnName + ",\n";
		strSQL += "	sql_text  = " + sqlText + ",\n";
		strSQL += "	snapshot  = " + snapshot + ",\n";
		strSQL += "	schedule_desc  = " + scheduleDesc + ",\n";
		strSQL += "	schedule_change  = true\n";
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

	public static void insertTable(String refreshType, String targetSchemaName, String targetTableName, String type, String serverName, String instanceName, String port, String databaseName, String schemaName, String tableName, String userName, String pass, String columnName, String sqlText, String snapshot, String scheduleDesc) throws SQLException
	{
		refreshType = OutsourcerModel.setSQLString(refreshType);
		targetSchemaName = OutsourcerModel.setSQLString(targetSchemaName);
		targetTableName = OutsourcerModel.setSQLString(targetTableName);
		type = OutsourcerModel.setSQLString(type);
		serverName = OutsourcerModel.setSQLString(serverName);
		instanceName = OutsourcerModel.setSQLString(instanceName);
		port = OutsourcerModel.setSQLInt(port);
		databaseName = OutsourcerModel.setSQLString(databaseName);
		schemaName = OutsourcerModel.setSQLString(schemaName);
		tableName = OutsourcerModel.setSQLString(tableName);
		userName = OutsourcerModel.setSQLString(userName);
		pass = OutsourcerModel.setSQLString(pass);
		columnName = OutsourcerModel.setSQLString(columnName);
		sqlText = OutsourcerModel.setSQLString(sqlText);
		snapshot = OutsourcerModel.setSQLString(snapshot);
		scheduleDesc = OutsourcerModel.setSQLString(scheduleDesc);
		
		String strSQL = "INSERT INTO os.job\n";
		strSQL += "(refresh_type, target, source, column_name, sql_text, snapshot, schedule_desc)\n";
		strSQL += "VALUES (" + refreshType + ",\n";
		strSQL += "	" + "(" + targetSchemaName + ", " + targetTableName + ")::os.type_target,\n";
		strSQL += "	" + "(" + type + ", " + serverName + ", " + instanceName + ", " + port + ", " + databaseName + ", ";
		strSQL += schemaName + ", " + tableName + ", " + userName + ", " + pass + ")::os.type_source,\n";
		strSQL += "	" + columnName + ",\n";
		strSQL += "	" + sqlText + ",\n";
		strSQL += "	" + snapshot + ",\n";
		strSQL += "	" + scheduleDesc + ")";

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
		String strSQL = "DELETE\n";
		strSQL += "FROM os.job\n";
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
	
	public JobModel (String aId) throws SQLException
	{
		String strSQL = "SELECT id, refresh_type, (target).schema_name, (target).table_name,\n";
		strSQL += "(source).type, (source).server_name, (source).instance_name, (source).port, (source).database_name,\n";
		strSQL += "(source).schema_name, (source).table_name,\n";
		strSQL += "(source).user_name, (source).pass,\n";
		strSQL += "column_name, sql_text, snapshot, schedule_desc, schedule_next\n";
		strSQL += "FROM os.job\n";
		strSQL += "WHERE id = " + aId;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = Integer.toString(rs.getInt(1));
				refreshType = rs.getString(2);
				targetSchemaName = rs.getString(3);
				targetTableName = rs.getString(4);
				type = rs.getString(5);
				serverName = rs.getString(6);
				instanceName = rs.getString(7);
				port = rs.getString(8);
				databaseName = rs.getString(9);
				schemaName = rs.getString(10);
				tableName = rs.getString(11);
				userName = rs.getString(12);
				pass = rs.getString(13);
				columnName = rs.getString(14);
				sqlText = rs.getString(15);
				snapshot = rs.getString(16);
				scheduleDesc = rs.getString(17);
				scheduleNext = rs.getString(18);
			}
		}
		catch (SQLException ex)
		{
			//do something??
		}

	}

	public static JobModel getModel(String id)
	{
		try
		{
			return new JobModel(id);
		}
		catch (Exception ex)
		{
			return null;
		}
	}

	public static ArrayList<String> getSchemas() throws SQLException
	{
		String strSQL = "SELECT (target).schema_name AS schema_name\n";
		strSQL += "FROM os.job\n";
		strSQL += "GROUP BY (target).schema_name\n"; 
		strSQL += "ORDER BY schema_name";

		ArrayList<String> schemas = new ArrayList<String>();

		try
		{
			schemas = OutsourcerModel.getStringArray(strSQL);
			return schemas;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void updateJobsSchedule(String scheduleDesc, String gpSchema) throws SQLException
	{
		scheduleDesc = OutsourcerModel.setSQLString(scheduleDesc);
		gpSchema = OutsourcerModel.setSQLString(gpSchema);

		try
		{
			String strSQL = "UPDATE os.job\n";
			strSQL += "SET schedule_desc = " + scheduleDesc + ",\n";
			strSQL += "schedule_change = true\n";
			strSQL += "WHERE (target).schema_name = " + gpSchema;

			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable() throws SQLException
	{
		String strSQL = "DELETE FROM os.job";

		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
}	
