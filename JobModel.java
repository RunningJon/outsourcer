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
	boolean targetAppendOnly;
	boolean targetCompressed;
	boolean targetRowOrientation;
	String sourceType;
	String sourceServerName;
	String sourceInstanceName;
	String sourcePort;
	String sourceDatabaseName;
	String sourceSchemaName;
	String sourceTableName;
	String sourceUserName;
	String sourcePass;
	String columnName;
	String sqlText;
	boolean snapshot;
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
		strSQL += "CASE WHEN j.refresh_type = 'transform' THEN 'Transform' ELSE initcap(j.source_type) END AS source_info,\n";
		strSQL += "CASE WHEN refresh_type = 'transform' THEN SUBSTRING(sql_text, 1, 30)\n";
		strSQL += "ELSE COALESCE((j.target_schema_name || '.' || j.target_table_name), '') END AS target_table_name,\n";
		strSQL += "COALESCE(j.schedule_desc, '') AS schedule_desc,\n";
		strSQL += "COALESCE(schedule_next::text, '') AS schedule_next\n";
 		strSQL += "FROM os.job j\n";
		strSQL += "LEFT OUTER JOIN (\n";
		strSQL += "SELECT id, status FROM (\n";
		strSQL += "SELECT id, status, row_number() over (partition by id order by queue_id desc) as rownum\n";
		strSQL += "FROM os.queue) AS sub WHERE rownum = 1) AS q on j.id = q.id\n";
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(j.refresh_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.target_schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.target_table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_server_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_instance_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_port) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_database_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(j.source_user_name) LIKE '%' || LOWER('" + search +"') || '%'\n";
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

	public static void insertTable(String id, String refreshType, String targetSchemaName, String targetTableName, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceSchemaName, String sourceTableName, String sourceUserName, String sourcePass, String columnName, String sqlText, boolean snapshot, String scheduleDesc) throws SQLException
	{
		refreshType = OutsourcerModel.setSQLString(refreshType);
		targetSchemaName = OutsourcerModel.setSQLString(targetSchemaName);
		targetTableName = OutsourcerModel.setSQLString(targetTableName);
		String strTargetAppendOnly = OutsourcerModel.setSQLString(targetAppendOnly);
		String strTargetCompressed = OutsourcerModel.setSQLString(targetCompressed);
		String strTargetOrientation = OutsourcerModel.setSQLString(targetRowOrientation);
		sourceType = OutsourcerModel.setSQLString(sourceType);
		sourceServerName = OutsourcerModel.setSQLString(sourceServerName);
		sourceInstanceName = OutsourcerModel.setSQLString(sourceInstanceName);
		sourcePort = OutsourcerModel.setSQLInt(sourcePort);
		sourceDatabaseName = OutsourcerModel.setSQLString(sourceDatabaseName);
		sourceSchemaName = OutsourcerModel.setSQLString(sourceSchemaName);
		sourceTableName = OutsourcerModel.setSQLString(sourceTableName);
		sourceUserName = OutsourcerModel.setSQLString(sourceUserName);
		sourcePass = OutsourcerModel.setSQLString(sourcePass);
		columnName = OutsourcerModel.setSQLString(columnName);
		sqlText = OutsourcerModel.setSQLString(sqlText);
		String strSnapshot = OutsourcerModel.setSQLString(snapshot);
		scheduleDesc = OutsourcerModel.setSQLString(scheduleDesc);
		
		String strSQL = "INSERT INTO os.ao_job\n";

		if (id.equals(""))
			strSQL += "(";
		else
			strSQL += "(id, ";
		strSQL += "	refresh_type, target_schema_name, target_table_name,\n";
		strSQL += "	target_append_only, target_compressed, target_row_orientation,\n";
		strSQL += "	source_type, source_server_name, source_instance_name, source_port, source_database_name,\n";
		strSQL += "	source_schema_name, source_table_name, source_user_name, source_pass,\n";
		strSQL += "	column_name, sql_text, snapshot, schedule_desc)\n";
		strSQL += "VALUES";

		//if id is "", then this is a new job and the sequence will create a new id
		if (id.equals(""))
			strSQL += "(";
		else
			strSQL += "(" + id + ", ";

	 	strSQL += "	" + refreshType + ",\n";
		strSQL += "	" + targetSchemaName + ", " + targetTableName + ",\n";
		strSQL += "	" + strTargetAppendOnly + ", " + strTargetCompressed + ", " + targetRowOrientation + ",\n";
		strSQL += "	" + sourceType + ", " + sourceServerName + ", " + sourceInstanceName + ", " + sourcePort + ", " + sourceDatabaseName + ",\n";
		strSQL += "	" + sourceSchemaName + ", " + sourceTableName + ", " + sourceUserName + ", " + sourcePass + ",\n";
		strSQL += "	" + columnName + ",\n";
		strSQL += "	" + sqlText + ",\n";
		strSQL += "	" + strSnapshot + ",\n";
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
		String strSQL = "INSERT INTO os.ao_job\n";
		strSQL += "(id, refresh_type, target_schema_name, target_table_name, target_append_only,\n";
		strSQL += "target_compressed, target_row_orientation, source_type, source_server_name,\n";
		strSQL += "source_instance_name, source_port, source_database_name, source_schema_name,\n";
		strSQL += "source_table_name, source_user_name, source_pass, column_name,\n";
		strSQL += "sql_text, snapshot, schedule_desc, schedule_next, schedule_change,\n"; 
		strSQL += "deleted)\n";
		strSQL += "SELECT id, refresh_type, target_schema_name, target_table_name, target_append_only,\n";
		strSQL += "target_compressed, target_row_orientation, source_type, source_server_name,\n";
		strSQL += "source_instance_name, source_port, source_database_name, source_schema_name,\n";
		strSQL += "source_table_name, source_user_name, source_pass, column_name,\n";
		strSQL += "sql_text, snapshot, schedule_desc, schedule_next, schedule_change,\n"; 
		strSQL += "TRUE AS deleted\n";
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
		String strSQL = "SELECT id, refresh_type, target_schema_name, target_table_name,\n";
		strSQL += "target_append_only, target_compressed, target_row_orientation,\n";
		strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name,\n";
		strSQL += "source_schema_name, source_table_name,\n";
		strSQL += "source_user_name, source_pass,\n";
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
				targetAppendOnly = rs.getBoolean(5);
				targetCompressed = rs.getBoolean(6);
				targetRowOrientation = rs.getBoolean(7);
				sourceType = rs.getString(8);
				sourceServerName = rs.getString(9);
				sourceInstanceName = rs.getString(10);
				sourcePort = rs.getString(11);
				sourceDatabaseName = rs.getString(12);
				sourceSchemaName = rs.getString(13);
				sourceTableName = rs.getString(14);
				sourceUserName = rs.getString(15);
				sourcePass = rs.getString(16);
				columnName = rs.getString(17);
				sqlText = rs.getString(18);
				snapshot = rs.getBoolean(19);
				scheduleDesc = rs.getString(20);
				scheduleNext = rs.getString(21);
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
		String strSQL = "SELECT target_schema_name AS schema_name\n";
		strSQL += "FROM os.job\n";
		strSQL += "GROUP BY target_schema_name\n"; 
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
			String strSQL = "INSERT INTO os.ao_job\n";
			strSQL += "(id, refresh_type, target_schema_name, target_table_name, target_append_only,\n";
			strSQL += "target_compressed, target_row_orientation, source_type, source_server_name,\n";
			strSQL += "source_instance_name, source_port, source_database_name, source_schema_name,\n";
			strSQL += "source_table_name, source_user_name, source_pass, column_name,\n";
			strSQL += "sql_text, snapshot, schedule_desc, schedule_next, schedule_change)\n"; 
			strSQL += "SELECT id, refresh_type, target_schema_name, target_table_name, target_append_only,\n";
			strSQL += "target_compressed, target_row_orientation, source_type, source_server_name,\n";
			strSQL += "source_instance_name, source_port, source_database_name, source_schema_name,\n";
			strSQL += "source_table_name, source_user_name, source_pass, column_name,\n";
			strSQL += "sql_text, snapshot, " + scheduleDesc + " AS schedule_desc, schedule_next, TRUE as schedule_change\n"; 
			strSQL += "FROM os.job\n";
			strSQL += "WHERE target_schema_name = " + gpSchema;

			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable() throws SQLException
	{
		String strSQL = "TRUNCATE os.ao_job";

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
