import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;

public class QueueModel
{
	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort) throws SQLException
	{
		String strSQL = "SELECT queue_id,\n";
		strSQL += "initcap(status) || CASE WHEN status = 'failed' THEN \n";
		strSQL += "	'&nbsp;<button onclick=\"updateQueue(' || queue_id || ', ''update'')\">Rerun</button>' ||\n";
		strSQL += "	'&nbsp;<button onclick=\"updateQueue(' || queue_id || ', ''delete'')\">Delete</button>'\n";
		strSQL += "	WHEN status = 'queued' THEN'&nbsp;<button onclick=\"updateQueue(' || queue_id || ', ''delete'')\">Delete</button>'\n";
		strSQL += "	WHEN status = 'success' THEN '&nbsp;<button onclick=\"updateQueue(' || queue_id || ', ''update'')\">Rerun</button>'\n";
		strSQL += "	else ''\n";
		strSQL += "	END AS status,\n";
		strSQL += "DATE_TRUNC('second', queue_date) AS queue_date, \n";
		strSQL += "COALESCE((DATE_TRUNC('second', start_date))::text, '') AS start_date,\n";
		strSQL += "COALESCE((DATE_TRUNC('second', end_date))::text, '') AS end_date,\n";
		strSQL += "COALESCE((COALESCE(DATE_TRUNC('second', end_date), CURRENT_TIMESTAMP(0)::timestamp) - DATE_TRUNC('second', start_date))::text, '') AS duration,\n";
		strSQL += "num_rows,\n";
		strSQL += "'<a href=\"jobs?action_type=update&id=' || id || '\">' || id || '</a>', COALESCE((target_schema_name || '.' || target_table_name), '') AS target_table_name, COALESCE(error_message, '') AS error_message\n";
 		strSQL += "FROM os.queue\n";
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(status) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(refresh_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(target_schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(target_table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_server_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_instance_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_port) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_database_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(source_user_name) LIKE '%' || LOWER('" + search +"') || '%'\n";
			strSQL += "OR LOWER(column_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(sql_text) LIKE '%' || LOWER('" + search + "') || '%'\n";
		}
		sortBy = sortBy.toLowerCase();
		if (sortBy.equals("queue_id") || sortBy.equals("status") || sortBy.equals("queue_date") || sortBy.equals("start_date") || sortBy.equals("end_date") || sortBy.equals("duration") || sortBy.equals("num_rows") || sortBy.equals("id") || sortBy.equals("target_table_name") || sortBy.equals("error_message"))
			strSQL += "ORDER BY " + sortBy + " " + sort + ", queue_id DESC\n";
		else
			strSQL += "ORDER BY status ASC, queue_id DESC\n";

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

	public static void updateTable(String queueId) throws SQLException 
	{
		String strSQL = "INSERT INTO os.ao_queue\n";
		strSQL += "(queue_id, status, queue_date, start_date, end_date, error_message,\n";
		strSQL += "num_rows, id, refresh_type, target_schema_name, target_table_name,\n";
		strSQL += "target_append_only, target_compressed, target_row_orientation,\n";
		strSQL += "source_type, source_server_name, source_instance_name, source_port,\n"; 
		strSQL += "source_database_name, source_schema_name, source_table_name,\n";
		strSQL += "source_user_name, source_pass, column_name, sql_text, snapshot)\n";
		strSQL += "SELECT queue_id, 'queued' as status, queue_date, NULL AS start_date, NULL AS end_date, NULL AS error_message,\n";
		strSQL += "0 AS num_rows, id, refresh_type, target_schema_name, target_table_name,\n";
		strSQL += "target_append_only, target_compressed, target_row_orientation,\n";
		strSQL += "source_type, source_server_name, source_instance_name, source_port,\n"; 
		strSQL += "source_database_name, source_schema_name, source_table_name,\n";
		strSQL += "source_user_name, source_pass, column_name, sql_text, snapshot\n";
		strSQL += "FROM os.queue\n";
		strSQL += "WHERE queue_id = " + queueId + "\n";
		strSQL += "AND status <> 'processing'";


		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void insertTable(String id) throws SQLException
	{
		String strSQL = "SELECT os.fn_queue(" + id + ")";

		try
		{
			OutsourcerModel.getResults(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void insertTableAll() throws SQLException
	{
		String strSQL = "SELECT os.fn_queue_all()";

		try
		{
			OutsourcerModel.getResults(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable(String queueID) throws SQLException
	{
		String strSQL = "INSERT INTO os.ao_queue\n";
		strSQL += "(queue_id, status, queue_date, start_date, end_date, error_message,\n";
		strSQL += "num_rows, id, refresh_type, target_schema_name, target_table_name,\n";
		strSQL += "target_append_only, target_compressed, target_row_orientation,\n";
		strSQL += "source_type, source_server_name, source_instance_name, source_port,\n"; 
		strSQL += "source_database_name, source_schema_name, source_table_name,\n";
		strSQL += "source_user_name, source_pass, column_name, sql_text, snapshot,\n";
		strSQL += "deleted)\n";
		strSQL += "SELECT queue_id, status, queue_date, start_date, end_date, error_message,\n";
		strSQL += "num_rows, id, refresh_type, target_schema_name, target_table_name,\n";
		strSQL += "target_append_only, target_compressed, target_row_orientation,\n";
		strSQL += "source_type, source_server_name, source_instance_name, source_port,\n"; 
		strSQL += "source_database_name, source_schema_name, source_table_name,\n";
		strSQL += "source_user_name, source_pass, column_name, sql_text, snapshot,\n";
		strSQL += "TRUE AS deleted\n";
		strSQL += "FROM os.queue\n";
		strSQL += "WHERE queue_id = " + queueID + "\n";
		strSQL += "AND status IN ('queued', 'failed')\n";

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
