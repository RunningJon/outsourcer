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
		strSQL += "	END as status,\n";
		strSQL += "DATE_TRUNC('second', queue_date) as queue_date, \n";
		strSQL += "COALESCE((DATE_TRUNC('second', start_date))::text, '') as start_date,\n";
		strSQL += "COALESCE((DATE_TRUNC('second', end_date))::text, '') as end_date,\n";
		strSQL += "COALESCE((COALESCE(DATE_TRUNC('second', end_date), CURRENT_TIMESTAMP(0)::timestamp) - DATE_TRUNC('second', start_date))::text, '')  as duration,\n";
		strSQL += "num_rows,\n";
		strSQL += "'<a href=\"jobs?action_type=update&id=' || id || '\">' || id || '</a>', (target).schema_name || '.' || (target).table_name as target_table_name, COALESCE(error_message, '') as error_message\n";
 		strSQL += "FROM os.queue\n";
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(status) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(refresh_type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((target).schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((target).table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).server_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).instance_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).port) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).database_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).schema_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).table_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER((source).user_name) LIKE '%' || LOWER('" + search +"') || '%'\n";
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
		String strSQL = "UPDATE os.queue\n";
		strSQL += "SET status = 'queued', error_message = null, start_date = null, end_date = null, num_rows = 0\n";
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
		String strSQL = "DELETE\n";
		strSQL += "FROM os.queue\n";
		strSQL += "WHERE queue_id = " + queueID + "\n";
		strSQL += "AND status IN ('queued', 'failed')";

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
