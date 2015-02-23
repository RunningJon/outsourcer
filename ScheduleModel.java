import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class ScheduleModel
{
	String description;
	String intervalTrunc;
	String intervalQuantity;

	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort) throws SQLException
	{

		String strSQL = "SELECT '<button onclick=\"updateSchedule(''' || description || ''', ''update'')\">Update</button>' ||\n";
		strSQL += "'&nbsp;<button onclick=\"updateSchedule(''' || description || ''', ''delete'')\">Delete</button>' ||\n";
		strSQL += "'&nbsp;<button onclick=\"updateSchedule(''' || description || ''', ''create'')\">Assign to Jobs</button>' AS manage,\n";
		strSQL += "description, interval_trunc, interval_quantity\n";
		strSQL += "FROM os.schedule\n";
		
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(description) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(interval_trunc) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(interval_quantity) LIKE '%' || LOWER('" + search + "') || '%'\n";
		}
		sortBy = sortBy.toLowerCase();
		if (sortBy.equals("description") || sortBy.equals("interval_trunc") || sortBy.equals("interval_quantity"))
			strSQL += "ORDER BY " + sortBy + " " + sort + "\n";
		else
			strSQL += "ORDER BY description ASC\n";

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

	public static void insertTable(String description, String intervalTrunc, String intervalQuantity) throws SQLException
	{
		description = OutsourcerModel.setSQLString(description);
		intervalTrunc = OutsourcerModel.setSQLString(intervalTrunc);
		intervalQuantity = OutsourcerModel.setSQLString(intervalQuantity);

		String strSQL = "INSERT INTO os.ao_schedule\n";
		strSQL += "(description, interval_trunc, interval_quantity)\n";
		strSQL += "VALUES (" + description + ",\n";
		strSQL += "	" + intervalTrunc + ", " + intervalQuantity + ")";

		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable(String description) throws SQLException
	{
		description = OutsourcerModel.setSQLString(description);

		try
		{
			//update jobs.schedule_desc to null if removing a matching schedule
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
			strSQL += "sql_text, snapshot, null as schedule_desc, schedule_next, schedule_change\n";
			strSQL += "FROM os.job\n";
			strSQL += "WHERE schedule_desc = " + description;
			OutsourcerModel.updateTable(strSQL);

			//remove the schedule
			strSQL = "INSERT INTO os.ao_schedule\n";
			strSQL += "(description, interval_trunc, interval_quantity, deleted)\n";
			strSQL += "SELECT description, interval_trunc, interval_quantity, TRUE AS deleted\n";
			strSQL += "FROM os.schedule\n";
			strSQL += "WHERE description = " + description;
			OutsourcerModel.updateTable(strSQL);

		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
	
	public ScheduleModel (String aDescription) throws SQLException
	{
		aDescription = OutsourcerModel.setSQLString(aDescription);

		String strSQL = "SELECT description, interval_trunc, interval_quantity\n";
		strSQL += "FROM os.schedule\n";
		strSQL += "WHERE description = " + aDescription;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				description = rs.getString(1);
				intervalTrunc = rs.getString(2);
				intervalQuantity = rs.getString(3);
			}
		}
		catch (SQLException ex)
		{
			//do something??
		}

	}

	public static ScheduleModel getModel(String description)
	{
		try
		{
			return new ScheduleModel(description);
		}
		catch (Exception ex)
		{
			return null;
		}
	}

	public static ArrayList<String> getDescriptions() throws SQLException
	{
		String strSQL = "SELECT description\n";
		strSQL += "FROM os.schedule\n";
		strSQL += "ORDER BY description";

		ArrayList<String> descriptions = new ArrayList<String>();

		try
		{
			descriptions = OutsourcerModel.getStringArray(strSQL);
			return descriptions;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
}
