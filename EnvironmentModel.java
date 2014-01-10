import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;

public class EnvironmentModel
{
	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort) throws SQLException
	{
		String strSQL = "SELECT CASE WHEN restart IS FALSE\n";
		strSQL += "	THEN '<a href=\"environment?action_type=' || name || '\">' || description || ' (Dynamic)</a>'\n";
		strSQL += "	ELSE name || ' (Set ' || UPPER(name) || ' in .bashrc and then restart Daemon)'\n";
		strSQL += "END as name,\n";
		strSQL += "value\n";
		strSQL += "FROM (SELECT name, name as description, value, restart FROM os.variables\n";
		strSQL += "UNION\n";
		strSQL += "SELECT 'Daemon' as name, 'Queue Daemon' as description,\n";
		strSQL += "status as value,\n";
		strSQL += "false as restart\n";
		strSQL += "FROM os.osstatus\n";
		strSQL += "UNION\n";
		strSQL += "SELECT 'Agent' as name, 'Scheduler Daemon' as description,\n";
		strSQL += "status as value,\n";
		strSQL += "false as restart\n";
		strSQL += "FROM os.agentstatus\n";
		strSQL += ") as sub\n";

		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(value) LIKE '%' || LOWER('" + search + "') || '%'\n";
		}

		sortBy = sortBy.toLowerCase();
		if (sortBy.equals("name") || sortBy.equals("value") || sortBy.equals("restart"))
			strSQL += "ORDER BY " + sortBy + " " + sort + "\n";
		else
			strSQL += "ORDER BY name asc\n";

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

	public static String getStatus() throws SQLException
	{
		String strSQL = "SELECT status FROM os.osstatus";
		String status = "Down";
		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				status = rs.getString(1);
			}
			return status;
		}	
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static String getAgentStatus() throws SQLException
	{
		String strSQL = "SELECT status FROM os.agentstatus";
		String status = "Down";
		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				status = rs.getString(1);
			}
			return status;
		}	
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static String getVariable(String name) throws SQLException
	{
		String strSQL = "SELECT value FROM os.variables WHERE name = '" + name + "'";
		String value = "";
		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				value = rs.getString(1);
			}
			return value;
		}	
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}

	}

	public static void setVariable(String name, String value) throws SQLException
	{
		String strSQL = "UPDATE os.variables SET value = '" + value + "' WHERE name = '" + name + "'";
		
		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void setStatus(String status) throws SQLException
	{
		String strSQL = "";
		if (status.equals("Down"))
		{
			strSQL = "SELECT * from os.osstart";	
		}
		else if (status.equals("Up"))
		{
			strSQL = "SELECT * from os.osstop";	
		}

		try
		{
			OutsourcerModel.getResults(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}	

	public static void setAgentStatus(String status) throws SQLException
	{
		String strSQL = "";
		if (status.equals("Down"))
		{
			strSQL = "SELECT * from os.agentstart";	
		}
		else if (status.equals("Up"))
		{
			strSQL = "SELECT * from os.agentstop";	
		}

		try
		{
			OutsourcerModel.getResults(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}	
}
