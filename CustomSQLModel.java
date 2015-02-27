import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class CustomSQLModel
{
	String id;
	String tableName;
	String columns;
	String sqlText;
	String sourceType;
	String sourceServerName;
	String sourceInstanceName;
	String sourcePort;
	String sourceDatabaseName;
	String sourceUserName;
	String sourcePass;
	
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

	//public static void insertTable(String tableName, String columns, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, int sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) throws SQLException
	public static void insertTable(String tableName, String columns, String sqlText, int extConnectionId) throws SQLException
	{
		tableName = OutsourcerModel.setSQLString(tableName.toLowerCase());
		////////////////////////////////////
		//Need to change this as the UI gets enhanced
		////////////////////////////////////
		columns = OutsourcerModel.setSQLString(columns);
		////////////////////////////////////
		//need to fix this...
		////////////////////////////////////
		////////////////////////////////////
		////////////////////////////////////
		////////////////////////////////////
		sqlText = OutsourcerModel.setSQLString(sqlText);

		String strSQL = "INSERT INTO os.ao_custom_sql\n";
		strSQL += "(table_name, columns, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass)\n";
		strSQL += "SELECT " + tableName + ", " + columns + ", " + sqlText + ", \n";
		strSQL += "source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass\n";
		strSQL += "FROM os.ext_connection\n";
		strSQL += "WHERE id = " + extConnectionId;

		try
		{
			OutsourcerModel.updateTable(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void deleteTable(int id) throws SQLException
	{

		try
		{
			String strSQL = "INSERT INTO os.ao_custom_sql\n";
			strSQL += "(table_name, columns, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, deleted)\n";
			strSQL += "SELECT table_name, columns, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass, TRUE AS deleted\n";
			strSQL += "FROM os.custom_sql\n";
			strSQL += "WHERE id = " + id;
			OutsourcerModel.updateTable(strSQL);

		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
	
	public CustomSQLModel (String aId) throws SQLException
	{
		String strSQL = "SELECT id, table_name, columns, sql_text, source_type, source_server_name, source_instance_name, source_port, source_database_name, source_user_name, source_pass\n";
		strSQL += "FROM os.custom_sql\n";
		strSQL += "WHERE id = " + aId;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = Integer.toString(rs.getInt(1));
				tableName = rs.getString(2);
				columns = rs.getString(3);
				sqlText = rs.getString(4);
				sourceType = rs.getString(5);
				sourceServerName = rs.getString(6);
				sourceInstanceName = rs.getString(7);
				sourcePort = rs.getString(8);
				sourceDatabaseName = rs.getString(9);
				sourceUserName = rs.getString(10);
				sourcePass = rs.getString(11);
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
