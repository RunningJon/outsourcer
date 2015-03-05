import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class OutsourcerModel
{

	public static ResultSet getResults(String strSQL) throws SQLException
	{
		try
		{
			Connection conn = UIConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			conn.close();
			return rs;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static ArrayList<String> getStringArray(String strSQL) throws SQLException
	{

		ArrayList<String> stringArray = new ArrayList<String>();
		try
		{
			String output = "";
			String columnValue = "";
			Connection conn = UIConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			while (rs.next())
			{
				output = "";
				for (int i=1; i<numberOfColumns+1; i++)
				{
					columnValue = rs.getString(i);
					if (i == 1)
						output = columnValue;
					else
						output += ';' + columnValue;
				}
				
				stringArray.add(output);
			}
			conn.close();
			return stringArray;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static ArrayList<String> gpArrayToJavaArray(String gpArray) 
	{
		//Array in Greenplum is formatted as a string with { } 
		gpArray = gpArray.substring(1, gpArray.length() - 1);
		
		ArrayList<String> stringArray = new ArrayList<String>(Arrays.asList(gpArray.split(",")));
		return stringArray;
	}

	public static String javaArrayToString(ArrayList<String> stringArray)
	{
		int i = 0;
		String output = "{";
		for (String s : stringArray)
		{
			i++;
			if (i == 1)
				output = "ARRAY[" + s;
			else
				output += "," + s;
		}
		output += "]";

		return output;
	}

	public static void updateTable(String strSQL) throws SQLException
	{
		try
		{
			Connection conn = UIConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(strSQL);
			conn.close();
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}
	public static String setSQLString(boolean columnValue)
	{
		String strColumnValue = "";

		if (columnValue)
			strColumnValue = "true";
		else
			strColumnValue = "false";

		return strColumnValue;
	}
	public static String setSQLString(String columnValue) 
	{
		columnValue = columnValue.replace("'", "\\'");

		if (columnValue.equals(""))
			columnValue = "null";
		else
			columnValue = "'" + columnValue + "'";

		return columnValue;
	}
	public static String setSQLString(int columnValue) 
	{
		String strColumnValue = Integer.toString(columnValue);

		if (strColumnValue.equals(""))
			strColumnValue = "null";

		return strColumnValue;
	}
	public static String setSQLInt(String columnValue)
	{
		if (columnValue.equals(""))
			columnValue = "null";

		return columnValue;
	}
	public static String setSQLTimestamp(Timestamp columnValue)
	{
		String strColumnValue = columnValue.toString();

		return strColumnValue;
	}
}
