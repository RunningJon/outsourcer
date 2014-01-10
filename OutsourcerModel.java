import java.sql.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;

public class OutsourcerModel
{

	public static ResultSet getResults(String strSQL) throws SQLException
	{
		try
		{
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
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
			int i = 1;
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				stringArray.add(rs.getString(1));
				i++;
			}
			conn.close();
			return stringArray;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static void updateTable(String strSQL) throws SQLException
	{
		try
		{
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(strSQL);
			conn.close();
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
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

	public static String setSQLInt(String columnValue)
	{
		if (columnValue.equals(""))
			columnValue = "null";

		return columnValue;
	}
}
