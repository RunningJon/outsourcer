import java.util.Map;
import java.sql.*;
import java.net.*;
import java.io.*;

public class ExternalTableModel
{
	String id;
	String type;
	String serverName;
	String instanceName;
	String port;
	String databaseName;
	String userName;
	String pass;

	public static ResultSet getList(String search, String limit, String offset, String sortBy, String sort, String referrer) throws SQLException
	{
		String strSQL = "";
		if (referrer.equals("external"))
		{
			strSQL = "SELECT '<button onclick=\"updateExternalTable(' || id || ', ''update'')\">Update</button>' ||\n";
			strSQL += "	'&nbsp;<button onclick=\"updateExternalTable(' || id || ', ''delete'')\">Delete</button>' ||\n";
			strSQL += "	'&nbsp;<button onclick=\"createJobs(' || id || ')\">Create Jobs</button>' as update_text,\n";
			strSQL += "id, type, server_name, instance_name, port, database_name, user_name, '<i>password</i>' as pass\n ";
		}
		else if (referrer.equals("schema"))
		{
			strSQL = "SELECT '<button onclick=\"schemaGrabWithConnection(' || id || ')\">Grab</button>' as update_text, \n";
			strSQL += "id, type, server_name, instance_name, port, database_name, user_name, '<i>password</i>' as pass\n ";
		}

		strSQL += "FROM os.ext_connection\n";
		if (!search.equals(""))
		{
			strSQL += "WHERE LOWER(type) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(server_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(instance_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(port) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(database_name) LIKE '%' || LOWER('" + search + "') || '%'\n";
			strSQL += "OR LOWER(user_name) LIKE '%' || LOWER('" + search +"') || '%'\n";	
		}
		sortBy = sortBy.toLowerCase();

		if (sortBy.equals("id") || sortBy.equals("type") || sortBy.equals("server_name") || sortBy.equals("instance_name") || sortBy.equals("port") || sortBy.equals("database_name") || sortBy.equals("user_name") || sortBy.equals("pass"))
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

	public static void updateTable(String id, String type, String serverName, String instanceName, String port, String databaseName, String userName, String pass) throws SQLException
	{
		type = OutsourcerModel.setSQLString(type);
		serverName = OutsourcerModel.setSQLString(serverName);
		instanceName = OutsourcerModel.setSQLString(instanceName);
		port = OutsourcerModel.setSQLInt(port);
		databaseName = OutsourcerModel.setSQLString(databaseName);
		userName = OutsourcerModel.setSQLString(userName);
		pass = OutsourcerModel.setSQLString(pass);

		String strSQL = "UPDATE os.ext_connection\n ";
		strSQL += "SET type = " + type + ", \n";
		strSQL += "	server_name = " + serverName + ", \n";
		strSQL += "	instance_name = " + instanceName + ", \n";
		strSQL += "	port = " + port + ", \n";
		strSQL += "	database_name = " + databaseName + ", \n";
		strSQL += "	user_name = " + userName + ", \n";
		strSQL += "	pass = " + pass + " \n";
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

	public static void insertTable(String type, String serverName, String instanceName, String port, String databaseName, String userName, String pass) throws SQLException
	{
		type = OutsourcerModel.setSQLString(type);
		serverName = OutsourcerModel.setSQLString(serverName);
		instanceName = OutsourcerModel.setSQLString(instanceName);
		port = OutsourcerModel.setSQLInt(port);
		databaseName = OutsourcerModel.setSQLString(databaseName);
		userName = OutsourcerModel.setSQLString(userName);
		pass = OutsourcerModel.setSQLString(pass);

		String strSQL = "INSERT INTO os.ext_connection\n ";
		strSQL += "(type, server_name, instance_name, port, database_name, user_name, pass)\n ";
		strSQL += "VALUES (" + type + ", \n";
		strSQL += "	" + serverName + ", \n";
		strSQL += "	" + instanceName + ", \n";
		strSQL += "	" + port + ", \n";
		strSQL += "	" + databaseName + ", \n";
		strSQL += "	" + userName + ", \n";
		strSQL += "	" + pass + ")\n";

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
		String strSQL = "SELECT id, type, server_name, instance_name, port, database_name,\n";
		strSQL += "user_name, pass\n";
		strSQL += "FROM os.ext_connection\n";
		strSQL += "WHERE id = " + aId;

		try
		{
			ResultSet rs = OutsourcerModel.getResults(strSQL);
			while (rs.next())
			{
				id = Integer.toString(rs.getInt(1));
				type = rs.getString(2);
				serverName = rs.getString(3);
				instanceName = rs.getString(4);
				port = rs.getString(5);
				databaseName = rs.getString(6);
				userName = rs.getString(7);
				pass = rs.getString(8);
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
	public static String validate(String type, String serverName, String instanceName, String port, String databaseName, String userName, String pass) 
	{
		String strSQL = "";
		String msg = "";
		try
		{
			Connection conn = null;

			if (type.equals("sqlserver"))
			{
				conn = CommonDB.connectSQLServer(serverName, instanceName, userName, pass);
				msg = SQLServer.validate(conn);
				conn.close();
			}
			else if (type.equals("oracle"))
			{
				int sourcePort = Integer.parseInt(port);
				conn = CommonDB.connectOracle(serverName, databaseName, sourcePort, userName, pass, 10);
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

	public static int createJobs(String id, String databaseName, String sourceSchema, String targetSchema, String refreshType, String scheduleDesc) throws SQLException
	{
		try 
		{
			ExternalTableModel e = getModel(id);
			String aRefreshType = "'" + refreshType + "'";
			String aTargetSchema = "'" + targetSchema + "'";
			String aTargetTable = "";
			String aType = "'" + e.type + "'";
			String aServerName = "'" + e.serverName + "'";
			String aInstanceName = "null";
			if (!(e.instanceName==null))
				aInstanceName = "'" + e.instanceName + "'";
			String aPort = e.port;
			if (e.type.equals("oracle"))
				databaseName = e.databaseName;
			String aDatabaseName = "'" + databaseName + "'";
			String aSchemaName = "'" + sourceSchema + "'";
			String aTableName = "";
			String aUserName = "'" + e.userName + "'";
			String aPassword = "'" + e.pass + "'";

			scheduleDesc = OutsourcerModel.setSQLString(scheduleDesc);

			Connection conn = null;
			ResultSet rs = null;
			int i = 0;
			String strSQL = "";
			String strSQLStart = "INSERT INTO os.job(refresh_type, target, source, schedule_desc)\n";
			strSQLStart += "VALUES (" + aRefreshType + ",\n";
			strSQLStart += "(" + aTargetSchema + ", ";

			if (e.type.equals("sqlserver"))
			{	
				conn = CommonDB.connectSQLServer(e.serverName, e.instanceName, e.userName, e.pass);
				Statement stmt = conn.createStatement();
				rs = SQLServer.getTableList(conn, databaseName, sourceSchema);
			}
			else if (e.type.equals("oracle"))
			{
				int sourcePort = Integer.parseInt(e.port);
				conn = CommonDB.connectOracle(e.serverName, e.databaseName, sourcePort, e.userName, e.pass, 10);
				rs = Oracle.getTableList(conn, sourceSchema);
			}

			while (rs.next())
			{
				i++;
				aTargetTable = "'" + rs.getString(1).toLowerCase() + "'";
				aTableName = "'" + rs.getString(1) + "'";

				strSQL = strSQLStart + aTargetTable + "),\n";
				strSQL += "(" + aType + ", " + aServerName + ", " + aInstanceName + ", " + aPort + ", " + aDatabaseName + ", ";
				strSQL += aSchemaName + ", " + aTableName + ", " + aUserName + ", " + aPassword + "), " + scheduleDesc + ")";

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

}
