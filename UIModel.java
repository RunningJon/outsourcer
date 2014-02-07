import java.util.Map;
import java.util.Random;
import java.sql.*;
import java.util.Date;

public class UIModel
{
	public static void logger(String uri, String sessionID)
	{
		Date now = new Date();
		System.out.println(now.toString() + "|" + sessionID + "|" + uri);
	}

	public static String getSessionID(Map<String, String> header)
	{

		String sessionID = "0";
		String cookies = header.get("cookie");
		if (cookies != null)
		{
			String[] cookieValues = cookies.split(";");
			for (int i = 0; i < cookieValues.length; i++) 
			{
				String[] split = cookieValues[i].trim().split("=");
				if (split.length == 2)
				{
					if (split[0].equals("OutsourcerSessionID"))
					{
						sessionID = split[1];
					}
				}
			}
		}
		return sessionID;
	}

	public static void insertSession(String sessionID) throws SQLException
	{
		try
		{
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
			Statement stmt = conn.createStatement();

			String strSQL = "INSERT INTO os.ao_sessions (session_id) VALUES (" + sessionID + ")";
			stmt.executeUpdate(strSQL);

			conn.close();
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static boolean authenticate(String username, String password) throws SQLException
	{
		boolean authenticated = false;
		try
		{
			username = username.toLowerCase();
			//Using the authServer value to prevent TRUST authentication.  
			Connection conn = CommonDB.connectGP(UI.authServer, UI.gpPort, UI.gpDatabase, username, password);
			String strSQL = "SELECT rolname FROM pg_roles WHERE rolsuper AND rolcanlogin AND rolname = '" + username + "'";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				authenticated = true;
			}

			conn.close();

		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return authenticated;
	}

	public static String setSessionID(Map<String, String> header)
	{
		String sessionID = getSessionID(header);
		if (sessionID.equals("0"))
		{
			Random session = new Random();
			int sessionInt = session.nextInt(1000000);
			sessionID = Integer.toString(sessionInt);
		}
		return sessionID;

	}

	public static boolean keepAlive(String sessionID) throws SQLException
	{
		boolean alive = false;
		boolean activeSession = false;

		try
		{
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
			Statement stmt = conn.createStatement();

			//make sure session is valid
			String strSQL = "SELECT session_id FROM os.ao_sessions WHERE session_id = " + sessionID + " AND expire_date > current_timestamp LIMIT 1";
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				activeSession = true;
			}

			if (activeSession)
			{
				strSQL = "INSERT INTO os.ao_sessions (session_id) VALUES (" + sessionID + ")";
				stmt.executeUpdate(strSQL);

				alive = true;
			}

			conn.close();
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return alive;
	}

	public static String getVersion() 
	{
		String version = "HEAP";

		try
		{
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
			Statement stmt = conn.createStatement();

			String strSQL = "SELECT CASE WHEN position ('HAWQ' IN version()) > 0 THEN 'HAWQ'\n";
			strSQL += "	WHEN position ('HAWQ' IN version()) = 0\n";
			strSQL += "	AND (split_part(split_part(substr(version(), position('Greenplum Database' in version())+19), ' ', 1), '.', 1))::int >= 4\n";
			strSQL += "	AND (split_part(split_part(substr(version(), position('Greenplum Database' in version())+19), ' ', 1), '.', 2))::int >= 3 THEN 'AO'\n";
			strSQL += "	ELSE 'HEAP'\n";
			strSQL += "END";

			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				version = rs.getString(1);
			}

			conn.close();
		}
		catch (SQLException ex)
		{
		
		}
		return version;
	}
}
