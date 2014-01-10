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
		String sessionID = header.get("cookie");
		if (sessionID == null)
			sessionID = "0";

		return sessionID;
	}

	public static void insertSession(String sessionID) throws SQLException
	{
		try
		{
			Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, UI.gpUserName);
			Statement stmt = conn.createStatement();

			String strSQL = "DELETE FROM os.sessions WHERE session_id = " + sessionID;
			stmt.executeUpdate(strSQL);

			strSQL = "INSERT INTO os.sessions (session_id) values (" + sessionID + ")";
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
			//Connection conn = CommonDB.connectGP(UI.gpServer, UI.gpPort, UI.gpDatabase, username, password);
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

			//remove expired sessions
			String strSQL = "DELETE FROM os.sessions WHERE expire_date < current_timestamp";
			int rowCount = stmt.executeUpdate(strSQL);

			//if any expired sessions, vacuum the table for housekeeping
			if (rowCount > 0)
			{
				strSQL = "VACUUM os.sessions";
				stmt.executeUpdate(strSQL);
			}

			//make sure session is valid
			strSQL = "SELECT session_id FROM os.sessions WHERE session_id = " + sessionID + " AND expire_date > current_timestamp";
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				activeSession = true;
			}

			if (activeSession)
			{
				strSQL = "DELETE FROM os.sessions WHERE session_id = " + sessionID;
				stmt.executeUpdate(strSQL);

				strSQL = "INSERT INTO os.sessions (session_id) values (" + sessionID + ")";
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

}
