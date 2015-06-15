import java.util.Map;
import java.util.Random;
import java.sql.*;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			String expireDate = sdf.format(new Date(System.currentTimeMillis()+15*60*1000));

			File file = new File(UI.sessions);
			if(!file.exists())
			{
				file.createNewFile();
			}
			FileWriter writer = new FileWriter(file, true);
			writer.write(sessionID + "|'" + expireDate + "'\n");
			writer.flush();
			writer.close();

		}
		catch (IOException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static boolean authenticate(String username, String password) throws SQLException
	{
		boolean authenticated = false;
		username = username.toLowerCase();
		try
		{
			//don't use the pool to authenticate
			Connection conn = CommonDB.connectGP(UI.configFile, username, password);
			
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
			Connection conn = UIConnectionFactory.getConnection();
			Statement stmt = conn.createStatement();

			//make sure session is valid
			String strSQL = "SELECT session_id FROM os.sessions WHERE session_id = " + sessionID + " AND expire_date > current_timestamp AT TIME ZONE 'UTC' LIMIT 1";
			ResultSet rs = stmt.executeQuery(strSQL);
			while (rs.next())
			{
				activeSession = true;
			}
			conn.close();

			if (activeSession)
			{
				insertSession(sessionID);
				alive = true;
			}

		}
		catch (SQLException ex)
		{
			System.out.println(ex.getMessage());
			throw new SQLException(ex.getMessage());
		}
		return alive;
	}

	public static String getVersion() 
	{
		String version = "HEAP";

		try
		{
			Connection conn = UIConnectionFactory.getConnection();
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
