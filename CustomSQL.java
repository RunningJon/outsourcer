import java.sql.*;
import java.net.*;
import java.io.*;

public class CustomSQL
{
	private static String myclass = "CustomSQL";
	public static boolean debug = true;
	public static String configFile = "";

	public static void main(String[] args) throws Exception
	{

		String method = "main";
		int location = 1000;
		int argsCount = args.length;

		configFile = args[0];
		String action = args[1];

		location = 2000;
		try
		{
			location = 3000;
			if (action.equals("start"))
			{
				Connection conn = CommonDB.connectGP(configFile);
				GP.customStartAll(conn);
				conn.close();
			} 
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
        }
}

