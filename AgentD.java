import java.sql.*;
import java.net.*;
import java.io.*;

public class AgentD
{
	private static String myclass = "AgentD";
	public static boolean debug = false;

	public static void main(String[] args) throws Exception
	{

		String method = "main";
		int location = 1000;
		String gpServer = args[0];
		int gpPort = Integer.parseInt(args[1]);
		String gpDatabase = args[2];
		String gpUserName = args[3];

		location = 2000;
		if (debug)
		{
			System.out.println("gpServer:" + gpServer);
			System.out.println("gpPort:" + gpPort);
			System.out.println("gpDatabase:" + gpDatabase);
			System.out.println("gpUsername:" + gpUserName);
		}
	
		try
		{
			location = 3000;	
			Connection conn = CommonDB.connectGP(gpServer, gpPort, gpDatabase, gpUserName);

			location = 3100;
    			Statement stmt = conn.createStatement();

			location = 3200;
			String strSQL = "VACUUM os.job";

			location = 3300;
    			stmt.executeUpdate(strSQL);

			location = 3400;
			strSQL = "SELECT os.fn_start_schedule()";

			location = 3500;
    			stmt.executeQuery(strSQL);

			location = 3600;
			conn.close();

			if (debug)	
				Logger.printMsg("Start to loop....");	
			loadLoop(gpServer, gpPort, gpDatabase, gpUserName);

		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
        }

	private static void loadLoop(String gpServer, int gpPort, String gpDatabase, String gpUserName) throws Exception
	{

		String method = "loadLoop";
		int location = 1000;

		boolean loop = true;

		while (loop)
		{
			try
			{
				location = 2000;
				Connection conn = CommonDB.connectGP(gpServer, gpPort, gpDatabase, gpUserName);

				location = 2100;
    				Statement stmt = conn.createStatement();

				location = 2200;
				String strSQL = "SELECT os.fn_schedule()";

				location = 2300;
    				stmt.executeQuery(strSQL);
				
				location = 2400;
				conn.close();

				location = 3000;
				Thread.sleep(10000);
				
			}
			catch (SQLException ex) 
			{
				Logger.printMsg("(" + method + ":" + location + ":" + ex.getMessage() + ")");
				Thread.sleep(10000);
			}
		}
   	
	}
}

