import java.sql.*;
import java.io.*;

public class GpfdistRunner
{
	public static void main(String[] args) throws Exception
	{
		String osHome = args[0];
		int myPort = jobStart(osHome);
		System.out.println("my port: " + myPort);

		String myMsg = jobStop(osHome, myPort);
		System.out.println("my stop message: " + myMsg);

		myPort = customStart(osHome);
		System.out.println("my port: " + myPort);

		
		myMsg = customStop(osHome, myPort);
		System.out.println("my stop message: " + myMsg);
		
	}

	public static int jobStart(String osHome) throws SQLException
	{
		String myCommand = "jobstart";
		int port = 0;

		try
		{
			port = start(osHome, myCommand);
		}
		catch (Exception ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return port;
	}

	public static String jobStop(String osHome, int port) throws SQLException
	{
		String myCommand = "jobstop";
		String myReturn = "";

		try
		{
			myReturn = stop(osHome, myCommand, port);
		}
		catch (Exception ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return myReturn;
	}

	public static int customStart(String osHome) throws SQLException
	{
		String myCommand = "customstart";
		int port = 0;

		try
		{
			port = start(osHome, myCommand);
		}
		catch (Exception ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return port;
	}

	public static String customStop(String osHome, int port) throws SQLException
	{
		String myCommand = "customstop";
		String myReturn = "";

		try
		{
			myReturn = stop(osHome, myCommand, port);
		}
		catch (Exception ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return myReturn;
	}

	private static int start(String osHome, String myCommand) throws SQLException
	{
		int port = 0;

		try
		{
			String shellCommand = osHome + "/bin/" + myCommand; 
			Process p = Runtime.getRuntime().exec(shellCommand);
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null) 
			{
				port = Integer.parseInt(line);
			}
			if (port == 0)
			{
				//unable to get a gpfdist process so fail
				throw new SQLException("ERROR: Unable to acquire a gpfdist process");
			}
		}
		catch (Exception ex)
		{
			throw new SQLException(ex.getMessage());
		}
		return port;
	}

	private static String stop(String osHome, String myCommand, int port) throws SQLException
	{
		String myReturn = "";

		try
		{
			String shellCommand = osHome + "/bin/" + myCommand; 
			shellCommand += " " + Integer.toString(port);
			Process p = Runtime.getRuntime().exec(shellCommand);
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null) 
			{
				myReturn += line + " ";
				//System.out.println(line);
			}
			return myReturn;
		}
		catch (Exception ex)
		{
			throw new SQLException(ex.getMessage());
		}

	}
}
