import fi.iki.elonen.*;
import java.io.IOException;

public class ServerRunnerUI 
{
	public static void run(Class serverClass) 
	{
		try 
		{
			executeInstance((NanoHTTPD) serverClass.newInstance());
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	public static void executeInstance(NanoHTTPD server) 
	{
		try 
		{
			server.start();
		} 
		catch (IOException ioe) 
		{
			System.err.println("Couldn't start server:\n" + ioe);
			System.exit(-1);
		}

		//System.out.println("Server started, Hit Enter to stop.\n");
		System.out.println("Server started.\n");

		boolean loop = true;
		while (loop)
		{
			try
			{
				Thread.sleep(5000);
			}
			catch (Exception e)
			{
				server.stop();
				System.out.println("Server stopped.\n");
			}
		}
	}
}
