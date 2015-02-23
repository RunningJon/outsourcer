import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

public class OSProperties
{
	public static String osServer = "";
	public static int osPort = 0;
	public static String gpServer = "";
	public static String gpDatabase = "";
	public static int gpPort = 0;
	public static String gpUserName = "";
	public static String gpPassword = "";

	public static void main(String[] args) throws Exception
	{
		String configFile = args[0];
		getPropValues(configFile);

		System.out.println("osServer: " + osServer);
		System.out.println("osPort: " + osPort);
		System.out.println("gpServer: " + gpServer);
		System.out.println("gpDatabase: " + gpDatabase);
		System.out.println("gpPort: " + gpPort);
		System.out.println("gpUserName: " + gpUserName);
		System.out.println("gpPassword: " + gpPassword);
		
	} 

	public static void getPropValues(String configFile) throws IOException
	{
		try
		{
 
			Properties prop = new Properties();
			InputStream inputStream = new FileInputStream(configFile);
 
			if (inputStream != null) 
			{
				prop.load(inputStream);
			} 
			else 
			{
				throw new FileNotFoundException(configFile + " not found!");
			}
 
			// set the property values
			osServer = prop.getProperty("osserver");
			String strOsPort = prop.getProperty("osport");
			if (strOsPort != null)
				osPort = Integer.parseInt(strOsPort);
			gpServer = prop.getProperty("gpserver");
			gpDatabase = prop.getProperty("gpdatabase");
			String strGpPort = prop.getProperty("gpport");
			if (strGpPort != null)
				gpPort = Integer.parseInt(strGpPort);
			gpUserName = prop.getProperty("gpusername");
			gpPassword = prop.getProperty("gppassword");

		}
		catch (IOException iex)
		{
			System.out.println("Unable to read config.properties located here: " + configFile);
			System.out.println(iex.getMessage());
		}
	}
}

