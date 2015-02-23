import java.sql.*;
import org.postgresql.ds.*;
import java.io.IOException;

public class UIConnectionFactory
{

	// example to use:
	// Connection conn = UIConnectionFactory.getConnection();

	private static PGPoolingDataSource ds; 

	private UIConnectionFactory()
	{
	}

	public static synchronized PGPoolingDataSource getDataSource() throws SQLException
	{
		if (ds == null)
		{
			try
			{
				System.out.println("Creating new connection pool");
				OSProperties.getPropValues(UI.configFile);
				ds = new PGPoolingDataSource();
				ds.setDataSourceName("OutsourcerUIPool");
				ds.setServerName(OSProperties.gpServer);
				ds.setDatabaseName(OSProperties.gpDatabase);
				ds.setPortNumber(OSProperties.gpPort);
				ds.setUser(OSProperties.gpUserName);
				ds.setPassword(OSProperties.gpPassword);
				ds.setMaxConnections(10);
			}
			catch (IOException iox)
			{
				System.out.println("Unable to load config file.  Check environment variables and try again.");
				throw new SQLException(iox.getMessage());
			}
		}
		return ds;
	}

	public static Connection getConnection() throws SQLException 
	{
		try
		{
			return getDataSource().getConnection();
		}
		catch (SQLException ex)
		{
			System.out.println(ex.getMessage());
			System.out.println("gpServer from properties: " + OSProperties.gpServer);
			System.out.println("gpDatabase from properties: " + OSProperties.gpDatabase);
			System.out.println("gpPort from properties: " + OSProperties.gpPort);
			System.out.println("gpUsername from properties: " + OSProperties.gpUserName);
			System.out.println("gpPassword from properties: " + OSProperties.gpPassword);
			throw new SQLException(ex.getMessage());
		}
	}
}
