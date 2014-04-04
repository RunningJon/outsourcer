import java.sql.*;
import org.postgresql.ds.*;

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
			ds = new PGPoolingDataSource();
			ds.setDataSourceName("OutsourcerUIPool");
			ds.setServerName(UI.gpServer);
			ds.setDatabaseName(UI.gpDatabase);
			ds.setPortNumber(UI.gpPort);
			ds.setUser(UI.gpUserName);
			ds.setMaxConnections(10);
		}
		return ds;
	}

	public static Connection getConnection() throws SQLException 
	{
		return getDataSource().getConnection();
	}
}
