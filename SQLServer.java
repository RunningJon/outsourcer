import java.sql.*;
import java.net.*;
import java.io.*;

public class SQLServer
{
	private static String myclass = "SQLServer";
	public static boolean debug = false;

	private static String getSQLInject(Connection conn, String sourceDatabase) throws SQLException
	{
		String method = "getSQLInject";
		int location = 1000;

		try
		{
			location = 2000;
			String SQLInject = "SET TRANSACTION ISOLATION LEVEL READ "; 
			String level = "UNCOMMITTED";

			location = 2100;
			String strSQL = "SELECT NULL \n" +
					"FROM [" + sourceDatabase + "].sys.databases \n" +
					"WHERE name = '" + sourceDatabase + "' \n" + 
					"     AND is_read_committed_snapshot_on = 1";
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2200;
			Statement stmt = conn.createStatement();

			location = 2300;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        while (rs.next())
			{
				level = "COMMITTED";
			}
			SQLInject = SQLInject + level + " \n";

			location = 2400;
			return SQLInject;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	private static String getSQLForColumns(String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException 
	{
		String method = "getSQLForColumns";
		int location = 1000;

		try 
		{
			location = 2000;
			String strSQL = "SELECT '[' + COLUMN_NAME + ']' AS COLUMN_NAME \n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.COLUMNS \n" +
					"WHERE TABLE_NAME = '" + sourceTable + "' \n" + 
					"     AND TABLE_SCHEMA = '" + sourceSchema + "' \n" +
					"     AND DATA_TYPE NOT IN ('binary', 'image', 'timestamp', 'xml', 'varbinary', 'text', 'ntext', 'sql_variant') \n" +
					"ORDER BY ORDINAL_POSITION";
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2200;
			return strSQL;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getSQLForData(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable, String refreshType, String appendColumnName, int appendColumnMax) throws SQLException 
	{
		String method = "getSQLForData";
		int location = 1000;

		try 
		{
			//Create SQL Statement for getting the column names
			location = 2000;
			String strSQL = getSQLForColumns(sourceDatabase, sourceSchema, sourceTable);
			
			//Execute SQL Statement AND format columns for next SELECT statement
			location = 2100;
			String columnSQL = CommonDB.formatSQLForColumnName(conn, strSQL);

			//Create SQL Statement for retrieving data FROM table
			location = 2200;
			strSQL = getSQLInject(conn, sourceDatabase);

			location = 2300;
			strSQL = strSQL + "SELECT " + columnSQL + "\n " +
				"FROM [" + sourceDatabase + "].[" + sourceSchema + "].[" + sourceTable + "] \n";
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);
			
			//Add filter for append refreshType
			if (debug)
				Logger.printMsg("About to refreshType: " + refreshType);

			location = 2400;
			if (refreshType.equals("append"))
			{
				location = 2500;
				strSQL = strSQL + "WHERE \"" + appendColumnName + "\" > " + appendColumnMax;  //greater than what is in GP currently
			}

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2600;
			return strSQL;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getSQLForCreateTable(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException 
	{
		String method = "getSQLForCreateTable";
		int location = 1000;

		try
		{
			location = 2000;
                        String strSQL = "SELECT sub.column_name, \n" +
                        	        "     CASE WHEN sub.datatype = 'char' THEN 'character' \n" +
                                        "          WHEN sub.datatype = 'nchar' THEN 'character' \n" +
                                        "          WHEN sub.datatype = 'datetime' THEN 'timestamp' \n" +
                                        "          WHEN sub.datatype = 'decimal' THEN 'numeric' \n" +
                                        "          WHEN sub.datatype = 'float' THEN 'float8' \n" +
                                        "          WHEN sub.datatype = 'int' THEN 'integer' \n" +
                                        "          WHEN sub.datatype = 'nvarchar' THEN 'varchar' \n" +
                                        "          WHEN sub.datatype = 'smalldatetime' THEN 'timestamp' \n" +
                                        "          WHEN sub.datatype = 'smallmoney' THEN 'numeric' \n" +
                                        "          WHEN sub.datatype = 'money' THEN 'numeric' \n" +
                                        "          WHEN sub.datatype = 'sysname' THEN 'varchar' \n" +
                                        "          WHEN sub.datatype = 'tinyint' THEN 'smallint' \n" +
                                        "          WHEN sub.datatype = 'uniqueidentifier' THEN 'varchar(36)' \n" +
                                        "          ELSE sub.datatype END + CASE WHEN sub.datatype in ('nchar', 'char', 'varchar', 'nvarchar', 'sysname') \n" +
				  	"                                       AND sub.length <> -1 THEN '(' + cast(sub.length as varchar) + ')' \n" +
                                        "                                       ELSE '' END as datatype \n" +
                                        "FROM (SELECT REPLACE(REPLACE(LOWER(sc.name), '\"', ''), '.', '_') column_name, \n" +
					"             st.name as datatype, \n" + 
                                        "             sc.max_length as length, \n" +
                                        "             sc.column_id \n" +
                                        "      FROM [" + sourceDatabase + "].sys.objects so \n" +
                                        "      JOIN [" + sourceDatabase + "].sys.columns sc ON so.object_id = sc.object_id \n" +
                                        "      JOIN [" + sourceDatabase + "].sys.schemas su ON so.schema_id = su.schema_id \n" +
                                        "      JOIN [" + sourceDatabase + "].sys.types st ON sc.system_type_id = st.system_type_id AND st.system_type_id = st.user_type_id \n" +
                                        "      WHERE so.type in ('U', 'V') \n" +
                                        "          AND su.name = '" + sourceSchema + "' \n" +
                                        "          AND so.name = '" + sourceTable + "' ) sub \n" +
                                        "WHERE sub.datatype not in ('binary', 'image', 'timestamp', 'xml', 'varbinary', 'text', 'ntext', 'sql_variant') \n" +
                                        "ORDER BY sub.column_id";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2100;
			return strSQL;

		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getSQLForDistribution(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException 
	{
		String method = "getSQLForDistribution";
		int location = 1000;

		try
		{

			location = 2000;
			String strSQL = "SELECT '\"' + LOWER(c.name) + '\"' as COLUMN_NAME \n" +
					"FROM [" + sourceDatabase + "].sys.objects o \n" +
					"JOIN [" + sourceDatabase + "].sys.schemas s ON o.schema_id = s.schema_id \n" +
					"JOIN [" + sourceDatabase + "].sys.indexes i ON o.object_id = i.object_id \n" +
					"JOIN [" + sourceDatabase + "].sys.index_columns ic ON o.object_id = ic.object_id AND i.index_id = ic.index_id \n" +
					"JOIN [" + sourceDatabase + "].sys.columns c ON o.object_id = c.object_id AND ic.column_id = c.column_id \n" +
					"WHERE o.name = '" + sourceTable + "' \n" +
					"	AND s.name = '" + sourceSchema + "' \n" + 
					"	AND i.is_primary_key = 1 \n" +
					"ORDER BY ic.key_ordinal";
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2100;
			return strSQL;

		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String validate(Connection conn) throws SQLException
	{
		String method = "validate";
		int location = 1000;

		try
		{
			location = 2000;
			String msg = "";
			String strSQL = "SELECT @@version";

			location = 2200;
			Statement stmt = conn.createStatement();

			location = 2300;
                        ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
                        while (rs.next())
			{
				msg = "Success!";
			}

			location = 2500;
			return msg;
                }

                catch (SQLException ex)
                {
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
                }

        }

	public static int getMaxId(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable, String columnName) throws SQLException
	{
		String method = "getMaxId";
		int location = 1000;

		try
		{
                
			location = 2000;
			int maxId = -1;
			String strSQL = getSQLInject(conn, sourceDatabase);

			location = 2100;
			strSQL = strSQL + "SELECT MAX([" + columnName + "]) \n" +
				"FROM [" + sourceDatabase + "].[" + sourceSchema + "].[" + sourceTable + "]";

			location = 2200;
			Statement stmt = conn.createStatement();

			location = 2300;
                        ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
                        while (rs.next())
			{
				maxId = rs.getInt(1);
			}

			location = 2500;
			return maxId;
                }

                catch (SQLException ex)
                {
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
                }

        }

	public static void configureReplication(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable, String appendColumnName) throws SQLException 
	{
		String method = "configureReplication";
		int location = 1000;

		try
		{
			location = 2000;
			String sourceType = "sqlserver";
			String replTable = GP.getReplTableName(sourceType, sourceTable);

			//1. drop triggers if exists
			//2. drop replTable if exists
			//3. create replTable
			//4. create triggers

			location = 2100;
                        Statement stmt = conn.createStatement();

			String strSQL = "USE [" + sourceDatabase + "]";
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2200;
			stmt.executeUpdate(strSQL);

			/////////////////////////////
			//1.  drop triggers if exists
			/////////////////////////////
			location = 3100;
                        strSQL = "IF EXISTS (SELECT NULL FROM [" + sourceDatabase + "].[sys].[objects] \n" +
				"WHERE name = 'T_" + replTable + "_I' AND type = 'TR') \n " + 
				"	DROP TRIGGER [" + sourceSchema + "].[T_" + replTable + "_I]";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 3200;
                        stmt.executeUpdate(strSQL);

			location = 3300;
                        strSQL = "IF EXISTS (SELECT NULL FROM [" + sourceDatabase + "].[sys].[objects] \n" +
				"WHERE name = 'T_" + replTable + "_U' AND type = 'TR') \n " + 
				"	DROP TRIGGER [" + sourceSchema + "].[T_" + replTable + "_U] \n";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 3400;
                        stmt.executeUpdate(strSQL);

			location = 3500;
                        strSQL = "IF EXISTS (SELECT NULL FROM [" + sourceDatabase + "].[sys].[objects] \n" +
				"WHERE name = 'T_" + replTable + "_D' AND type = 'TR') \n " + 
				"	DROP TRIGGER [" + sourceSchema + "].[T_" + replTable + "_D] \n";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 3600;
                        stmt.executeUpdate(strSQL);

			/////////////////////////////
			//2. drop replTable if exists
			/////////////////////////////
			location = 4000;
                        strSQL = "IF EXISTS (SELECT NULL FROM [" + sourceDatabase + "].[sys].[objects] \n" +
				"WHERE object_id = OBJECT_ID(N'[" + sourceDatabase + "].[" + sourceSchema + "].[" + replTable + "]" + "') AND type = (N'U')) \n" +
				"	DROP TABLE [" + sourceDatabase + "].[" + sourceSchema + "].[" + replTable + "]";

			location = 4100;
                        stmt.executeUpdate(strSQL);
			
			/////////////////////////////
			//3. create replTable
			/////////////////////////////
			location = 5000;
			strSQL = "SELECT COLUMN_NAME, \n" +
				"	DATA_TYPE + \n" +
				"	CASE WHEN DATA_TYPE in ('nchar', 'char', 'varchar', 'nvarchar', 'sysname') \n" +
				"	AND CHARACTER_MAXIMUM_LENGTH <> -1 \n" + 
				"	THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(8000)) + ')' \n" +
				"	WHEN DATA_TYPE in ('nchar', 'char', 'varchar', 'nvarchar') \n" +
				"	AND CHARACTER_MAXIMUM_LENGTH = -1 \n" + 
				"	THEN '(MAX)' \n" + 
				"	ELSE '' END + \n" +
				"	CASE WHEN IS_NULLABLE = 'NO' THEN ' NOT NULL' ELSE ' NULL' END AS ATTRIBUTES \n" +
				"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.COLUMNS \n" +
				"WHERE TABLE_CATALOG = '" + sourceDatabase + "' \n " +
				"	AND TABLE_SCHEMA = '" + sourceSchema + "' \n" +
				"	AND TABLE_NAME = '" + sourceTable + "' \n" +
				"	AND DATA_TYPE NOT IN ('binary', 'image', 'timestamp', 'xml', 'varbinary', 'text', 'ntext', 'sql_variant') \n" +
				"ORDER BY ORDINAL_POSITION";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

                        //Get Column names
                        String columnName = "";
                        String attributes = "";
			String columns = "";

			location = 5200;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        while (rs.next())
                        {
                                columnName = rs.getString(1);
				attributes = rs.getString(2);

                                if (rs.getRow() == 1)
                                {
					location = 5300;
					strSQL = "CREATE TABLE [" + sourceDatabase + "].[" + sourceSchema + "].[" + replTable + "] \n" +
						"([" + appendColumnName + "] bigint IDENTITY NOT NULL PRIMARY KEY, \n" +
						"change_type char(1) not null, \n";

					strSQL = strSQL + "[" + columnName + "] " + attributes;

					columns = "	[" + columnName + "]";
                                }
				else
				{
					strSQL = strSQL + ", \n [" + columnName + "] " + attributes;

					columns = columns  + ", \n	[" + columnName + "]";
				}
                        }

			location = 5400;
			strSQL = strSQL + ");";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 5500;
                        stmt.executeUpdate(strSQL);

			/////////////////////////////
			//4. create triggers
			/////////////////////////////
			location = 6000;
			strSQL = "CREATE TRIGGER [" + sourceSchema + "].[T_" + replTable + "_I] \n" +
				"ON [" + sourceSchema + "].[" + sourceTable + "] \n" +
				"AFTER INSERT AS \n" +
				"BEGIN \n" +
				"SET NOCOUNT ON; \n" +
				"INSERT INTO [" + sourceDatabase + "].[" + sourceSchema + "].[" + replTable + "] \n" +
				"	(change_type, \n" + 
					columns + ") \n" +
				"SELECT 'I', \n" +
					columns + " \n" +
				"FROM INSERTED \n" +
				"END";
			
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 6100;
			stmt.executeUpdate(strSQL);

			location = 7000;
			strSQL = "CREATE TRIGGER [" + sourceSchema + "].[T_" + replTable + "_U] \n" +
				"ON [" + sourceSchema + "].[" + sourceTable + "] \n" +
				"AFTER UPDATE AS \n" +
				"BEGIN \n" +
				"SET NOCOUNT ON; \n" +
				"INSERT INTO [" + sourceDatabase + "].[" + sourceSchema + "].[" + replTable + "] \n" +
				"	(change_type, \n" + 
					columns + ") \n" +
				"SELECT 'U', \n" +
					columns + " \n" +
				"FROM INSERTED \n" +
				"END";
			
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 7100;
			stmt.executeUpdate(strSQL);

			location = 8000;
			strSQL = "CREATE TRIGGER [" + sourceSchema + "].[T_" + replTable + "_D] \n" +
				"ON [" + sourceSchema + "].[" + sourceTable + "] \n" +
				"AFTER DELETE AS \n" +
				"BEGIN \n" +
				"SET NOCOUNT ON; \n" +
				"INSERT INTO [" + sourceDatabase + "].[" + sourceSchema + "].[" + replTable + "] \n" +
				"	(change_type, \n" + 
					columns + ") \n" +
				"SELECT 'D', \n" +
					columns + " \n" +
				"FROM DELETED \n" +
				"END";
			
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 8100;
			stmt.executeUpdate(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static boolean snapshotSourceTable(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException
	{
		String method = "snapshotSourceTable";
		int location = 1000;

		try
		{
			location = 2000;
			String sourceType = "sqlserver";
			String replTable = GP.getReplTableName(sourceType, sourceTable);
			boolean found = false;

			location = 2100;
                        Statement stmt = conn.createStatement();
	
			//SQL Server has three distinct triggers instead of just one
			location = 2300;
			String strSQL = "SELECT COUNT(*) AS counter \n" +
					"FROM [" + sourceDatabase + "].sys.objects trig \n" +
					"JOIN [" + sourceDatabase + "].sys.objects tab on trig.parent_object_id = tab.object_id \n" +
					"JOIN [" + sourceDatabase + "].sys.schemas sch on trig.schema_id = sch.schema_id \n" +
					"WHERE trig.type = 'TR' \n" +
					"	AND sch.name = '" + sourceSchema + "' \n" +
					"	AND tab.name = '" + sourceTable + "' \n" +
					"	AND trig.name in ('T_" + replTable + "_I', 'T_" + replTable + "_U', 'T_" + replTable + "_D')"; 
 
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2400;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        while (rs.next())
                        {
				if (rs.getInt(1) == 3)
				{
					location = 2500;
					found = true;
				}	
			}

			if (found)
			{
				location = 2600;
				found = false;

				location = 2700;
				strSQL = "SELECT NULL \n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.TABLES \n" +
					"WHERE TABLE_SCHEMA = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + replTable + "' \n" +
					"	AND TABLE_TYPE = 'BASE TABLE'";
 
				if (debug)
                	                Logger.printMsg("Executing SQL: " + strSQL);

				location = 2800;
				rs = stmt.executeQuery(strSQL);

				while (rs.next())
				{
					location = 2900;
					found = true;
				}
			}

			location = 3000;
			return found;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void checkSourceSchema(Connection conn, String sourceDatabase, String sourceSchema) throws SQLException
	{
		String method = "checkSourceSchema";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "SELECT NULL \n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.SCHEMATA \n" +
					"WHERE SCHEMA_NAME = '" + sourceSchema + "'";

			if (debug)
               	                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2300;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				location = 2500;
				found = true;
			}

			if (!(found))
			{
				throw new SQLException("SourceSchema: \"" + sourceSchema + "\" NOT FOUND!");
			} 
		}
		
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void checkSourceTable(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException
	{
		String method = "checkSourceTable";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "SELECT NULL \n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.TABLES \n" +
					"WHERE TABLE_SCHEMA = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + sourceTable + "'";

			if (debug)
               	                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2300;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				location = 2500;
				found = true;
			}

			if (!(found))
			{
				throw new SQLException("SourceTable: \"" + sourceSchema + "\".\"" + sourceTable + "\" NOT FOUND!");
			} 
		}

		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void checkAppendColumnName(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable, String appendColumnName) throws SQLException
	{
		String method = "checkAppendColumnName";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "SELECT NULL \n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.COLUMNS \n" +
					"WHERE TABLE_SCHEMA = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + sourceTable + "' \n" +
					"	AND COLUMN_NAME = '" + appendColumnName + "'";

			if (debug)
               	                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2300;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				location = 2500;
				found = true;
			}

			if (!(found))
			{
				throw new SQLException("AppendColumnName: \"" + appendColumnName + "\" does not exist in the SourceTable: \"" + sourceSchema + "\".\"" + sourceTable + "\"!");
			} 
		}

		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
	
	public static void checkReplAppendColumnName(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable, String appendColumnName) throws SQLException
	{
		String method = "checkReplAppendColumnName";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "SELECT NULL \n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.COLUMNS \n" +
					"WHERE TABLE_SCHEMA = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + sourceTable + "' \n" +
					"	AND COLUMN_NAME = '" + appendColumnName + "'";

			if (debug)
               	                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2300;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				location = 2500;
				found = true;
			}

			//For replication, you need to specify a NEW column that doesn't already exist
			// If found, throw an error
			if (found)
			{
				throw new SQLException("Replication ColumnName: \"" + appendColumnName + "\" exists in the SourceTable: \"" + sourceSchema + "\".\"" + sourceTable + "\"!  Provide a NEW column name for Replication that doesn't exist in the SourceTable.");
			} 
		}

		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static void checkReplPrimaryKey(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException
	{
		String method = "checkReplPrimaryKey";
		int location = 1000;

		try
		{
			location = 2000;
			boolean found = false;

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			String strSQL = "SELECT NULL \n" + 
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.TABLE_CONSTRAINTS \n " + 
					"WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' \n" +
					"    AND TABLE_SCHEMA = '" + sourceSchema + "' \n" +
					"    AND TABLE_NAME = '" + sourceTable + "'";
			if (debug)
               	                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2300;
			ResultSet rs = stmt.executeQuery(strSQL);

			location = 2400;
			while (rs.next())
			{
				location = 2500;
				found = true;
			}

			if (!(found))
			{
				throw new SQLException("Primary key required on SourceTable: \"" + sourceSchema + "\".\"" + sourceTable + "\" for replication!");
			} 
		}

		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static ResultSet getDatabaseList(Connection conn) throws SQLException
	{
		String method = "getDatabaseList";
		int location = 1000;

		try
		{
			location = 2000;
			String strSQL = "SELECT name FROM sys.databases WHERE HAS_DBACCESS(name) = 1 ORDER BY name";

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			ResultSet rs = stmt.executeQuery(strSQL);
		
			location = 3000;
			return rs;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static ResultSet getSchemaList(Connection conn, String sourceDatabase) throws SQLException
	{
		String method = "getSchemaList";
		int location = 1000;

		try
		{
			location = 2000;
			String strSQL = "SELECT DISTINCT TABLE_SCHEMA FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA";

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			ResultSet rs = stmt.executeQuery(strSQL);
		
			location = 3000;
			return rs;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static ResultSet getTableList(Connection conn, String sourceDatabase, String sourceSchema) throws SQLException
	{
		String method = "getTableList";
		int location = 1000;

		try
		{
			location = 2000;
			String strSQL = "SELECT TABLE_NAME\n" +
					"FROM [" + sourceDatabase + "].INFORMATION_SCHEMA.TABLES\n" + 
					"WHERE TABLE_SCHEMA = '" + sourceSchema + "'\n" +
					"ORDER BY TABLE_NAME";

			location = 2100;
                        Statement stmt = conn.createStatement();

			location = 2200;
			ResultSet rs = stmt.executeQuery(strSQL);
		
			location = 3000;
			return rs;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}
}
