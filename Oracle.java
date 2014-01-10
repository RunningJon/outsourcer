import java.sql.*;
import java.net.*;
import java.io.*;

public class Oracle
{

	private static String myclass = "Oracle";
	public static boolean debug = false;

	private static String getSQLForColumns(String sourceSchema, String sourceTable) throws SQLException 
	{
		String method = "getSQLForColumns";
		int location = 1000;

		try 
		{
			location = 2000;
			String strSQL = "SELECT '\"' || COLUMN_NAME || '\"' AS COLUMN_NAME \n" +
					"FROM ALL_TAB_COLUMNS \n" +
					"WHERE TABLE_NAME = '" + sourceTable + "' \n" + 
					"	AND OWNER = '" + sourceSchema + "' \n" +
					"	AND DATA_TYPE NOT IN ('BLOB', 'BFILE', 'RAW', 'LONG RAW', 'MLSLABEL', 'BFILE', 'XMLTYPE') \n" +
					"ORDER BY COLUMN_ID";
			if (debug)
                                Logger.printMsg("Returning: " + strSQL);

			location = 2100;
			return strSQL;
		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getSQLForData(Connection conn, String sourceSchema, String sourceTable, String refreshType, String appendColumnName, int appendColumnMax) throws SQLException 
	{
		String method = "getSQLForData";
		int location = 1000;

		try 
		{
			location = 2000;
			//Create SQL Statement for getting the column names
			String strSQL = getSQLForColumns(sourceSchema, sourceTable);
		
			location = 2100;	
			//Execute SQL Statement and format columns for next SELECT statement
			String columnSQL = CommonDB.formatSQLForColumnName(conn, strSQL);

			location = 2200;
			//Create SQL Statement for retrieving data from table
			strSQL = "SELECT " + columnSQL + " \n" +
				"FROM \"" + sourceSchema + "\".\"" + sourceTable + "\" \n";
		
			location = 2300;	
			//Add filter for append refreshType
			if (debug)
				Logger.printMsg("About to refreshType: " + refreshType);

			if (refreshType.equals("append"))
			{
				location = 2400;
				strSQL = strSQL + "WHERE \"" + appendColumnName + "\" > " + appendColumnMax;  //greater than what is in GP currently
			}

			if (debug)
                                Logger.printMsg("Returning: " + strSQL);

			location = 2500;
			return strSQL;
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getSQLForCreateTable(Connection conn, String sourceSchema, String sourceTable) throws SQLException 
	{
		String method = "getSQLForCreateTable";
		int location = 1000;

		try
		{
			location = 2000;
                        String strSQL = "SELECT REPLACE(REPLACE(LOWER(COLUMN_NAME), '\"', ''), '.', '_') AS COLUMN_NAME, \n" +
					"CASE WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN 'float8' \n" +
		      			"     WHEN DATA_TYPE = 'BINARY_FLOAT' THEN 'float8' \n" + 
		     			"     WHEN DATA_TYPE = 'NUMBER' THEN 'numeric' \n" + 
		      			"     WHEN DATA_TYPE = 'DATE' THEN 'timestamp' \n" + 
		      			"     WHEN DATA_TYPE = 'CHAR' THEN 'character' \n" + 
		      			"     WHEN DATA_TYPE = 'NCHAR' THEN 'character' \n" + 
		      			"     WHEN DATA_TYPE = 'VARCHAR' THEN 'varchar' \n" + 
		      			"     WHEN DATA_TYPE = 'VARCHAR2' THEN 'varchar' \n" + 
		      			"     WHEN DATA_TYPE = 'NVARCHAR2' THEN 'varchar' \n" + 
		      			"     WHEN DATA_TYPE = 'ROWID' THEN 'varchar(18)' \n" + 
		      			"     WHEN DATA_TYPE = 'UROWID' THEN 'varchar' \n" + 
		      			"     WHEN DATA_TYPE = 'LONG' THEN 'text' \n" + 
		      			"     WHEN DATA_TYPE = 'CLOB' THEN 'text' \n" + 
		      			"     WHEN DATA_TYPE = 'NCLOB' THEN 'text' \n" + 
		      			"     WHEN DATA_TYPE LIKE 'TIMESTAMP%' AND DATA_TYPE LIKE '%TIME ZONE' THEN 'timestamptz' \n" + 
		      			"     WHEN DATA_TYPE LIKE 'TIMESTAMP%' AND DATA_TYPE NOT LIKE '%TIME ZONE' THEN 'timestamp' \n" + 
		      			"     WHEN DATA_TYPE LIKE 'INTERVAL%' THEN 'interval' \n" + 
					"     ELSE DATA_TYPE end || \n" +
					"     CASE WHEN DATA_TYPE IN ('CHAR', 'NCHAR', 'VARCHAR', 'VARCHAR2', 'NVARCHAR2', 'UROWID') THEN '(' || TO_CHAR(DATA_LENGTH) || ') ' \n " + 
					"          ELSE ' ' END AS DATATYPE \n" +
					"FROM ALL_TAB_COLUMNS \n" + 
					"WHERE TABLE_NAME = '" + sourceTable + "' \n" + 
					"     AND OWNER = '" + sourceSchema + "' \n" +
					"     AND DATA_TYPE NOT IN ('BLOB', 'BFILE', 'RAW', 'LONG RAW', 'MLSLABEL', 'BFILE', 'XMLTYPE') \n" +
					"ORDER BY COLUMN_ID";
			
			if (debug)
                                Logger.printMsg("Returning: " + strSQL);

			location = 2100;
			return strSQL;

		}
		catch (Exception ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static String getSQLForDistribution(Connection conn, String sourceSchema, String sourceTable) throws SQLException 
	{
		String method = "getSQLForDistribution";
		int location = 1000;

		try
		{
			location = 2000;
                        String strSQL = "SELECT '\"' || LOWER(dcc.COLUMN_NAME) || '\"' as COLUMN_NAME \n" +
					"FROM ALL_CONSTRAINTS dc \n" +
					"JOIN ALL_CONS_COLUMNS dcc ON dc.CONSTRAINT_NAME = dcc.CONSTRAINT_NAME and dc.OWNER = dcc.OWNER \n" +
					"WHERE dc.OWNER = '" + sourceSchema + "' \n" +
					"     AND dc.TABLE_NAME = '" + sourceTable + "' \n" +
					"     AND dc.CONSTRAINT_TYPE = 'P' \n" +
					"ORDER BY dcc.POSITION";

			if (debug)
                                Logger.printMsg("Returning: " + strSQL);

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
                        String strSQL = "SELECT VERSION FROM V$INSTANCE";

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
                        ResultSet rs = stmt.executeQuery(strSQL);

			location = 2300;
                        while (rs.next())
                        {
                                msg = "Success!";
                        }

			location = 2400;
                        return msg;
                }

                catch (SQLException ex)
                {
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
                }

        }

	public static int getMaxId(Connection conn, String sourceSchema, String sourceTable, String columnName) throws SQLException
        {
		String method = "getMaxId";
		int location = 1000;

                try
                {
			location = 2000;
                        int maxId = -1;
                        String strSQL = "SELECT MAX(\"" + columnName + "\") \n" +
					"FROM \"" + sourceSchema + "\".\"" + sourceTable + "\"";

			location = 2100;
			Statement stmt = conn.createStatement();

			location = 2200;
                        ResultSet rs = stmt.executeQuery(strSQL);

			location = 2300;
                        while (rs.next())
                        {
                                maxId = rs.getInt(1);
                        }

			location = 2400;
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
			String sourceType = "oracle";
			String replTable = GP.getReplTableName(sourceType, sourceTable);

			//1. drop triggers if exists
			//2. drop replTable if exists
			//3. drop sequence if exists
			//4. create sequence
			//5. create replTable
			//6. create trigger to capture changes

                        Statement stmt = conn.createStatement();

			String strSQL = "";

			/////////////////////////////
			//1.  drop trigger if exists
			/////////////////////////////
			location = 2100;
			strSQL = "BEGIN \n" +
				"FOR x IN (SELECT * FROM DUAL WHERE EXISTS (SELECT NULL FROM ALL_TRIGGERS \n" + 
				"						WHERE TABLE_OWNER = '" + sourceSchema + "' \n" +
				"						AND TRIGGER_NAME = 'T_" + replTable + "_AIUD') ) LOOP \n" +
				"	EXECUTE IMMEDIATE('DROP TRIGGER \"" + sourceSchema + "\".\"T_" + replTable + "_AIUD\"'); \n" +
				"END LOOP; \n" +
				"END;";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2200;
                        stmt.executeUpdate(strSQL);

			/////////////////////////////
			//2. drop replTable if exists
			/////////////////////////////
			location = 3100;
			strSQL = "BEGIN \n" +
				"FOR x IN (SELECT * FROM DUAL WHERE EXISTS (SELECT NULL FROM ALL_TABLES \n" + 
				"						WHERE OWNER = '" + sourceSchema + "' \n" +
				"						AND TABLE_NAME = '" + replTable + "') ) LOOP \n" +
				"	EXECUTE IMMEDIATE('DROP TABLE \"" + sourceSchema + "\".\"" + replTable + "\"'); \n" +
				"END LOOP; \n" +
				"END;";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 3200;
                        stmt.executeUpdate(strSQL);
			
			/////////////////////////////
			//3. drop sequence if exists
			/////////////////////////////
			location = 4100;
			strSQL = "BEGIN \n" +
				"FOR x IN (SELECT * FROM DUAL WHERE EXISTS (SELECT NULL FROM ALL_SEQUENCES \n" + 
				"						WHERE SEQUENCE_OWNER = '" + sourceSchema + "' \n" +
				"						AND SEQUENCE_NAME = 'SEQ_" + replTable + "') ) LOOP \n" +
				"	EXECUTE IMMEDIATE('DROP SEQUENCE \"" + sourceSchema + "\".\"SEQ_" + replTable + "\"'); \n" +
				"END LOOP; \n" +
				"END;";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 4200;
                        stmt.executeUpdate(strSQL);

			/////////////////////////////
			//4. create sequence
			/////////////////////////////
			location = 5100;
			strSQL = "CREATE SEQUENCE \"" + sourceSchema + "\".\"SEQ_" + replTable + "\" ORDER";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 5200;
                        stmt.executeUpdate(strSQL);

			/////////////////////////////
			//5. create replTable
			/////////////////////////////
			location = 6100;
			strSQL = "SELECT COLUMN_NAME, \n" +
					"	DATA_TYPE || \n" +
					"	CASE WHEN DATA_TYPE IN ('CHAR', 'NCHAR', 'VARCHAR', 'VARCHAR2', 'NVARCHAR2') \n" +
					"	THEN '(' || TO_CHAR(DATA_LENGTH) || ') ' \n " + 
					"	ELSE ' ' END || \n" +
					"	CASE WHEN NULLABLE = 'Y' THEN 'NULL' ELSE 'NOT NULL' END AS ATTRIBUTES \n" +
					"FROM ALL_TAB_COLUMNS \n" +
					"WHERE OWNER = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + sourceTable + "' \n" +
                                        "	AND DATA_TYPE NOT IN ('BLOB', 'BFILE', 'RAW', 'LONG RAW', 'MLSLABEL', 'BFILE', 'XMLTYPE') \n" +
					"ORDER BY COLUMN_ID";

			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

                        //Get Column names
                        String columnName = "";
                        String attributes = "";
			String oldColumns = "";
			String newColumns = "";
			String columns = "";

			location = 6200;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        while (rs.next())
                        {
                                columnName = rs.getString(1);
				attributes = rs.getString(2);

                                if (rs.getRow() == 1)
                                {
					location = 6300;
					strSQL = "CREATE TABLE \"" + sourceSchema + "\".\"" + replTable + "\" \n" +
						"(\"" + appendColumnName + "\" NUMBER NOT NULL PRIMARY KEY, \n" +
						"change_type CHAR(1) NOT NULL, \n";

					strSQL = strSQL + "\"" + columnName + "\" " + attributes;

					oldColumns = "		:OLD.\"" + columnName + "\"";
					newColumns = "		:NEW.\"" + columnName + "\"";
					columns = "		\"" + columnName + "\"";
                                }
				else
				{
					strSQL = strSQL + ", \n \"" + columnName + "\" " + attributes;

					oldColumns = oldColumns  + ", \n		:OLD.\"" + columnName + "\"";
					newColumns = newColumns  + ", \n		:NEW.\"" + columnName + "\"";
					columns = columns  + ", \n		\"" + columnName + "\"";
				}
                        }

			location = 6400;
			strSQL = strSQL + ")";

			if (debug)
                                Logger.printMsg("Executing SQL: \n" + strSQL);

			location = 6500;
                        stmt.executeUpdate(strSQL);

			/////////////////////////////
			//6. create trigger to capture changes
			/////////////////////////////
			location = 7100;
			strSQL = "CREATE TRIGGER T_" + replTable + "_AIUD \n" +
				"AFTER INSERT OR UPDATE OR DELETE \n" +
				"ON \"" + sourceSchema + "\".\"" + sourceTable + "\" \n" +
				"REFERENCING \n" +
				" NEW AS NEW \n" +
				" OLD AS OLD \n" +
				"FOR EACH ROW \n" +
				"DECLARE \n" +
				"	v_id NUMBER; \n" +
				"\n" +
				"BEGIN \n" +
				"	SELECT \"" + sourceSchema + "\".\"SEQ_" + replTable + "\".NEXTVAL \n" +
				"	INTO v_id \n" +
				"	FROM DUAL; \n" +
				"\n" +
				"	IF INSERTING THEN \n " +
				"		INSERT INTO \"" + sourceSchema + "\".\"" + replTable + "\" \n" +
				"		(\"" + appendColumnName + "\", \n" +
				"		change_type, \n" + 
						columns + ") \n" +
				"	VALUES (v_id, \n" + 
				"		'I', \n" +
						newColumns + "); \n" +
				"\n" +
				"	ELSIF UPDATING THEN \n " +
				"		INSERT INTO \"" + sourceSchema + "\".\"" + replTable + "\" \n" +
				"		(\"" + appendColumnName + "\", \n" +
				"		change_type, \n" + 
						columns + ") \n" +
				"	VALUES (v_id, \n" + 
				"		'U', \n" +
						newColumns + "); \n" +
				"\n" +
				"	ELSIF DELETING THEN \n " +
				"		INSERT INTO \"" + sourceSchema + "\".\"" + replTable + "\" \n" +
				"		(\"" + appendColumnName + "\", \n" +
				"		change_type, \n" + 
						columns + ") \n" +
				"	VALUES (v_id, \n" + 
				"		'D', \n" +
						oldColumns + "); \n" +
				"\n" +
				"	END IF; \n " +
				"END;";
			
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 7200;
			stmt.executeUpdate(strSQL);
		}
		catch (SQLException ex)
		{
			throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
		}
	}

	public static boolean snapshotSourceTable(Connection conn, String sourceSchema, String sourceTable) throws SQLException
	{
		String method = "snapshotSourceTable";
		int location = 1000;

		try
		{
			location = 2000;
			String sourceType = "oracle";
			String replTable = GP.getReplTableName(sourceType, sourceTable);
			boolean found  = false;

			location = 2100;
                        Statement stmt = conn.createStatement();


			//Check for the trigger 
			//for the trigger to exist, the table must exist too
			location = 2200;	
			String strSQL = "SELECT NULL \n" +
					"FROM ALL_TRIGGERS \n" + 
					"WHERE TABLE_OWNER = '" + sourceSchema + "' \n" +
					"	AND TRIGGER_NAME = 'T_" + replTable + "_AIUD'";
 
			if (debug)
                                Logger.printMsg("Executing SQL: " + strSQL);

			location = 2300;
                        ResultSet rs = stmt.executeQuery(strSQL);

                        while (rs.next())
			{
				found = true;
			}

			if (found)
			{
				location = 2400;
				found = false;
				//Check for the OS table
				//for the trigger to exist, the table must exist too
				strSQL = "SELECT NULL \n" +
					"FROM ALL_TABLES \n" + 
					"WHERE OWNER = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + replTable + "'";
 
				if (debug)
        	                        Logger.printMsg("Executing SQL: " + strSQL);

				location = 2500;
                	        rs = stmt.executeQuery(strSQL);

                        	while (rs.next())
                      		{
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
					"FROM ALL_USERS \n" +
					"WHERE USERNAME = '" + sourceSchema + "'";

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
					"FROM ALL_TABLES \n" +
					"WHERE OWNER = '" + sourceSchema + "' \n" +
					"	AND TABLE_NAME = '" + sourceTable + "' \n" +
					"UNION \n" +
					"SELECT NULL \n" +
					"FROM ALL_VIEWS \n" +
					"WHERE OWNER = '" + sourceSchema + "' \n" +
					"	AND VIEW_NAME = '" + sourceTable + "'";

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
					"FROM ALL_TAB_COLUMNS \n" +
					"WHERE TABLE_NAME = '" + sourceTable + "' \n" + 
					"	AND OWNER = '" + sourceSchema + "' \n" +
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
					"FROM ALL_TAB_COLUMNS \n" +
					"WHERE TABLE_NAME = '" + sourceTable + "' \n" + 
					"	AND OWNER = '" + sourceSchema + "' \n" +
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
					"FROM ALL_CONSTRAINTS dc \n" +
					"WHERE OWNER = '" + sourceSchema + "' \n" +
					"     AND TABLE_NAME = '" + sourceTable + "' \n" +
					"     AND CONSTRAINT_TYPE = 'P'"; 
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

	public static ResultSet getSchemaList(Connection conn) throws SQLException
	{
		String method = "getSchemaList";
		int location = 1000;

		try
		{
			location = 2000;
			//only get schemas in which there is a table or view
			String strSQL = "SELECT DISTINCT t.OWNER AS SCHEMA_NAME\n" +
					"FROM ALL_TABLES t\n" + 
					"JOIN DBA_USERS u ON t.OWNER = u.USERNAME\n" +
					"WHERE u.DEFAULT_TABLESPACE NOT IN ('SYSTEM', 'SYSAUX')\n" +
					"UNION\n" + 
					"SELECT DISTINCT v.OWNER\n" +
					"FROM ALL_VIEWS v\n" +
					"JOIN DBA_USERS u ON v.OWNER = u.USERNAME\n" + 
					"WHERE u.DEFAULT_TABLESPACE NOT IN ('SYSTEM', 'SYSAUX')";

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

	public static ResultSet getTableList(Connection conn, String sourceSchema) throws SQLException
	{
		String method = "getTableList";
		int location = 1000;

		try
		{
			location = 2000;
			//only get tables in which there is a table or view
			String strSQL = "SELECT t.TABLE_NAME\n" +
					"FROM ALL_TABLES t\n" + 
					"JOIN DBA_USERS u ON t.OWNER = u.USERNAME\n" +
					"WHERE u.DEFAULT_TABLESPACE NOT IN ('SYSTEM', 'SYSAUX')\n" +
					"AND u.USERNAME = '" + sourceSchema + "'";

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
