import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class CustomSQLControl
{
	public static String buildPage(Map<String, String> parms)
	{
		ResultSet rs = null;
		String search = parms.get("search");
		String limit = parms.get("limit");
		String offset = parms.get("offset");
		String actionType = parms.get("action_type");
		String sortBy = parms.get("sort_by");
		String sort = parms.get("sort");
		String id = parms.get("id");
		String tableName = parms.get("table_name");
		String columns = parms.get("columns");
		String sqlText = parms.get("sql_text");
		String sourceType = parms.get("source_type");
		String sourceServerName = parms.get("source_server_name");
		String sourceInstanceName = parms.get("source_instance_name");
		String sourcePort = parms.get("source_port");
		String sourceDatabaseName = parms.get("source_database_name");
		String sourceUserName = parms.get("source_user_name");
		String sourcePass = parms.get("source_pass");
		String submit = parms.get("submit_form");
		String schema = parms.get("schema");
		ArrayList<String> extConnectionIdList = new ArrayList<String>();

		String extConnectionId = parms.get("ext_connection_id");


		if (search == null)
			search = "";

		if (limit == null)
			limit = "10";

		if (offset == null)
			offset = "0";

		if (sort == null || sort.equals(""))
			sort = "asc";

		if (sortBy == null || sortBy.equals(""))
			sortBy = "id";

		if (id == null)
			id = "";

		if (extConnectionId == null)
			extConnectionId = "";

		if (tableName == null)
			tableName = "";

		if (columns == null)
			columns = "";

		if (sqlText == null)
			sqlText = "";

		if (sourceType == null)
			sourceType = "";

		if (sourceServerName == null)
			sourceServerName = "";

		if (sourceInstanceName == null)
			sourceInstanceName = "";

		if (sourcePort == null)
			sourcePort = "";

		if (sourceDatabaseName == null)
			sourceDatabaseName = "";

		if (sourceUserName == null)
			sourceUserName = "";

		if (sourcePass == null)
			sourcePass = "";

		if (actionType == null || actionType.equals(""))
			actionType = "view";

		if (submit == null || submit.equals(""))
			submit = "0";

		if (schema == null)
			schema = "";

		String msg = "";

		if (actionType.equals("view"))
		{
			try
			{
				rs = CustomSQLModel.getList(search, limit, offset, sortBy, sort);
				msg = CustomSQLView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		}
		else if (actionType.equals("update"))
		{
			if (submit.equals("0"))
			{
				CustomSQLModel e = CustomSQLModel.getModel(id);
				msg = CustomSQLView.viewUpdate(e.id, e.tableName, e.columns, e.sqlText, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceUserName, e.sourcePass);
			}
			else
			{
				try
				{
					CustomSQLModel.insertTable(id, tableName, columns, sqlText, sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceUserName, sourcePass);
					rs = CustomSQLModel.getList(search, limit, offset, sortBy, sort);
					msg = CustomSQLView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
		}
		else if (actionType.equals("delete"))
		{	
			if (submit.equals("0"))
			{
				CustomSQLModel e = CustomSQLModel.getModel(id);
				msg = CustomSQLView.viewDelete(e.id, e.tableName, e.columns, e.sqlText, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceUserName, e.sourcePass);
			}
			else
			{
				try
				{
					CustomSQLModel.deleteTable(id);
					rs = CustomSQLModel.getList(search, limit, offset, sortBy, sort);
					msg = CustomSQLView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
		}
		else if (actionType.equals("create"))
		{	
			try
			{
				if (submit.equals("0"))
				{
					extConnectionIdList = ExternalTableModel.getExtConnectionIds();
					msg = CustomSQLView.viewCreate(tableName, columns, sqlText, extConnectionIdList);
				}
				else
				{
					CustomSQLModel.insertTable(tableName, columns, sqlText, extConnectionId);
					rs = CustomSQLModel.getList(search, limit, offset, sortBy, sort);
					msg = CustomSQLView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		}
		return msg;
	}
}
