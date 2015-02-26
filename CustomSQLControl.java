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
		String submit = parms.get("submit_form");
		String schema = parms.get("schema");
		ArrayList<String> schemaList = new ArrayList<String>();

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

		if (tableName == null)
			tableName = "";

		if (columns == null)
			columns = "";

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
		else if (actionType.equals("update") || actionType.equals("insert"))
		{
			if (submit.equals("0"))
			{
				CustomSQLModel e = CustomSQLModel.getModel(id);
				msg = CustomSQLView.viewUpdate(e.id, e.tableName, e.columns, e.sqlText, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceUserName, e.sourcePass);
			}
/*
			else
			{
				try
				{
					CustomSQLModel.insertTable(id, tableName, columns);
					rs = CustomSQLModel.getList(search, limit, offset, sortBy, sort);
					msg = CustomSQLView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
*/
		}
 /*
		else if (actionType.equals("delete"))
		{	
			if (submit.equals("0"))
			{
				CustomSQLModel e = CustomSQLModel.getModel(id);
				msg = CustomSQLView.viewDelete(e.id, e.tableName, e.columns);
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
					schemaList = JobModel.getSchemas();
					msg = CustomSQLView.viewCreate(id, schemaList);
				}
				else
				{
					JobModel.updateJobsCustomSQL(id, schema);
					rs = CustomSQLModel.getList(search, limit, offset, sortBy, sort);
					msg = CustomSQLView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		}
*/
		return msg;
	}
}
