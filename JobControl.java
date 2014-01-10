import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class JobControl
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
		String refreshType = parms.get("refresh_type");
		String targetSchemaName = parms.get("target_schema_name");
		String targetTableName = parms.get("target_table_name");
		String type = parms.get("type");
		String serverName = parms.get("server_name");
		String instanceName = parms.get("instance_name");
		String port = parms.get("port");
		String databaseName = parms.get("database_name");
		String schemaName = parms.get("schema_name");
		String tableName = parms.get("table_name");
		String userName = parms.get("user_name");
		String pass = parms.get("pass");
		String columnName = parms.get("column_name");
		String sqlText = parms.get("sql_text");
		String snapshot = parms.get("snapshot");
		String submit = parms.get("submit_form");
		String queueAction = parms.get("queue_action");

		String scheduleDesc = parms.get("schedule_desc");
		ArrayList<String> scheduleList = new ArrayList<String>();

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

		if (refreshType == null)
			refreshType = "";

		if (targetSchemaName == null)
			targetSchemaName = "";

		if (targetTableName == null)
			targetTableName = "";

		if (type == null)
			type = "";

		if (serverName == null)
			serverName = "";

		if (instanceName == null)
			instanceName = "";

		if (port == null)
			port = "";

		if (databaseName == null)
			databaseName = "";

		if (schemaName == null)
			schemaName = "";

		if (tableName == null)
			tableName = "";

		if (userName == null)
			userName = "";

		if (pass == null)
			pass = "";

		if (columnName == null)
			columnName = "";

		if (sqlText == null)
			sqlText = "";

		if (snapshot == null)
			snapshot = "";

		if (actionType == null || actionType.equals(""))
			actionType = "view";

		if (submit == null || submit.equals(""))
			submit = "0";

		if (queueAction == null)
			queueAction = "";

		if (scheduleDesc == null)
			scheduleDesc = "";

		//need the parsing of parms into local variables to call the right views

		String msg = "";

		try
		{
			if (actionType.equals("view"))
			{
				rs = JobModel.getList(search, limit, offset, sortBy, sort);
				msg = JobView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			else if (actionType.equals("update"))
			{
				if (submit.equals("0"))
				{
					JobModel e = JobModel.getModel(id);
					scheduleList = ScheduleModel.getDescriptions();
					msg = JobView.viewUpdate(e.id, e.refreshType, e.targetSchemaName, e.targetTableName, e.type, e.serverName, e.instanceName, e.port, e.databaseName, e.schemaName, e.tableName, e.userName, e.pass, e.columnName, e.sqlText, e.snapshot, e.scheduleDesc, scheduleList);
				}
				else
				{
					if (id.equals(""))
						JobModel.insertTable(refreshType, targetSchemaName, targetTableName, type, serverName, instanceName, port, databaseName, schemaName, tableName, userName, pass, columnName, sqlText, snapshot, scheduleDesc);
					else
						JobModel.updateTable(id, refreshType, targetSchemaName, targetTableName, type, serverName, instanceName, port, databaseName, schemaName, tableName, userName, pass, columnName, sqlText, snapshot, scheduleDesc);
					rs = JobModel.getList(search, limit, offset, sortBy, sort);
					msg = JobView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			else if (actionType.equals("delete"))
			{	
				if (submit.equals("0"))
				{
					JobModel e = JobModel.getModel(id);
					scheduleList = ScheduleModel.getDescriptions();
					msg = JobView.viewDelete(e.id, e.refreshType, e.targetSchemaName, e.targetTableName, e.type, e.serverName, e.instanceName, e.port, e.databaseName, e.schemaName, e.tableName, e.userName, e.pass, e.columnName, e.sqlText, e.snapshot, e.scheduleDesc, scheduleList);
				}
				else
				{
					JobModel.deleteTable(id);
					rs = JobModel.getList(search, limit, offset, sortBy, sort);
					msg = JobView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				
			}
			else if (actionType.equals("queue"))
			{
				if (queueAction.equals("insert"))
					QueueModel.insertTable(id);
				else if (queueAction.equals("update"))
					QueueModel.updateTable(id);

				rs = JobModel.getList(search, limit, offset, sortBy, sort);
				msg = JobView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			else if (actionType.equals("delete_all"))
			{
				if (submit.equals("0"))
				{
					msg = JobView.viewDelete();
				}
				else
				{
                                	JobModel.deleteTable();
					rs = JobModel.getList(search, limit, offset, sortBy, sort);
					msg = JobView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
		}
		catch (Exception ex)
		{
			msg = ex.getMessage();
		}
		return msg;
	}
}
