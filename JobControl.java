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
		boolean targetAppendOnly = Boolean.valueOf(parms.get("target_append_only"));
		boolean targetCompressed = Boolean.valueOf(parms.get("target_compressed"));
		boolean targetRowOrientation = Boolean.valueOf(parms.get("target_row_orientation"));
		String sourceType = parms.get("source_type");
		String sourceServerName = parms.get("source_server_name");
		String sourceInstanceName = parms.get("source_instance_name");
		String sourcePort = parms.get("source_port");
		String sourceDatabaseName = parms.get("source_database_name");
		String sourceSchemaName = parms.get("source_schema_name");
		String sourceTableName = parms.get("source_table_name");
		String sourceUserName = parms.get("source_user_name");
		String sourcePass = parms.get("source_pass");
		String columnName = parms.get("column_name");
		String sqlText = parms.get("sql_text");
		boolean snapshot = Boolean.valueOf(parms.get("snapshot"));
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

		if (sourceSchemaName == null)
			sourceSchemaName = "";

		if (sourceTableName == null)
			sourceTableName = "";

		if (sourceUserName == null)
			sourceUserName = "";

		if (sourcePass == null)
			sourcePass = "";

		if (columnName == null)
			columnName = "";

		if (sqlText == null)
			sqlText = "";

		//if (snapshot == null)
		//	snapshot = false;

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
					msg = JobView.viewUpdate(e.id, e.refreshType, e.targetSchemaName, e.targetTableName, e.targetAppendOnly, e.targetCompressed, e.targetRowOrientation, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceSchemaName, e.sourceTableName, e.sourceUserName, e.sourcePass, e.columnName, e.sqlText, e.snapshot, e.scheduleDesc, scheduleList);
				}
				else
				{
					JobModel.insertTable(id, refreshType, targetSchemaName, targetTableName, targetAppendOnly, targetCompressed, targetRowOrientation, sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceSchemaName, sourceTableName, sourceUserName, sourcePass, columnName, sqlText, snapshot, scheduleDesc);
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
					msg = JobView.viewDelete(e.id, e.refreshType, e.targetSchemaName, e.targetTableName, e.targetAppendOnly, e.targetCompressed, e.targetRowOrientation, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceSchemaName, e.sourceTableName, e.sourceUserName, e.sourcePass, e.columnName, e.sqlText, e.snapshot, e.scheduleDesc, scheduleList);
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
