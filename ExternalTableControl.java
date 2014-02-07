import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class ExternalTableControl
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
		String sourceType = parms.get("source_type");
		String sourceServerName = parms.get("source_server_name");
		String sourceInstanceName = parms.get("source_instance_name");
		String sourcePort = parms.get("source_port");
		String sourceDatabaseName = parms.get("source_database_name");
		String sourceUserName = parms.get("source_user_name");
		String sourcePass = parms.get("source_pass");
		String submit = parms.get("submit_form");
		String validate = parms.get("validate");
		String validateMsg = "";
		String sourceSchema = parms.get("source_schema");
		String targetSchema = parms.get("target_schema");
		String strTargetAppendOnly = parms.get("target_append_only");
		boolean targetAppendOnly = Boolean.valueOf(parms.get("target_append_only"));
		boolean targetCompressed = Boolean.valueOf(parms.get("target_compressed"));
		boolean targetRowOrientation = Boolean.valueOf(parms.get("target_row_orientation"));
		String refreshType = parms.get("refresh_type");

		String scheduleDesc = parms.get("schedule_desc");
		ArrayList<String> scheduleList = new ArrayList<String>();

		if (search == null)
			search = "";

		if (limit == null)
			limit = "10";

		if (offset == null)
			offset = "0";

		if (sortBy == null || sortBy.equals(""))
			sortBy = "id";

		if (sort == null || sort.equals(""))
			sort = "asc";

		if (id == null)
			id = "";

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

		if (validate == null)
			validate = "0";

		if (sourceSchema == null)
			sourceSchema = "";

		if (targetSchema == null)
			targetSchema = "";

		if (refreshType == null)
			refreshType = "";

		if (scheduleDesc == null)
			scheduleDesc = "";

		if (strTargetAppendOnly == null)
		{
			targetAppendOnly = true;
			targetCompressed = false;
			targetRowOrientation = true;
		}

		//need the parsing of parms into local variables to call the right views

		String msg = "";

		if (actionType.equals("view"))
		{
			try
			{
				rs = ExternalTableModel.getList(search, limit, offset, sortBy, sort);
				msg = ExternalTableView.viewList(search, rs, limit, offset, sortBy, sort);
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
				ExternalTableModel e = ExternalTableModel.getModel(id);
				msg = ExternalTableView.viewUpdate(e.id, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceUserName, e.sourcePass, "");
			}
			else
			{
				if (validate.equals("0"))
				{
					try
					{
						ExternalTableModel.insertTable(id, sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceUserName, sourcePass);
						rs = ExternalTableModel.getList(search, limit, offset, sortBy, sort);
						msg = ExternalTableView.viewList(search, rs, limit, offset, sortBy, sort);
					}
					catch (Exception ex)
					{
						msg = ex.getMessage();
					}
				}
				else
				{
					if ( ( sourceType.equals("oracle") && (sourceServerName.equals("") || sourcePort.equals("") || sourceDatabaseName.equals("") || sourceUserName.equals("") || sourcePass.equals("")) ) || (sourceType.equals("sqlserver") && (sourceServerName.equals("") || sourceUserName.equals("") || sourcePass.equals("")) ) )
						validateMsg = "Please provide correct values for connection and try again.";
					else
						validateMsg = ExternalTableModel.validate(sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceUserName, sourcePass);
					
					msg = ExternalTableView.viewUpdate(id, sourceType, sourceServerName, sourceInstanceName, sourcePort, sourceDatabaseName, sourceUserName, sourcePass, validateMsg);
					
				}
			}

		} 
		else if (actionType.equals("delete"))
		{	
			if (submit.equals("0"))
			{
				ExternalTableModel e = ExternalTableModel.getModel(id);
				msg = ExternalTableView.viewDelete(e.id, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, e.sourceDatabaseName, e.sourceUserName, e.sourcePass);
			}
			else
			{
				try
				{
					ExternalTableModel.deleteTable(id);
					rs = ExternalTableModel.getList(search, limit, offset, sortBy, sort);
					msg = ExternalTableView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
				
		}
		//creating jobs based on an schema from the source
		else if (actionType.equals("create"))
		{
			ResultSet databaseList = null;
			ResultSet schemaList = null;
			Connection conn = null;

			//make sure valid values are set for submitting, else refresh the page.
			if (submit.equals("1") && (id.equals("") || sourceSchema.equals("") || targetSchema.equals("") || refreshType.equals("")) )
				submit = "0";
	
			if (submit.equals("0"))
			{
				ExternalTableModel e = ExternalTableModel.getModel(id);
				try
				{
					if (e.sourceType.equals("sqlserver"))
					{
						conn = CommonDB.connectSQLServer(e.sourceServerName, e.sourceInstanceName, e.sourceUserName, e.sourcePass);
						//get list of databases
						databaseList = SQLServer.getDatabaseList(conn);

						if (!(sourceDatabaseName.equals("")))
						{
							//database picked, now get the schemas in that database
							//note that for SQLServer, it is sourceDatabaseName and not e.sourceDatabaseName
							schemaList = SQLServer.getSchemaList(conn, sourceDatabaseName);
						}
					}
					else if (e.sourceType.equals("oracle"))
					{
						//note that for Oracle, it is e.sourceDatabaseName
						sourceDatabaseName = e.sourceDatabaseName;
						int intSourcePort = Integer.parseInt(e.sourcePort);
						conn = CommonDB.connectOracle(e.sourceServerName, e.sourceDatabaseName, intSourcePort, e.sourceUserName, e.sourcePass, 10);
						schemaList = Oracle.getSchemaList(conn);
					}

					scheduleList = ScheduleModel.getDescriptions();
					msg = ExternalTableView.viewCreate(databaseList, schemaList, e.id, e.sourceType, e.sourceServerName, e.sourceInstanceName, e.sourcePort, sourceDatabaseName, e.sourceUserName, targetSchema, targetAppendOnly, targetCompressed, targetRowOrientation, refreshType, scheduleList);
					if (conn != null)
						conn.close();
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
			else
			{
				try
				{
					int count = ExternalTableModel.createJobs(id, sourceDatabaseName, sourceSchema, targetSchema, targetAppendOnly, targetCompressed, targetRowOrientation, refreshType, scheduleDesc);
					msg = ExternalTableView.viewResults(count);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
		}

		return msg;
	}
}
