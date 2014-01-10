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
		String type = parms.get("type");
		String serverName = parms.get("server_name");
		String instanceName = parms.get("instance_name");
		String port = parms.get("port");
		String databaseName = parms.get("database_name");
		String userName = parms.get("user_name");
		String pass = parms.get("pass");
		String submit = parms.get("submit_form");
		String validate = parms.get("validate");
		String validateMsg = "";
		String sourceSchema = parms.get("source_schema");
		String targetSchema = parms.get("target_schema");
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

		if (userName == null)
			userName = "";

		if (pass == null)
			pass = "";

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

		//need the parsing of parms into local variables to call the right views

		String msg = "";

		if (actionType.equals("view"))
		{
			try
			{
				rs = ExternalTableModel.getList(search, limit, offset, sortBy, sort, "external");
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
				msg = ExternalTableView.viewUpdate(e.id, e.type, e.serverName, e.instanceName, e.port, e.databaseName, e.userName, e.pass, "");
			}
			else
			{
				if (validate.equals("0"))
				{
					try
					{
						if (id.equals(""))
							ExternalTableModel.insertTable(type, serverName, instanceName, port, databaseName, userName, pass);
						else
							ExternalTableModel.updateTable(id, type, serverName, instanceName, port, databaseName, userName, pass);
						rs = ExternalTableModel.getList(search, limit, offset, sortBy, sort, "external");
						msg = ExternalTableView.viewList(search, rs, limit, offset, sortBy, sort);
					}
					catch (Exception ex)
					{
						msg = ex.getMessage();
					}
				}
				else
				{
					if ( ( type.equals("oracle") && (serverName.equals("") || port.equals("") || databaseName.equals("") || userName.equals("") || pass.equals("")) ) || (type.equals("sqlserver") && (serverName.equals("") || userName.equals("") || pass.equals("")) ) )
						validateMsg = "Please provide correct values for connection and try again.";
					else
						validateMsg = ExternalTableModel.validate(type, serverName, instanceName, port, databaseName, userName, pass);
					
					msg = ExternalTableView.viewUpdate(id, type, serverName, instanceName, port, databaseName, userName, pass, validateMsg);
					
				}
			}

		} 
		else if (actionType.equals("delete"))
		{	
			if (submit.equals("0"))
			{
				ExternalTableModel e = ExternalTableModel.getModel(id);
				msg = ExternalTableView.viewDelete(e.id, e.type, e.serverName, e.instanceName, e.port, e.databaseName, e.userName, e.pass);
			}
			else
			{
				try
				{
					ExternalTableModel.deleteTable(id);
					rs = ExternalTableModel.getList(search, limit, offset, sortBy, sort, "external");
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
					if (e.type.equals("sqlserver"))
					{
						conn = CommonDB.connectSQLServer(e.serverName, e.instanceName, e.userName, e.pass);
						//get list of databases
						databaseList = SQLServer.getDatabaseList(conn);

						if (!(databaseName.equals("")))
						{
							//database picked, now get the schemas in that database
							//note that for SQLServer, it is databaseName and not e.databaseName
							schemaList = SQLServer.getSchemaList(conn, databaseName);
						}
					}
					else if (e.type.equals("oracle"))
					{
						//note that for Oracle, it is e.databaseName
						databaseName = e.databaseName;
						int sourcePort = Integer.parseInt(e.port);
						conn = CommonDB.connectOracle(e.serverName, e.databaseName, sourcePort, e.userName, e.pass, 10);
						schemaList = Oracle.getSchemaList(conn);
					}

					scheduleList = ScheduleModel.getDescriptions();
					msg = ExternalTableView.viewCreate(databaseList, schemaList, e.id, e.type, e.serverName, e.instanceName, e.port, databaseName, e.userName, targetSchema, refreshType, scheduleList);
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
					int count = ExternalTableModel.createJobs(id, databaseName, sourceSchema, targetSchema, refreshType, scheduleDesc);
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
