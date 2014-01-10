import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class ExternalTableView
{
	public static String action = "external";

	private static String getHead()
	{
		String myScript = "function formSubmit(offset)\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"offset\").value = offset;\n";
		myScript += "   document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		myScript += "function sortRS(sortBy, sort)\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"offset\").value = 0;\n";
		myScript += "   document.getElementById(\"sort_by\").value = sortBy;\n";
		myScript += "   document.getElementById(\"sort\").value = sort;\n";
		myScript += "   document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		myScript += "function updateExternalTable(id, myAction)\n";
		myScript += "{\n";
		myScript += "   document.myForm.action = 'external';\n";
		myScript += "   document.getElementById(\"action_type\").value = myAction;\n";
		myScript += "   document.getElementById(\"offset\").value = 0;\n";
		myScript += "   document.getElementById(\"submit_form\").value = \"0\";\n";
		myScript += "   document.getElementById(\"id\").value = id;\n";
		myScript += "   document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		myScript += "function createJobs(id)\n";
		myScript += "{\n";
		myScript += "   document.myForm.action = 'external';\n";
		myScript += "   document.getElementById(\"action_type\").value = 'create';\n";
		myScript += "   document.getElementById(\"offset\").value = 0;\n";
		myScript += "   document.getElementById(\"submit_form\").value = \"0\";\n";
		myScript += "   document.getElementById(\"id\").value = id;\n";
		myScript += "   document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";

		return myScript;
	}

	public static String viewList(String search, ResultSet rs, String limit, String offset, String sortBy, String sort)
	{
		String myScript = getHead();
		
		String msg = OutsourcerView.viewSearch(action, search, limit, offset, sortBy, sort, myScript);
		msg += "<table class=\"tftable\" border=\"0\">\n";
		msg += "<tr>\n";
		msg += "<td><h4><a href=\"?action_type=update\">Define New Source</a></h4></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "<br>\n";
		msg += getHeader(sortBy, sort);

		try
		{
			msg += OutsourcerView.viewResults(limit, offset, rs);
		}
		catch (SQLException ex)
		{
			msg += ex.getMessage();
		}
		return msg;
	}

	private static String getJavaScriptFunctions()
	{
		String myScript = "function disableInputFields()\n";
		myScript += "{\n";
		myScript += "	var val = document.getElementById(\"type\").selectedIndex;\n";
		myScript += "	if (val == 0) //Oracle\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"instance_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"port\").disabled = false;\n";
		myScript += "		document.getElementById(\"database_name\").disabled = false;\n";
		myScript += "		document.getElementById(\"r_instance_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_port\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_database_name\").style.display = \"\";\n";
		myScript += "	}\n";
		myScript += "	if (val == 1) //SQL Server\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"port\").value = \"\";\n";
		myScript += "		document.getElementById(\"database_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"r_instance_name\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_port\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_database_name\").style.display = \"none\";\n";
		myScript += "	}\n";
		myScript += "}\n";
		myScript += "function submitExternal(validate)\n";
		myScript += "{\n";
		myScript += "		document.getElementById(\"validate\").value = validate;\n";
		myScript += "		document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		myScript += "function createJobs()\n";
		myScript += "{\n";
		myScript += "		window.alert(\"After clicking OK, the jobs will be created.  Please do not leave or refresh page.\");\n";
		myScript += "		document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		myScript += "function submitDatabase()\n";
		myScript += "{\n";
		myScript += "		document.getElementById(\"submit_form\").value = \"0\";\n";
		myScript += "		document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		return myScript;

	}

	public static String viewUpdate(String id, String type, String serverName, String instanceName, String port, String databaseName, String userName, String pass, String validateMsg)
	{

		String buttonText = "";
		if (id == null)
		{
			id = "";
			buttonText = "Insert";
		}
		else
		{
			buttonText = "Update";
		}

		serverName = OutsourcerView.setHTMLField(serverName);
		instanceName = OutsourcerView.setHTMLField(instanceName);
		port = OutsourcerView.setHTMLField(port);
		databaseName = OutsourcerView.setHTMLField(databaseName);
		userName = OutsourcerView.setHTMLField(userName);
		pass = OutsourcerView.setHTMLField(pass);

		String myScript = getJavaScriptFunctions();
		String onLoad="disableInputFields()";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"external\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_type\"><td><b>Type</b></td>\n";
		msg += "<td><select id=\"type\" name=\"type\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"oracle\"";
		if (type != null && type.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (type != null && type.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_server_name\"><td><b>Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"server_name\" name=\"server_name\" value=" + serverName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_instance_name\"><td><b>Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"instance_name\" name=\"instance_name\" value=" + instanceName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_port\"><td><b>Port</b></td>";
		msg += "<td><input type=\"text\" id=\"port\" name=\"port\" onkeyup=\"this.value=this.value.replace(/[^\\d]/,'')\" value=" + port + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_database_name\"><td><b>Database Name</b></td>";
		msg += "<td><input type=\"text\" id=\"database_name\" name=\"database_name\" value=" + databaseName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_user_name\"><td><b>User Name</b></td>";
		msg += "<td><input type=\"text\" id=\"user_name\" name=\"user_name\" value=" + userName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_pass\"><td><b>Password</b></td>";
		msg += "<td><input type=\"password\" id=\"pass\" name=\"pass\" value=" + pass + ">";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><button onclick=\"submitExternal('0')\">" + buttonText + "</button>\n";
		msg += "<button onclick=\"submitExternal('1')\">Validate</button>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"update\">\n";
		msg += "<input type=\"hidden\" id=\"validate\" name=\"validate\" value=\"\">\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		if (id != null)
			msg += "<input type=\"hidden\" name=\"id\" value=\"" + id + "\">\n";
		msg += "</form>\n";

		if (!(validateMsg.equals("")))
		{
			msg += "<p>Connection Validation Result: " + validateMsg + "\n";
		}

		return msg;
	}
	
	public static String viewDelete(String id, String type, String serverName, String instanceName, String port, String databaseName, String userName, String pass)
	{
		serverName = OutsourcerView.setHTMLField(serverName);
		instanceName = OutsourcerView.setHTMLField(instanceName);
		port = OutsourcerView.setHTMLField(port);
		databaseName = OutsourcerView.setHTMLField(databaseName);
		userName = OutsourcerView.setHTMLField(userName);
		pass = OutsourcerView.setHTMLField(pass);

		String myScript = getJavaScriptFunctions();
		String onLoad = "disableInputFields()";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"external\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_type\"><td><b>Type</b></td>\n";
		msg += "<td><select id=\"type\" name=\"type\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"oracle\"";
		if (type != null && type.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (type != null && type.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_server_name\"><td><b>Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"server_name\" name=\"server_name\" value=" + serverName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_instance_name\"><td><b>Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"instance_name\" name=\"instance_name\" value=" + instanceName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_port\"><td><b>Port</b></td>";
		msg += "<td><input type=\"text\" id=\"port\" name=\"port\" value=" + port + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_database_name\"><td><b>Database Name</b></td>";
		msg += "<td><input type=\"text\" id=\"database_name\" name=\"database_name\" value=" + databaseName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_user_name\"><td><b>User Name</b></td>";
		msg += "<td><input type=\"text\" id=\"user_name\" name=\"user_name\" value=" + userName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_pass\"><td><b>Password</b></td>";
		msg += "<td><input type=\"password\" id=\"pass\" name=\"pass\" value=" + pass + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Delete\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"delete\">\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		if (id != null)
			msg += "<input type=\"hidden\" name=\"id\" value=\"" + id + "\">\n";
		msg += "</form>\n";

		return msg;
	}

	private static String getHeader(String sortBy, String sort)
	{

		String downArrow = "&#8595;";
		String upArrow = "&#8593;";
		String defaultSort = "asc";

		String manageHeader = "<th align=\"center\"><b>Manage</b></th>\n";
		String passHeader = "<th align=\"center\"><b>Password</b></th>\n";

		String IDHeader = "<th><b>ID</b><button ";
		String IDFocus = "";
		String IDArrow = downArrow;
		String IDSort = defaultSort;

		String typeHeader = "<th><b>Type</b><button ";
		String typeFocus = "";
		String typeArrow = downArrow;
		String typeSort = defaultSort;

		String serverNameHeader = "<th><b>Server</b><button ";
		String serverNameFocus = "";
		String serverNameArrow = downArrow;
		String serverNameSort = defaultSort;

		String instanceNameHeader = "<th><b>Instance</b><button ";
		String instanceNameFocus = "";
		String instanceNameArrow = downArrow;
		String instanceNameSort = defaultSort;

		String portHeader = "<th><b>Port</b><button ";
		String portFocus = "";
		String portArrow = downArrow;
		String portSort = defaultSort;

		String databaseNameHeader = "<th><b>Database</b><button ";
		String databaseNameFocus = "";
		String databaseNameArrow = downArrow;
		String databaseNameSort = defaultSort;

		String userNameHeader = "<th><b>User</b><button ";
		String userNameFocus = "";
		String userNameArrow = downArrow;
		String userNameSort = defaultSort;

		if (sortBy.equals("id"))
		{
			IDFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				IDSort = "desc";
			else
			{
				IDSort = "asc";
				IDArrow = upArrow;
			}
		}
		else if (sortBy.equals("type"))
		{
			typeFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				typeSort = "desc";
			else
			{
				typeSort = "asc";
				typeArrow = upArrow;
			}
		}
		else if (sortBy.equals("server_name"))
		{
			serverNameFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				serverNameSort = "desc";
			else
			{
				serverNameSort = "asc";
				serverNameArrow = upArrow;
			}
		}
		else if (sortBy.equals("instance_name"))
		{
			instanceNameFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				instanceNameSort = "desc";
			else
			{
				instanceNameSort = "asc";
				instanceNameArrow = upArrow;
			}
		}
		else if (sortBy.equals("port"))
		{
			portFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				portSort = "desc";
			else
			{
				portSort = "asc";
				portArrow = upArrow;
			}
		}
		else if (sortBy.equals("database_name"))
		{
			databaseNameFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				databaseNameSort = "desc";
			else
			{
				databaseNameSort = "asc";
				databaseNameArrow = upArrow;
			}
		}
		else if (sortBy.equals("user_name"))
		{
			userNameFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				userNameSort = "desc";
			else
			{
				userNameSort = "asc";
				userNameArrow = upArrow;
			}
		}

		IDHeader += IDFocus + "onclick=\"sortRS('id', '" + IDSort + "')\">" + IDArrow + "</button></th>\n";
		typeHeader += typeFocus + "onclick=\"sortRS('type', '" + typeSort + "')\">" + typeArrow + "</button></th>\n";
		serverNameHeader += serverNameFocus + "onclick=\"sortRS('server_name', '" + serverNameSort + "')\">" + serverNameArrow + "</button></th>\n";
		instanceNameHeader += instanceNameFocus + "onclick=\"sortRS('instance_name', '" + instanceNameSort + "')\">" + instanceNameArrow + "</button></th>\n";
		portHeader += portFocus + "onclick=\"sortRS('port', '" + portSort + "')\">" + portArrow + "</button></th>\n";
		databaseNameHeader += databaseNameFocus + "onclick=\"sortRS('database_name', '" + databaseNameSort + "')\">" + databaseNameArrow + "</button></th>\n";
		userNameHeader += userNameFocus + "onclick=\"sortRS('user_name', '" + userNameSort + "')\">" + userNameArrow + "</button></th>\n";
		
		String msg = "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += manageHeader + IDHeader + typeHeader + serverNameHeader + instanceNameHeader + portHeader + databaseNameHeader + userNameHeader + passHeader;
		msg += "</tr>\n";

		return msg;
	}

	public static String viewCreate(ResultSet databaseList, ResultSet schemaList, String id, String type, String serverName, String instanceName, String port, String databaseName, String userName, String targetSchemaName, String refreshType, ArrayList<String> scheduleList)
	{
		if (instanceName == null)
			instanceName = "";

		targetSchemaName = OutsourcerView.setHTMLField(targetSchemaName);

		String myScript = getJavaScriptFunctions();
		String msg = OutsourcerView.viewHeader(myScript, "", action);
		msg += "<form id=\"myForm\" action=\"external\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";

		msg += "<tr><td><b>Type</b></td><td>" + type + "</td></tr>\n";
		msg += "<tr><td><b>Server Name</b></td><td>" + serverName + "</td></tr>\n";
		if (type.equals("sqlserver"))
			msg += "<tr><td><b>Instance Name</b></td><td>" + instanceName + "</td></tr>\n";
		else if (type.equals("oracle"))
		{
			msg += "<tr><td><b>Port</b></td><td>" + port + "</td></tr>\n";
			msg += "<tr><td><b>Database Name</b></td><td>" + databaseName + "</td></tr>\n";
		}

		//Only SQL Server has the list of databases
		if (!(databaseList==null))
		{
			msg += "<tr><td><b>Database Name</b></td>";
			msg += "<td><select onchange=\"submitDatabase()\" id=\"database_name\" name=\"database_name\">\n";
			msg += "<option value=\"\"></option>\n";

			try
			{
				while (databaseList.next())
				{
					msg += "<option value=\"" + databaseList.getString(1) + "\"";
					if (databaseList.getString(1).equals(databaseName))
						msg += " selected";
					msg += ">"+ databaseList.getString(1) + "</option>\n";
				}
			}
			catch (SQLException ex)
			{
				msg += ex.getMessage();
			}
			
			msg += "</select></td></tr>\n";
		}

		msg += "<tr><td><b>Schema</b></td>";
		msg += "<td><select id=\"source_schema\" name=\"source_schema\">\n";
		msg += "<option value=\"\"></option>\n";

		if (!(schemaList==null))
		{
			try
			{
				while (schemaList.next())
				{
					msg += "<option value=\"" + schemaList.getString(1) + "\">"+ schemaList.getString(1) + "</option>\n";
				}
			}
			catch (SQLException ex)
			{
				msg += ex.getMessage();
			}
		}
		msg += "</select></td></tr>\n";

		msg += "<tr><td><b>User Name</b></td><td>" + userName + "</td></tr>\n";
		msg += "<tr><td><b>Password</b></td><td>********</td></tr>\n";
		msg += "<tr><td><b>Target Schema</b></td>\n";
		msg += "<td><input type=\"text\" id=\"target_schema\" name=\"target_schema\" value=" + targetSchemaName + "></td></tr>\n";
		msg += "<tr><td><b>Refresh Type</b></td>\n";
		msg += "<td><select id=\"refresh_type\" name=\"refresh_type\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"ddl\"";
		if (refreshType != null && refreshType.equals("ddl"))
			msg += " selected";
		msg += ">DDL</option>\n";
		msg += "<option value=\"refresh\"";
		if (refreshType != null && refreshType.equals("refresh"))
			msg += " selected";
		msg += ">Refresh</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr><td><b>Schedule</b></td>\n";
		msg += "<td><select id=\"schedule_desc\" name=\"schedule_desc\">\n";
		msg += "<option value=\"\"></option>\n";
		for (int i = 0; i < scheduleList.size(); i++)
		{
			msg += "<option value=\"" + scheduleList.get(i) + "\">" + scheduleList.get(i) + "</option>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"button\" onclick=\"createJobs()\" value=\"Create Jobs\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" id=\"id\" name=\"id\" value=\"" + id + "\">\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"create\">\n";
		msg += "</form>\n";
		return msg;
	}

	public static String viewResults(int count)
	{
		String myScript = getJavaScriptFunctions();
		String msg = OutsourcerView.viewHeader(myScript, "", action);
		msg += "<p>Successfully created " + Integer.toString(count) + " Jobs!</p>";
		return msg;
	}
}
