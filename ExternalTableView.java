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
		myScript += "	var ver = \"" + UI.gpVersion + "\";\n";
		myScript += "	var val = document.getElementById(\"source_type\").selectedIndex;\n";
		myScript += "	var act = document.getElementById(\"action_type\").value;\n";
		myScript += " 	document.getElementById(\"r_source_type\").style.display = \"\";\n";
		myScript += "	document.getElementById(\"r_source_user_name\").style.display = \"\";\n";
		myScript += "	document.getElementById(\"r_source_pass\").style.display = \"\";\n";
		myScript += "	if (val == 0) //Oracle\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"source_instance_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"r_source_instance_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_database_name\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_source_port\").style.display = \"\";\n";
		myScript += "	}\n";
		myScript += "	if (val == 1) //SQL Server\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"source_port\").value = \"\";\n";
		myScript += "		document.getElementById(\"r_source_port\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_instance_name\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_source_database_name\").style.display = \"none\";\n";
		myScript += "	}\n";
		myScript += "	if (act == \"create\")\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"r_target_append_only\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_source_database_name\").style.display = \"\";\n";
		myScript += "		var a = document.getElementById(\"target_append_only\").selectedIndex;\n";
		myScript += "		if (ver == \"HAWQ\")\n";
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_target_append_only\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"target_append_only\").value = \"true\";\n";
		myScript += "		}\n";
		myScript += "		if (a == 0) // Append Only\n";
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_target_compressed\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_target_row_orientation\").style.display = \"\";\n";
		myScript += "		} else\n";
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_target_compressed\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_target_row_orientation\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"target_append_only\").value = \"false\";\n";
		myScript += "			document.getElementById(\"target_compressed\").value = \"false\";\n";
		myScript += "			document.getElementById(\"target_row_orientation\").value = \"true\";\n";
		myScript += "		}\n";
		myScript += "	} else if (val == 1)\n"; //SQL Server
		myScript += "	{\n";
		myScript += "		document.getElementById(\"r_source_database_name\").style.display = \"none\";\n";
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

	public static String viewUpdate(String id, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass, String validateMsg)
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

		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);

		String myScript = getJavaScriptFunctions();
		String onLoad="disableInputFields()";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form id=\"myForm\" action=\"external\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=" + sourceInstanceName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Source Port</b></td>";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" onkeyup=\"this.value=this.value.replace(/[^\\d]/,'')\" value=" + sourcePort + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=" + sourceDatabaseName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=" + sourceUserName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>";
		msg += "<td><input type=\"password\" id=\"source_pass\" name=\"source_pass\" value=" + sourcePass + ">";
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
	
	public static String viewDelete(String id, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass)
	{
		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);

		String myScript = getJavaScriptFunctions();
		String onLoad = "disableInputFields()";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form id=\"myForm\" action=\"external\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=" + sourceInstanceName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Source Port</b></td>";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" value=" + sourcePort + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=" + sourceDatabaseName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=" + sourceUserName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>";
		msg += "<td><input type=\"password\" id=\"source_pass\" name=\"source_pass\" value=" + sourcePass + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Delete\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"delete\">\n";
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
		else if (sortBy.equals("sourceType"))
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

	public static String viewCreate(ResultSet databaseList, ResultSet schemaList, String id, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String targetSchemaName, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String refreshType, ArrayList<String> scheduleList)
	{
		if (sourceInstanceName == null)
			sourceInstanceName = "";

		targetSchemaName = OutsourcerView.setHTMLField(targetSchemaName);

		String myScript = getJavaScriptFunctions();
		String onLoad="disableInputFields()";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form id=\"myForm\" action=\"external\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=\"" + sourceInstanceName + "\" readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Source Port</b></td>";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" value=" + sourcePort + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>\n";

		//Only SQL Server has the list of databases
		if (!(databaseList==null))
		{
			msg += "<td><select onchange=\"submitDatabase()\" id=\"source_database_name\" name=\"source_database_name\">\n";
			msg += "<option value=\"\"></option>\n";

			try
			{
				while (databaseList.next())
				{
					msg += "<option value=\"" + databaseList.getString(1) + "\"";
					if (databaseList.getString(1).equals(sourceDatabaseName))
						msg += " selected";
					msg += ">"+ databaseList.getString(1) + "</option>\n";
				}
			}
			catch (SQLException ex)
			{
				msg += ex.getMessage();
			}
			
			msg += "</select></td></tr>\n";
		} else
		{
			msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=\"" + sourceDatabaseName + "\" readonly>";
			msg += "</td></tr>\n";
		}

		msg += "<tr id=\"r_source_schema_name\"><td><b>Source Schema</b></td>";
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

		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=\"" + sourceUserName + "\" readonly>\n";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_pass\" name=\"source_pass\" value=\"********\" readonly></td></tr>\n";
		msg += "<tr id=\"r_target_schema_name\"><td><b>Target Schema</b></td>\n";
		msg += "<td><input type=\"text\" id=\"target_schema\" name=\"target_schema\" value=" + targetSchemaName + "></td></tr>\n";

		msg += "<tr id=\"r_target_append_only\"><td><b>Target Append-Only</b></td>\n";
		msg += "<td><select id=\"target_append_only\" name=\"target_append_only\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"true\"";
		if (targetAppendOnly == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetAppendOnly == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_target_compressed\"><td><b>Target Compressed</b></td>\n";
		msg += "<td><select id=\"target_compressed\" name=\"target_compressed\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"true\"";
		if (targetCompressed == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetCompressed == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_target_row_orientation\"><td><b>Target Row Orientation</b></td>\n";
		msg += "<td><select id=\"target_row_orientation\" name=\"target_row_orientation\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"true\"";
		if (targetRowOrientation == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetRowOrientation == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";

		msg += "<tr id=\"r_refresh_type\"><td><b>Refresh Type</b></td>\n";
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
		msg += "<tr id=\"r_schedule\"><td><b>Schedule</b></td>\n";
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
