import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class CustomSQLView
{
	public static String action = "custom";

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
		myScript += "function updateCustomSQL(id, myAction)\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"action_type\").value = myAction;\n";
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
		msg += "<td><h4><a href=\"?action_type=create\">Define New CustomSQL</a></h4></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "<br>\n";
		msg += getHeader(sortBy, sort);


		try
		{
			msg += OutsourcerView.viewResults(limit, offset, rs);
		}
		catch (Exception ex)
		{
			msg += ex.getMessage();
		}
		return msg;
	}

	private static String getJavaScriptFunctions()
	{

		String myScript = "function disableInputFields()\n";
		myScript += "{\n";
		myScript += "   var val = document.getElementById(\"source_type\").selectedIndex;\n";
		myScript += "   document.getElementById(\"r_source_type\").style.display = \"\";\n";
		myScript += "   document.getElementById(\"r_source_user_name\").style.display = \"\";\n";
		myScript += "   document.getElementById(\"r_source_pass\").style.display = \"\";\n";
		myScript += "   //0 is the first entry which is blank\n";
		myScript += "   if (val == 1) //Oracle\n";
		myScript += "   {\n";
		myScript += "           document.getElementById(\"source_instance_name\").value = \"\";\n";
		myScript += "           document.getElementById(\"r_source_instance_name\").style.display = \"none\";\n";
		myScript += "           document.getElementById(\"r_source_database_name\").style.display = \"\";\n";
		myScript += "           document.getElementById(\"r_source_port\").style.display = \"\";\n";
		myScript += "   }\n";
		myScript += "   if (val == 2) //SQL Server\n";
		myScript += "   {\n";
		myScript += "           document.getElementById(\"source_port\").value = \"\";\n";
		myScript += "           document.getElementById(\"r_source_port\").style.display = \"none\";\n";
		myScript += "           document.getElementById(\"r_source_instance_name\").style.display = \"\";\n";
		myScript += "           document.getElementById(\"r_source_database_name\").style.display = \"none\";\n";
		myScript += "   }\n";
		myScript += "}\n";
		myScript += "function loadPage()\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"table_name\").focus()\n";
		myScript += "}\n";

		return myScript;

	}

	public static String viewUpdate(String id, String tableName, String columns, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) 
	{

		String buttonText = "Update";
		tableName = OutsourcerView.setHTMLField(tableName);
		columns = OutsourcerView.setHTMLTextArea(columns);
		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);
		
		String myScript = getJavaScriptFunctions();
		String onLoad="disableInputFields()";

		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"custom\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_table_name\"><td><b>Table Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"table_name\" name=\"table_name\" value=" + tableName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_columns\"><td><b>Columns</b></td>\n";
		msg += "<td><textarea cols=\"50\" rows=\"10\" id=\"columns\" name=\"columns\">" + columns + "</textarea>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_sql_text\"><td><b>SQL Text</b></td>\n";
		msg += "<td><textarea cols=\"50\" rows=\"10\" id=\"sql_text\" name=\"sql_text\">" + sqlText + "</textarea>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=" + sourceInstanceName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Source Port</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" onkeyup=\"this.value=this.value.replace(/[^\\d]/,'')\" value=" + sourcePort + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=" + sourceDatabaseName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=" + sourceUserName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>\n";
		msg += "<td><input type=\"password\" id=\"source_pass\" name=\"source_pass\" value=" + sourcePass + ">";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"" + buttonText + "\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"id\" value=\"" + id + "\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"update\">\n";
		msg += "</form>\n";
		return msg;
	}

	public static String viewDelete(String id, String tableName, String columns, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) 
	{
		String buttonText = "Delete";
		tableName = OutsourcerView.setHTMLField(tableName);
		columns = OutsourcerView.setHTMLTextArea(columns);
		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);
		
		String myScript = "";
		String onLoad= "";

		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"custom\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_id\"><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_table_name\"><td><b>Table Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"table_name\" name=\"table_name\" value=" + tableName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_columns\"><td><b>Columns</b></td>\n";
		msg += "<td><textarea readonly cols=\"50\" rows=\"10\" id=\"columns\" name=\"columns\">" + columns + "</textarea>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_sql_text\"><td><b>SQL Text</b></td>\n";
		msg += "<td><textarea readonly cols=\"50\" rows=\"10\" id=\"sql_text\" name=\"sql_text\">" + sqlText + "</textarea>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=" + sourceInstanceName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Source Port</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" value=" + sourcePort + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=" + sourceDatabaseName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=" + sourceUserName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>\n";
		msg += "<td><input type=\"password\" id=\"source_pass\" name=\"source_pass\" value=" + sourcePass + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"" + buttonText + "\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"id\" value=\"" + id + "\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"delete\">\n";
		msg += "</form>\n";
		return msg;
	}

	private static String getHeader(String sortBy, String sort)
	{

		String downArrow = "&#8595;";
		String upArrow = "&#8593;";
		String defaultSort = "asc";

		String manageHeader = "<th><b>Manage</b></th>\n";

		String idHeader = "<th><b>ID</b><button ";
		String idFocus = "";
		String idArrow = downArrow;
		String idSort = defaultSort;

		String tableNameHeader = "<th><b>Table Name</b><button ";
		String tableNameFocus = "";
		String tableNameArrow = downArrow;
		String tableNameSort = defaultSort;

		String sourceTypeHeader = "<th><b>Source Type</b><button ";
		String sourceTypeFocus = "";
		String sourceTypeArrow = downArrow;
		String sourceTypeSort = defaultSort;

		if (sortBy.equals("id"))
		{
			idFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				idSort = "desc";
			else
			{
				idSort = "asc";
				idArrow = upArrow;
			}
		}
		else if (sortBy.equals("table_name"))
		{
			tableNameFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				tableNameSort = "desc";
			else
			{
				tableNameSort = "asc";
				tableNameArrow = upArrow;
			}
		}
		else if (sortBy.equals("source_type"))
		{
			sourceTypeFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				sourceTypeSort = "desc";
			else
			{
				sourceTypeSort = "asc";
				sourceTypeArrow = upArrow;
			}
		}

		idHeader += idFocus + "onclick=\"sortRS('id', '" + idSort + "')\">" + idArrow + "</button></th>\n";
		tableNameHeader += tableNameFocus + "onclick=\"sortRS('table_name', '" + tableNameSort + "')\">" + tableNameArrow + "</button></th>\n";
		sourceTypeHeader += sourceTypeFocus + "onclick=\"sortRS('source_type', '" + sourceTypeSort + "')\">" + sourceTypeArrow + "</button></th>\n";
		
		String msg = "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += manageHeader + idHeader + tableNameHeader + sourceTypeHeader;
		msg += "</tr>\n";

		return msg;
	}

	//public static String viewCreate(String id, String tableName, String columns, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) 
	public static String viewCreate(String tableName, String columns, String sqlText, ArrayList<String> extConnectionIdList)
	{
		String myScript = getHead();
		String[] parts;
		String optionValue = "";
		String extConnectionId = "";

		//String msg = OutsourcerView.viewHeader(myScript, "", action);
		String msg = OutsourcerView.viewHeader(myScript, "", "");
		msg += "<form id=\"myForm\" action=\"custom\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";

		msg += "<tr><td><b>Table Name</b></td>";
		msg += "<td><input type=\"text\" id=\"table_name\" name=\"table_name\" value=" + tableName + ">";
		msg += "</td></tr>\n";

		msg += "<tr id=\"r_columns\"><td><b>Columns</b></td>\n";
		msg += "<td><textarea cols=\"50\" rows=\"10\" id=\"columns\" name=\"columns\">" + columns + "</textarea>";
		msg += "</td></tr>\n";

		msg += "<tr id=\"r_sql_text\"><td><b>SQL Text</b></td>\n";
		msg += "<td><textarea cols=\"50\" rows=\"10\" id=\"sql_text\" name=\"sql_text\">" + sqlText + "</textarea>";
		msg += "</td></tr>\n";

		msg += "<tr><td><b>External Connection ID</b></td>";
		msg += "<td><select id=\"ext_connection_id\" name=\"ext_connection_id\">\n";
		//msg += "<td><input type=\"text\" id=\"ext_connection_id\" name=\"ext_connection_id\" value=" + extConnectionId + ">";
		msg += "<option value=\"\"></option>\n";
		for (int i = 0; i < extConnectionIdList.size(); i++)
		{
			optionValue = extConnectionIdList.get(i); 	
			parts = optionValue.split(";");
			extConnectionId = parts[0];

			msg += "<option value=\"" + extConnectionId + "\">" + optionValue + "</option>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Insert\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"create\">\n";
		msg += "</form>\n";	
		return msg;
	}
}
