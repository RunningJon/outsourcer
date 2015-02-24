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
		myScript += "function updateCustomSQL(description, myAction)\n";
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
		msg += "<td><h4><a href=\"?action_type=update\">Define New CustomSQL</a></h4></td>\n";
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

		String myScript = "function loadPage()\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"table_name\").focus()\n";
		myScript += "}\n";

		return myScript;

	}
/*
	public static String viewUpdate(String id, String tableName, String columns, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, int sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass) 
	{

		String buttonText = "";
		if (id == null)
		{
			description = "";
			buttonText = "Insert";
		}
		else
		{
			buttonText = "Update";
		}

		tableName = OutsourcerView.setHTMLField(tableName);
		columns = OutsourcerView.setHTMLField(columns);
		sqlText = OutsourcerView.setHTMLField(sqlText);
		sourceType = OutsourcerView.setHTMLField(sourceType);
		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);
		
		String myScript = getJavaScriptFunctions();
		String onLoad="loadPage()";

		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"custom\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr id=\"r_table_name\"><td><b>Table Name</b></td>\n";
		msg += "<td><input type=\"text\" id=\"table_name\" name=\"table_name\" value=" + tableName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_columns\"><td><b>Columns</b></td>\n";
		msg += "<td><input type=\"text\" id=\"columns\" name=\"columns\" value=" + columns + ">";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"" + buttonText + "\"></td></tr>\n";
		msg += "</table>\n";
		if (description.equals(""))
			msg += "<input type=\"hidden\" name=\"action_type\" value=\"insert\">\n";
		else
			msg += "<input type=\"hidden\" name=\"action_type\" value=\"update\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "</form>\n";
		return msg;
	}

	public static String viewDelete(String description, String tableName, String intervalQuantity)
	{
		description = OutsourcerView.setHTMLField(description);
		tableName = OutsourcerView.setHTMLField(tableName);
		intervalQuantity = OutsourcerView.setHTMLField(intervalQuantity);
		
		String myScript = "";
		String onLoad="";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"custom\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td width=\"30%\"><b>Description</b></td>";
		msg += "<td><input type=\"text\" id=\"description\" name=\"description\" value=" + description + " readonly></td></tr>\n";
		msg += "<tr id=\"r_table_name\"><td><b>Interval Trunc</b></td>\n";
		msg += "<td><input type=\"text\" id=\"table_name\" name=\"table_name\" value=" + tableName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_interval_quantity\"><td><b>Interval Quantity</b></td>\n";
		msg += "<td><input type=\"text\" id=\"interval_quantity\" name=\"interval_quantity\" value=" + intervalQuantity + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Delete\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"delete\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "</form>\n";

		return msg;
	}
*/
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
/*
	public static String viewCreate(String id, String tableName, String columns, String sqlText, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceUserName, String sourcePass)

	{
		String myScript = getHead();
		String msg = OutsourcerView.viewHeader(myScript, "", action);
		msg += "<form id=\"myForm\" action=\"custom\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td><b>CustomSQL</b></td><td>" + scheduleDesc + "</td></tr>\n";
		msg += "<tr><td><b>Greenplum Schema</b></td>";
		msg += "<td><select id=\"schema\" name=\"schema\">\n";
		msg += "<option value=\"\"></option>\n";
		for (int i = 0; i < schemaList.size(); i++)
		{
			msg += "<option value=\"" + schemaList.get(i) + "\">" + schemaList.get(i) + "</option>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Update CustomSQLs\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" id=\"description\" name=\"description\" value=\"" + scheduleDesc + "\">\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"create\">\n";
		msg += "</form>\n";	
		return msg;
	}
*/
}
