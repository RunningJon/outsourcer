import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class ScheduleView
{
	public static String action = "schedule";

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
		myScript += "function updateSchedule(description, myAction)\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"action_type\").value = myAction;\n";
		myScript += "   document.getElementById(\"offset\").value = 0;\n";
		myScript += "   document.getElementById(\"submit_form\").value = \"0\";\n";
		myScript += "   document.getElementById(\"description\").value = description;\n";
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
		msg += "<td><h4><a href=\"?action_type=update\">Define New Schedule</a></h4></td>\n";
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
		myScript += "   document.getElementById(\"description\").focus()\n";
		myScript += "}\n";

		return myScript;

	}

	public static String viewUpdate(String description, String intervalTrunc, String intervalQuantity)
	{

		String buttonText = "";
		if (description == null)
		{
			description = "";
			buttonText = "Insert";
		}
		else
		{
			buttonText = "Update";
		}

		intervalTrunc = OutsourcerView.setHTMLField(intervalTrunc);
		intervalQuantity = OutsourcerView.setHTMLField(intervalQuantity);
		
		String myScript = getJavaScriptFunctions();
		String onLoad="loadPage()";

		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"schedule\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr id=\"r_description\"><td><b>Description</b></td>";
		if (description.equals(""))
			msg += "<td><input type=\"text\" id=\"description\" name=\"description\" value=" + description + "></td></tr>\n";
		else
			msg += "<td>" + description + "</td></tr>\n";
		msg += "<tr id=\"r_interval_trunc\"><td><b>Interval Trunc</b></td>\n";
		msg += "<td><input type=\"text\" id=\"interval_trunc\" name=\"interval_trunc\" value=" + intervalTrunc + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_interval_quantity\"><td><b>Interval Quantity</b></td>\n";
		msg += "<td><input type=\"text\" id=\"interval_quantity\" name=\"interval_quantity\" value=" + intervalQuantity + ">";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"" + buttonText + "\"></td></tr>\n";
		msg += "</table>\n";
		if (description.equals(""))
			msg += "<input type=\"hidden\" name=\"action_type\" value=\"insert\">\n";
		else
			msg += "<input type=\"hidden\" name=\"action_type\" value=\"update\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		if (!(description.equals("")))
			msg += "<input type=\"hidden\" name=\"description\" value=\"" + description + "\">\n";
		msg += "</form>\n";
		return msg;
	}

	public static String viewDelete(String description, String intervalTrunc, String intervalQuantity)
	{
		intervalTrunc = OutsourcerView.setHTMLField(intervalTrunc);
		intervalQuantity = OutsourcerView.setHTMLField(intervalQuantity);
		
		String myScript = "";
		String onLoad="";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"schedule\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td><b>Description</b></td>";
		msg += "<td>" + description + "</td></tr>\n";
		msg += "<tr id=\"r_interval_trunc\"><td><b>Interval Trunc</b></td>\n";
		msg += "<td><input type=\"text\" id=\"interval_trunc\" name=\"interval_trunc\" value=" + intervalTrunc + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_interval_quantity\"><td><b>Interval Quantity</b></td>\n";
		msg += "<td><input type=\"text\" id=\"interval_quantity\" name=\"interval_quantity\" value=" + intervalQuantity + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Delete\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"delete\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" name=\"description\" value=\"" + description + "\">\n";
		msg += "</form>\n";

		return msg;
	}

	private static String getHeader(String sortBy, String sort)
	{

		String downArrow = "&#8595;";
		String upArrow = "&#8593;";
		String defaultSort = "asc";

		String manageHeader = "<th><b>Manage</b></th>\n";

		String descriptionHeader = "<th><b>Description</b><button ";
		String descriptionFocus = "";
		String descriptionArrow = downArrow;
		String descriptionSort = defaultSort;

		String intervalTruncHeader = "<th><b>Interval Trunc</b><button ";
		String intervalTruncFocus = "";
		String intervalTruncArrow = downArrow;
		String intervalTruncSort = defaultSort;

		String intervalQuantityHeader = "<th><b>Interval Quantity</b><button ";
		String intervalQuantityFocus = "";
		String intervalQuantityArrow = downArrow;
		String intervalQuantitySort = defaultSort;

		if (sortBy.equals("description"))
		{
			descriptionFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				descriptionSort = "desc";
			else
			{
				descriptionSort = "asc";
				descriptionArrow = upArrow;
			}
		}
		else if (sortBy.equals("interval_trunc"))
		{
			intervalTruncFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				intervalTruncSort = "desc";
			else
			{
				intervalTruncSort = "asc";
				intervalTruncArrow = upArrow;
			}
		}
		else if (sortBy.equals("interval_quantity"))
		{
			intervalQuantityFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				intervalQuantitySort = "desc";
			else
			{
				intervalQuantitySort = "asc";
				intervalQuantityArrow = upArrow;
			}
		}

		descriptionHeader += descriptionFocus + "onclick=\"sortRS('description', '" + descriptionSort + "')\">" + descriptionArrow + "</button></th>\n";
		intervalTruncHeader += intervalTruncFocus + "onclick=\"sortRS('interval_trunc', '" + intervalTruncSort + "')\">" + intervalTruncArrow + "</button></th>\n";
		intervalQuantityHeader += intervalQuantityFocus + "onclick=\"sortRS('interval_quantity', '" + intervalQuantitySort + "')\">" + intervalQuantityArrow + "</button></th>\n";
		
		String msg = "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += manageHeader + descriptionHeader + intervalTruncHeader + intervalQuantityHeader;
		msg += "</tr>\n";

		return msg;
	}

	public static String viewCreate(String scheduleDesc, ArrayList<String> schemaList)
	{
		String myScript = getHead();
		String msg = OutsourcerView.viewHeader(myScript, "", action);
		msg += "<form id=\"myForm\" action=\"schedule\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td><b>Schedule</b></td><td>" + scheduleDesc + "</td></tr>\n";
		msg += "<tr><td><b>Greenplum Schema</b></td>";
		msg += "<td><select id=\"schema\" name=\"schema\">\n";
		msg += "<option value=\"\"></option>\n";
		for (int i = 0; i < schemaList.size(); i++)
		{
			msg += "<option value=\"" + schemaList.get(i) + "\">" + schemaList.get(i) + "</option>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Update Schedules\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" id=\"description\" name=\"description\" value=\"" + scheduleDesc + "\">\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"create\">\n";
		msg += "</form>\n";	
		return msg;
	}

}
