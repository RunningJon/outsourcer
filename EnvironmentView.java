import java.util.Map;
import java.sql.*;

public class EnvironmentView
{
	public static String action = "environment";

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

		return myScript;
	}

	public static String viewList(String search, ResultSet rs, String limit, String offset, String sortBy, String sort)
	{
		String myScript = getHead();
		String msg = OutsourcerView.viewSearch(action, search, limit, offset, sortBy, sort, myScript);
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

	private static String getHeader(String sortBy, String sort)
	{

		String downArrow = "&#8595;";
		String upArrow = "&#8593;";
		String defaultSort = "asc";

		String nameHeader = "<th><b>Name</b><button ";
		String nameFocus = "";
		String nameArrow = downArrow;
		String nameSort = defaultSort;
		String valueHeader = "<th><b>Value</b><button ";
		String valueFocus = "";
		String valueArrow = downArrow;
		String valueSort = defaultSort;

		if (sortBy.equals("name"))
		{
			nameFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				nameSort = "desc";
			else
			{
				nameSort = "asc";
				nameArrow = upArrow;
			}
		}
		else if (sortBy.equals("value"))
		{
			valueFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				valueSort = "desc";
			else
			{
				valueSort = "asc";
				valueArrow = upArrow;
			}
		}

		nameHeader += nameFocus + "onclick=\"sortRS('name', '" + nameSort + "')\">" + nameArrow + "</button></th>\n";
		valueHeader += valueFocus + "onclick=\"sortRS('value', '" + valueSort + "')\">" + valueArrow + "</button></th>\n";
		
		String msg = "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += nameHeader + valueHeader;
		msg += "</tr>\n";

		return msg;
	}

	public static String viewStartStop(String status) 
	{
		String msg = OutsourcerView.viewHeader("", "", action);
		msg += "<form action=\"environment\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += "<td width=\"50%\"><b>Queue Daemon Status</td><td width=\"50%\"><b>" + status + "</b></td>\n";
		msg += "</tr>\n";
		msg += "<tr>\n";
		msg += "<td width=\"50%\"><b>Start/Stop</b></td><td width=\"50%\"><input type=\"hidden\" name=\"submit_form\" value=\"1\"><input type=\"hidden\" name=\"action_type\" value=\"Daemon\"><input type=\"submit\" value=\"Submit\"></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "</form>\n";
		
		return msg;
	}

	public static String viewAgentStartStop(String status) 
	{
		String msg = OutsourcerView.viewHeader("", "", action);
		msg += "<form action=\"environment\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += "<td width=\"50%\"><b>Scheduler Daemon Status</td><td width=\"50%\"><b>" + status + "</b></td>\n";
		msg += "</tr>\n";
		msg += "<tr>\n";
		msg += "<td width=\"50%\"><b>Start/Stop</b></td><td width=\"50%\"><input type=\"hidden\" name=\"submit_form\" value=\"1\"><input type=\"hidden\" name=\"action_type\" value=\"Agent\"><input type=\"submit\" value=\"Submit\"></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "</form>\n";
		
		return msg;
	}

	public static String viewMaxJobs(String maxJobs)
	{
		String msg = OutsourcerView.viewHeader("", "", action);
		msg += "<form action=\"environment\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += "<td width=\"33%\"><b>Max Jobs</b></td><td width=\"33%\"><input type=\"text\" id=\"max_jobs\" name=\"max_jobs\" value=" + maxJobs + "></td>\n";
		msg += "<td width=\"33%\"><input type=\"hidden\" name=\"submit_form\" value=\"1\"><input type=\"hidden\" name=\"action_type\" value=\"max_jobs\"><input type=\"submit\" value=\"Submit\"></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "</form>\n";
		
		return msg;
	}

	public static String viewFetchSize(String oFetchSize)
	{
		String msg = OutsourcerView.viewHeader("", "", action);
		msg += "<form action=\"environment\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += "<td width=\"33%\"><b>Oracle Fetch Size</b></td><td width=\"33%\"><input type=\"text\" id=\"oFetchSize\" name=\"oFetchSize\" value=" + oFetchSize + "></td>\n";
		msg += "<td width=\"33%\"><input type=\"hidden\" name=\"submit_form\" value=\"1\"><input type=\"hidden\" name=\"action_type\" value=\"oFetchSize\"><input type=\"submit\" value=\"Submit\"></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "</form>\n";
		
		return msg;
	}
}
