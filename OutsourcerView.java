import java.sql.*;

public class OutsourcerView
{

	public static String action = "/";

	public static String focus = "style=\"background-color:#707070\" ";

	public static String viewSearch(String formAction, String search, String limit, String offset, String sortBy, String sort, String myScript)
	{
		String searchHTML = setHTMLField(search);

		String msg = viewHeader(myScript, "", formAction);
		msg += "<form name=\"myForm\" id=\"myForm\" action=\"" + formAction + "\" method=\"post\">\n";
		msg += "<table class=\"ostable\">\n";
		msg += "<tr>\n";
		msg += "<td colspan=\"4\"><input type=\"text\" id=\"search\" name=\"search\" size=\"100\" value=" + searchHTML +">\n";
		msg += "<td width=\"20\"><b>Limit</b></td><td width=\"20\" align=\"left\"><select name=\"limit\">\n";
		msg += "<option value=\"10\"";
		if (limit.equals("10"))
			msg += " selected";
		msg += ">10</option>\n";
		msg += "<option value=\"20\"";
		if (limit.equals("20"))
			msg += " selected";
		msg += ">20</option>\n";
		msg += "<option value=\"30\"";
		if (limit.equals("30"))
			msg += " selected";
		msg += ">30</option>\n";
		msg += "<option value=\"\"";
		if (limit.equals(""))
			msg += " selected";
		msg += ">All</option>\n";
		msg += "</select></td>\n";
		msg += "<td align=\"left\"><button onclick=\"formSubmit(0)\">Search</button></td>\n";
		msg += "</tr>\n";
		msg += "<input type=\"hidden\" id=\"offset\" name=\"offset\" value=\"" + offset + "\">\n";
		msg += "<input type=\"hidden\" id=\"sort_by\" name=\"sort_by\" value=\"" + sortBy + "\">\n";
		msg += "<input type=\"hidden\" id=\"sort\" name=\"sort\" value=\"" + sort + "\">\n";
		msg += "<input type=\"hidden\" id=\"action_type\" name=\"action_type\" value=\"\">\n";
		msg += "<input type=\"hidden\" id=\"queueID\" name=\"queueID\" value=\"\">\n";
		msg += "<input type=\"hidden\" id=\"queue_action\" name=\"queue_action\" value=\"\">\n";
		msg += "<input type=\"hidden\" id=\"id\" name=\"id\" value=\"\">\n";
		msg += "<input type=\"hidden\" id=\"description\" name=\"description\" value=\"\">\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		msg += "</table>\n";

		return msg;
	}


	public static String getHead(String myScript, String onLoad)
	{
		String msg = "<html><head>\n";
		msg += "<style type=\"text/css\">\n";

		msg += "p{font-family:Arial, Helvetica, sans-serif}\n";
		msg += "h1{font-family:Arial, Helvetica, sans-serif}\n";
		msg += "h2{font-family:Arial, Helvetica, sans-serif}\n";
		msg += "h3{font-family:Arial, Helvetica, sans-serif}\n";
		msg += "h4{font-family:Arial, Helvetica, sans-serif}\n";
		msg += "h5{font-family:Arial, Helvetica, sans-serif}\n";

		msg += "a:link    {color:#008000;text-decoration:none;}\n";
		msg += "a:visited {color:#008000;text-decoration:none;}\n";
		msg += "a:hover   {color:#000000;text-decoration:none;}\n";
		msg += "a:active  {color:#000000;text-decoration:none;}\n";
		msg += "a.m1:link    {color:#008000;text-decoration:none;}\n";
		msg += "a.m1:visited {color:#008000;text-decoration:none;}\n";
		msg += "a.m1:hover   {color:#FFFFFF;text-decoration:none;}\n";
		msg += "a.m1:active  {color:#FFFFFF;text-decoration:none;}\n";
		msg += "a.m2:link {color:#FFFFFF;text-decoration:none;}\n";
		msg += "a.m2:visited {color:#FFFFFF;text-decoration:none;}\n";
		msg += "a.m2:hover   {color:#FFFFFF;text-decoration:none;}\n";
		msg += "a:m2:active  {color:#FFFFFF;text-decoration:none;}\n";

		msg += "table.ostable {font-family:Arial, Helvetica, sans-serif;font-size:12px;color:#333333;width:100%;border-width: 0px;border-collapse: collapse;}\n";
		msg += "table.tftable {font-family:Arial, Helvetica, sans-serif;font-size:12px;color:#333333;width:100%;border-width: 1px;border-color: #9dcc7a;border-collapse: collapse;}\n";
		msg += "table.logintable {font-family:Arial, Helvetica, sans-serif;font-size:14px;color:#333333;border-width: 1px;border-color: #9dcc7a;border-collapse: collapse;}\n";
		msg += "table.logintable th {font-family:Arial, Helvetica, sans-serif;font-size:14px;color:#FF0000;}\n";
		msg += "table.tftable th {font-family:Arial, Helvetica, sans-serif;font-size:12px;background-color:#abd28e;border-width: 1px;padding: 8px;border-style: solid;border-color: #9dcc7a;text-align:center;}\n";
		msg += "table.tftable tr {background-color:#ffffff;}\n";
		msg += "table.tftable td {font-family:Arial, Helvetica, sans-serif;font-size:12px;border-width: 1px;padding: 8px;border-style: solid;border-color: #9dcc7a;}\n";

		msg += "</style>\n";

		if (!myScript.equals(""))
		{
			msg += "<script type=\"text/javascript\">\n";
			msg += myScript + "\n";
			msg += "</script>\n";
		} 
		msg += "</head>\n";

		if (!onLoad.equals(""))
		{
			msg += "<body onload=\"" + onLoad + "\">\n";
		}
		else
		{
			msg += "<body>\n";
		}

		return msg;

	}
	public static String viewHeader(String myScript, String onLoad, String page)
	{
		String externalHighlight = "class=\"m1\" ";
		String jobsHighlight = "class=\"m1\" ";
		String queueHighlight = "class=\"m1\" ";
		String scheduleHighlight = "class=\"m1\" ";
		String environmentHighlight = "class=\"m1\" ";
		String highlight = "class=\"m2\" ";

		if (page.equals("external"))
			externalHighlight = highlight;
		else if (page.equals("jobs"))
			jobsHighlight = highlight;
		else if (page.equals("queue"))
			queueHighlight = highlight;
		else if (page.equals("schedule"))
			scheduleHighlight = highlight;
		else if (page.equals("environment"))
			environmentHighlight = highlight;

		String msg = getHead(myScript, onLoad);
		msg += "<table class=\"ostable\">\n";
		msg += "<tr><td><h1><a href=\"/\">Outsourcer</a></h1></td>\n";
		msg += "<td align=\"right\"><a href=\"/?action_type=logout\">Logout</a></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "<table>\n";
		msg += "<table class=\"tftable\">\n";
		msg += "<tr>\n";
		msg += "<th width=\"20%\"><a " + externalHighlight + "href=\"external\"><h3>Sources</h3></a></th>\n";
		msg += "<th width=\"20%\"><a " + jobsHighlight + "href=\"jobs\"><h3>Jobs</h3></a></th>\n";
		msg += "<th width=\"20%\"><a " + queueHighlight + "href=\"queue\"><h3>Queue</h3></a></th>\n";
		msg += "<th width=\"20%\"><a " + scheduleHighlight + "href=\"schedule\"><h3>Schedules</h3></a></th>\n";
		msg += "<th width=\"20%\"><a " + environmentHighlight + "href=\"environment\"><h3>Environment</h3></a></th>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		return msg;
	}

	public static String viewFooter()
	{
		String msg = "</body></html>\n";
		return msg;
	}
	public static String viewError(String errorMsg)
	{
		String msg = "<h1>Error</h1>";
		msg += errorMsg;
		return msg;
	}

	public static String viewMain()
	{
		String msg = viewHeader("", "", "");
		msg += "<p><b><i>Outsourcer</b></i> is a brought to you by <a href=\"http://www.pivotalguru.com\" target=\"_new\">PivotalGuru.com</a></br>\n";
		msg += "</p>\n";
		return msg;
	}

	public static String viewPageNotFound()
	{
		String msg = viewHeader("", "", "");	
		msg = "Page not found!";
		return msg;
	}

	public static String viewResults(String limit, String offset, ResultSet rs) throws SQLException
	{
		try
		{
			int intLimit = 0;
			int intOffset = 0;

			if (!limit.equals(""))
			{
				intLimit = Integer.parseInt(limit);
				intOffset = Integer.parseInt(offset);
			}

			String nextOffset = String.valueOf(intOffset + intLimit);
			String previousOffset = String.valueOf(intOffset - intLimit);

			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();

			int counter = 0;

			String msg = "";
			while (rs.next())
			{
				counter++;
				for (int i=1; i<numberOfColumns+1; i++)
				{
					if (i==1)
					{
						msg += "<tr>";
					}
					msg += "<td>" + rs.getString(i) + "</td>\n"; 
				}
				msg += "</tr>\n";
			}
			msg += "</table>\n";

			if (intOffset > 0)
			{
				msg += "<button onclick=\"formSubmit(" + previousOffset + ")\">Previous</button>\n";
			}

			if (counter >= intLimit && !limit.equals(""))
			{
				msg += "<button onclick=\"formSubmit(" + nextOffset + ")\">Next</button>\n";

			}
			msg += "</form>\n";
			return msg;
		}
		catch (SQLException ex)
		{
			throw new SQLException(ex.getMessage());
		}
	}

	public static String setHTMLField(String val) 
	{

		if (val != null)
			val = "\"" + val + "\"";
		else
			val = "\"\"";

		return val;
	}
}
