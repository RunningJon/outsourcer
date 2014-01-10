public class UIView
{
	public static String viewLogin(String loginMessage)
	{

		String myScript = getJavaScriptFunctions();
		String onLoad = "login()";
		String msg = OutsourcerView.getHead(myScript, onLoad);

		if (!(loginMessage==""))
		{
			String[] errorMessage = loginMessage.split(":");
			int i = errorMessage.length;
//			loginMessage += ":length:" + i;
			loginMessage = errorMessage[errorMessage.length-1];
			loginMessage = loginMessage.substring(0, loginMessage.length() - 1);
		}
		msg += "<table class=\"ostable\">\n";
		msg += "<tr><td><h1><a href=\"/\">Outsourcer</a></h1></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "<form action=\"/\" name=\"login\" id=\"login\" method=\"post\">\n";
		msg += "<input type=\"hidden\" id=\"submit_form\" name=\"submit_form\" value=\"1\">\n";
		msg += "<table class=\"logintable\" width=\"100%\">\n";
		msg += "<tr><td align=\"center\">\n";
		msg += "<table class=\"logintable\">\n";
		msg += "<tr>\n";
		msg += "<td>Username</td>\n";
		msg += "<td><input type=\"text\" id=\"username\" name=\"username\"></td>\n";
		msg += "</tr>\n";
		msg += "<tr>\n";
		msg += "<td>Password</td>\n";
		msg += "<td><input type=\"password\" id=\"password\" name=\"password\"></td>\n";
		msg += "</tr>\n";
		msg += "<tr>\n";
		msg += "<td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Login\"></td>\n";
		msg += "</tr>\n";
		msg += "</table>\n";
		msg += "</td></tr>\n";
		if (!(loginMessage == ""))
			msg += "<tr><th>" + loginMessage + "</th></tr>\n";
		msg += "</table>\n";
		msg += "</form>\n";
		msg += "</body></html>\n";
		return msg;
	}

	private static String getJavaScriptFunctions()
	{
		String myScript = "function login()\n";
		myScript += "{\n";
		myScript += "	document.getElementById(\"username\").focus()\n";
		myScript += "}\n";

		return myScript;
	}
}
