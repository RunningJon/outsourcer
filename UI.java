import java.util.Map;
import fi.iki.elonen.*;
import java.sql.*;

public class UI extends NanoHTTPD 
{
	public static String gpServer = "localhost";
	public static int gpPort = 5432;
	public static String gpDatabase = "gpdb";
	public static String gpUserName = "gpadmin";
	public static int webPort = 8080;
	public static String authServer = "mdw";
	
	public static void main(String[] args) 
	{
		for (int i = 0; i < args.length; ++i)
		{
			if (i == 0)
				gpServer = args[0];
			if (i == 1)
				gpPort = Integer.parseInt(args[1]);
			if (i == 2)
				gpDatabase = args[2];
			if (i == 3)
				gpUserName = args[3];
			if (i == 4)
				webPort = Integer.parseInt(args[4]);
			if (i == 5)
				authServer = args[5];
		}
		ServerRunnerUI.run(UI.class);
	}

	public UI()
	{
		super(webPort);
	}

	@Override
	public Response serve(String uri, Method method, Map<String, String> header, Map<String, String> parms, Map<String, String> files) 
	{

		String username = parms.get("username");
		String password = parms.get("password");
		String submit = parms.get("submit_form");
		String action_type = parms.get("action_type");
		String loginMessage = "";
		boolean alive = false;

		String msg = "";
		boolean auth = false;

		if (submit == null || submit.equals(""))
			submit = "0";

		if (username == null)
			username = "";

		if (password == null)
			password = "";

		if (action_type == null)
			action_type = "";

		String sessionID = UIModel.getSessionID(header);

		if (sessionID == null)
			sessionID = "0";

		if (action_type.equals("logout"))
		{
			sessionID = "0";
		}

		if (submit.equals("1") && (!username.equals("")) && (!password.equals(""))) 
		{
			try
			{
				auth = UIModel.authenticate(username, password);
			}
			catch (SQLException ex)
			{
				loginMessage = ex.getMessage();
			}
			if (auth)
			{
				sessionID = UIModel.setSessionID(header);
				try
				{
					UIModel.insertSession(sessionID);
				}
				catch (SQLException ex)
				{
					loginMessage = ex.getMessage();
				}
			}
			else
				if (loginMessage.equals(""))
					loginMessage = "Failed to login.  Must use a valid Greenplum database account that is a SuperUser.";
		}

		if (!(sessionID.equals("0")))
		{
			try
			{
				alive = UIModel.keepAlive(sessionID);
			}
			catch (SQLException ex)
			{
				loginMessage = ex.getMessage();
			}
		}
		if (alive)
			msg = OutsourcerControl.buildPage(uri, parms);
		else
			sessionID = "0";

		if (sessionID.equals("0"))
			msg = UIView.viewLogin(loginMessage);

		NanoHTTPD.Response out = new NanoHTTPD.Response(msg);
		out.addHeader("Set-Cookie", sessionID);
		UIModel.logger(uri, sessionID);
		return out;
	}
}
