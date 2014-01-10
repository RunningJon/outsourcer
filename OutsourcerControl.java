import java.util.Map;

public class OutsourcerControl
{
	public static String buildPage(String uri, Map<String, String> parms)
	{
		String msg = "";

		if (uri.equals("/"))
		//Build navigation page
		{
			msg += OutsourcerView.viewMain();
		} 
		else if (uri.equals("/queue"))
		{
			msg += QueueControl.buildPage(parms);
		} 
		else if (uri.equals("/jobs"))
		{
			msg += JobControl.buildPage(parms);
		} 
		else if (uri.equals("/environment"))
		{
			msg += EnvironmentControl.buildPage(parms);
		} 
		else if (uri.equals("/external"))
		{
			msg += ExternalTableControl.buildPage(parms);
		} 
		else if (uri.equals("/schedule"))
		{
			msg += ScheduleControl.buildPage(parms);
		} 
		else 
		{
			msg += OutsourcerView.viewPageNotFound();
		}
		msg += OutsourcerView.viewFooter();

		return msg;

	}
}
