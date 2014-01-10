import java.util.Map;
import java.sql.*;

public class EnvironmentControl
{
	public static String buildPage(Map<String, String> parms)
	{
		ResultSet rs = null;
		String search = parms.get("search");
		String limit = parms.get("limit");
		String offset = parms.get("offset");
		String actionType = parms.get("action_type");
		String sortBy = parms.get("sort_by");
		String sort = parms.get("sort");
		String maxJobs = parms.get("max_jobs");
		String oFetchSize = parms.get("oFetchSize");
		String submit = parms.get("submit_form");

		if (search == null)
			search = "";

		if (limit == null)
			limit = "10";

		if (offset == null)
			offset = "0";

		if (actionType == null || actionType.equals(""))
			actionType = "view";

		if (sortBy == null || sortBy.equals(""))
			sortBy = "name";

		if (sort == null || sort.equals(""))
			sort = "asc";

		if (submit == null || submit.equals(""))
			submit = "0";

		if (maxJobs == null || maxJobs.equals(""))
			maxJobs = "";

		if (oFetchSize == null || oFetchSize.equals(""))
			oFetchSize = "";

		String msg = "";

		if (actionType.equals("view"))
		{
			try
			{
				rs = EnvironmentModel.getList(search, limit, offset, sortBy, sort);
				msg = EnvironmentView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		} 
		else if (actionType.equals("Daemon"))
		{
			String status = "";
			try 
			{
				status = EnvironmentModel.getStatus();	
				if (submit.equals("0"))
				{
					msg = EnvironmentView.viewStartStop(status);
				}
				else
				{
					EnvironmentModel.setStatus(status);
					rs = EnvironmentModel.getList(search, limit, offset, sortBy, sort);
					msg = EnvironmentView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		}
		else if (actionType.equals("Agent"))
		{
			String status = "";
			try 
			{
				status = EnvironmentModel.getAgentStatus();	
				if (submit.equals("0"))
				{
					msg = EnvironmentView.viewAgentStartStop(status);
				}
				else
				{
					EnvironmentModel.setAgentStatus(status);
					rs = EnvironmentModel.getList(search, limit, offset, sortBy, sort);
					msg = EnvironmentView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		}
		else if (actionType.equals("max_jobs"))
		{
			try 
			{
				if (submit.equals("0"))
				{
					maxJobs = EnvironmentModel.getVariable("max_jobs");
					msg = EnvironmentView.viewMaxJobs(maxJobs);
				}
				else
				{
					EnvironmentModel.setVariable("max_jobs", maxJobs);
					rs = EnvironmentModel.getList(search, limit, offset, sortBy, sort);
					msg = EnvironmentView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		}
		else if (actionType.equals("oFetchSize"))
		{
			try 
			{
				if (submit.equals("0"))
				{
					oFetchSize = EnvironmentModel.getVariable("oFetchSize");
					msg = EnvironmentView.viewFetchSize(oFetchSize);
				}
				else
				{
					EnvironmentModel.setVariable("oFetchSize", oFetchSize);
					rs = EnvironmentModel.getList(search, limit, offset, sortBy, sort);
					msg = EnvironmentView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		}
		return msg;
	}
}
