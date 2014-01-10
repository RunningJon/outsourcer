import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class ScheduleControl
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
		String description = parms.get("description");
		String intervalTrunc = parms.get("interval_trunc");
		String intervalQuantity = parms.get("interval_quantity");
		String submit = parms.get("submit_form");
		String schema = parms.get("schema");
		ArrayList<String> schemaList = new ArrayList<String>();

		if (search == null)
			search = "";

		if (limit == null)
			limit = "10";

		if (offset == null)
			offset = "0";

		if (sort == null || sort.equals(""))
			sort = "asc";

		if (sortBy == null || sortBy.equals(""))
			sortBy = "description";

		if (description == null)
			description = "";

		if (intervalTrunc == null)
			intervalTrunc = "";

		if (intervalQuantity == null)
			intervalQuantity = "";

		if (actionType == null || actionType.equals(""))
			actionType = "view";

		if (submit == null || submit.equals(""))
			submit = "0";

		if (schema == null)
			schema = "";

		String msg = "";

		if (actionType.equals("view"))
		{
			try
			{
				rs = ScheduleModel.getList(search, limit, offset, sortBy, sort);
				msg = ScheduleView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}
		} 
		else if (actionType.equals("update") || actionType.equals("insert"))
		{
			if (submit.equals("0"))
			{
				ScheduleModel e = ScheduleModel.getModel(description);
				msg = ScheduleView.viewUpdate(e.description, e.intervalTrunc, e.intervalQuantity);
			}
			else
			{
				try
				{
					if (actionType.equals("insert"))
						ScheduleModel.insertTable(description, intervalTrunc, intervalQuantity);
					else
						ScheduleModel.updateTable(description, intervalTrunc, intervalQuantity);
					rs = ScheduleModel.getList(search, limit, offset, sortBy, sort);
					msg = ScheduleView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
		} 
		else if (actionType.equals("delete"))
		{	
			if (submit.equals("0"))
			{
				ScheduleModel e = ScheduleModel.getModel(description);
				msg = ScheduleView.viewDelete(e.description, e.intervalTrunc, e.intervalQuantity);
			}
			else
			{
				try
				{
					ScheduleModel.deleteTable(description);
					rs = ScheduleModel.getList(search, limit, offset, sortBy, sort);
					msg = ScheduleView.viewList(search, rs, limit, offset, sortBy, sort);
				}
				catch (Exception ex)
				{
					msg = ex.getMessage();
				}
			}
		}
		else if (actionType.equals("create"))
		{	
			try
			{
				if (submit.equals("0"))
				{
					schemaList = JobModel.getSchemas();
					msg = ScheduleView.viewCreate(description, schemaList);
				}
				else
				{
					JobModel.updateJobsSchedule(description, schema);
					rs = ScheduleModel.getList(search, limit, offset, sortBy, sort);
					msg = ScheduleView.viewList(search, rs, limit, offset, sortBy, sort);
				}
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		}
		return msg;
	}
}
