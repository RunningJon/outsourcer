import java.util.Map;
import java.sql.*;

public class QueueControl
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
		String queueID = parms.get("queueID");
		String id = parms.get("id");

		if (search == null)
			search = "";

		if (limit == null)
			limit = "10";

		if (offset == null)
			offset = "0";

		if (sort == null || sort.equals(""))
			sort = "asc";

		if (sortBy == null || sortBy.equals(""))
			sortBy = "status";

		if (queueID == null)
			queueID = "";

		if (id == null)
			id = "";

		if (actionType == null || actionType.equals(""))
			actionType = "view";

		String msg = "";

		if (actionType.equals("view"))
		{
			try
			{
				rs = QueueModel.getList(search, limit, offset, sortBy, sort);
				msg = QueueView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg += ex.getMessage();
			}

		} 
		else if (actionType.equals("insert"))
		{
			try
			{
				QueueModel.insertTable(id); 
				rs = QueueModel.getList(search, limit, offset, sortBy, sort);
				msg = QueueView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		} 
		else if (actionType.equals("insert_all"))
		{
			try
			{
				QueueModel.insertTableAll(); 
				rs = QueueModel.getList(search, limit, offset, sortBy, sort);
				msg = QueueView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		} 
		else if (actionType.equals("update"))
		{
			try
			{
				QueueModel.updateTable(queueID);
				rs = QueueModel.getList(search, limit, offset, sortBy, sort);
				msg = QueueView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		} 
		else if (actionType.equals("delete"))
		{
			try
			{
				QueueModel.deleteTable(queueID);
				rs = QueueModel.getList(search, limit, offset, sortBy, sort);
				msg = QueueView.viewList(search, rs, limit, offset, sortBy, sort);
			}
			catch (Exception ex)
			{
				msg = ex.getMessage();
			}
		}
		return msg;
	}
}
