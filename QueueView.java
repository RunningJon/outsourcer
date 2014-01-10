import java.sql.ResultSet;

public class QueueView
{
	public static String action = "queue";

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
		myScript += "function updateQueue(id, myAction)\n";
		myScript += "{\n";
		myScript += "   document.myForm.action = 'queue';\n";
		myScript += "   document.getElementById(\"action_type\").value = myAction;\n";
		myScript += "   document.getElementById(\"offset\").value = 0;\n";
		myScript += "   document.getElementById(\"queueID\").value = id;\n";
		myScript += "   document.getElementById(\"id\").value = id;\n";
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
			msg = msg + OutsourcerView.viewResults(limit, offset, rs);
		}
		catch (Exception localException)
		{
			msg = msg + localException.getMessage();
		}
		return msg;
	}

	private static String getHeader(String sortBy, String sort)
	{

		String downArrow = "&#8595;";
		String upArrow = "&#8593;";
		String defaultSort = "asc";

		String queueIDHeader = "<th><b>Queue</b><button ";
		String queueIDFocus = "";
		String queueIDArrow = downArrow;
		String queueIDSort = defaultSort;

		String statusHeader = "<th><b>Status</b><button ";
		String statusFocus = "";
		String statusArrow = downArrow;
		String statusSort = defaultSort;

		String queueDateHeader = "<th><b>Queue</b><button ";
		String queueDateFocus = "";
		String queueDateArrow = downArrow;
		String queueDateSort = defaultSort;

		String startDateHeader = "<th><b>Start</b><button ";
		String startDateFocus = "";
		String startDateArrow = downArrow;
		String startDateSort = defaultSort;

		String endDateHeader = "<th><b>End</b><button ";
		String endDateFocus = "";
		String endDateArrow = downArrow;
		String endDateSort = defaultSort;

		String durationHeader = "<th><b>Duration</b><button ";
		String durationFocus = "";
		String durationArrow = downArrow;
		String durationSort = defaultSort;

		String rowsHeader = "<th><b>Rows</b><button ";
		String rowsFocus = "";
		String rowsArrow = downArrow;
		String rowsSort = defaultSort;

		String jobIDHeader = "<th><b>Job</b><button ";
		String jobIDFocus = "";
		String jobIDArrow = downArrow;
		String jobIDSort = defaultSort;

		String targetHeader = "<th><b>Target</b><button ";
		String targetFocus = "";
		String targetArrow = downArrow;
		String targetSort = defaultSort;

		String errorMsgHeader = "<th><b>Error</b><button ";
		String errorMsgFocus = "";
		String errorMsgArrow = downArrow;
		String errorMsgSort = defaultSort;

		if (sortBy.equals("queue_id"))
		{
			queueIDFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				queueIDSort = "desc";
			else
			{
				queueIDSort = "asc";
				queueIDArrow = upArrow;
			}
		}
		else if (sortBy.equals("status"))
		{
			statusFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				statusSort = "desc";
			else
			{
				statusSort = "asc";
				statusArrow = upArrow;
			}
		}
		else if (sortBy.equals("queue_date"))
		{
			queueDateFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				queueDateSort = "desc";
			else
			{
				queueDateSort = "asc";
				queueDateArrow = upArrow;
			}
		}
		else if (sortBy.equals("start_date"))
		{
			startDateFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				startDateSort = "desc";
			else
			{
				startDateSort = "asc";
				startDateArrow = upArrow;
			}
		}
		else if (sortBy.equals("end_date"))
		{
			endDateFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				endDateSort = "desc";
			else
			{
				endDateSort = "asc";
				endDateArrow = upArrow;
			}
		}
		else if (sortBy.equals("duration"))
		{
			durationFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				durationSort = "desc";
			else
			{
				durationSort = "asc";
				durationArrow = upArrow;
			}
		}
		else if (sortBy.equals("num_rows"))
		{
			rowsFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				rowsSort = "desc";
			else
			{
				rowsSort = "asc";
				rowsArrow = upArrow;
			}
		}
		else if (sortBy.equals("id"))
		{
			jobIDFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				jobIDSort = "desc";
			else
			{
				jobIDSort = "asc";
				jobIDArrow = upArrow;
			}
		}
		else if (sortBy.equals("target_table_name"))
		{
			targetFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				targetSort = "desc";
			else
			{
				targetSort = "asc";
				targetArrow = upArrow;
			}
		}
		else if (sortBy.equals("error_message"))
		{
			errorMsgFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				errorMsgSort = "desc";
			else
			{
				errorMsgSort = "asc";
				errorMsgArrow = upArrow;
			}
		}

		queueIDHeader += queueIDFocus + "onclick=\"sortRS('queue_id', '" + queueIDSort + "')\">" + queueIDArrow + "</button></th>\n";
		statusHeader += statusFocus + "onclick=\"sortRS('status', '" + statusSort + "')\">" + statusArrow + "</button></th>\n";
		queueDateHeader += queueDateFocus + "onclick=\"sortRS('queue_date', '" + queueDateSort + "')\">" + queueDateArrow + "</button></th>\n";
		startDateHeader += startDateFocus + "onclick=\"sortRS('start_date', '" + startDateSort + "')\">" + startDateArrow + "</button></th>\n";
		endDateHeader += endDateFocus + "onclick=\"sortRS('end_date', '" + endDateSort + "')\">" + endDateArrow + "</button></th>\n";
		durationHeader += durationFocus + "onclick=\"sortRS('duration', '" + durationSort + "')\">" + durationArrow + "</button></th>\n";
		rowsHeader += rowsFocus + "onclick=\"sortRS('num_rows', '" + rowsSort + "')\">" + rowsArrow + "</button></th>\n";
		jobIDHeader += jobIDFocus + "onclick=\"sortRS('id', '" + jobIDSort + "')\">" + jobIDArrow + "</button></th>\n";
		targetHeader += targetFocus + "onclick=\"sortRS('target_table_name', '" + targetSort + "')\">" + targetArrow + "</button></th>\n";
		errorMsgHeader += errorMsgFocus + "onclick=\"sortRS('error_message', '" + errorMsgSort + "')\">" + errorMsgArrow + "</button></th>\n";
		
		String msg = "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += queueIDHeader + statusHeader + queueDateHeader + startDateHeader + endDateHeader + durationHeader + rowsHeader + jobIDHeader + targetHeader + errorMsgHeader;
		msg += "</tr>\n";

		return msg;
	}
}
