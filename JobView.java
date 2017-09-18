import java.util.Map;
import java.sql.*;
import java.util.ArrayList;

public class JobView
{
	public static String action = "jobs";

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
		myScript += "   document.getElementById(\"action_type\").value = \"queue\";\n";
		myScript += "   document.getElementById(\"queue_action\").value = myAction;\n";
		myScript += "   document.getElementById(\"id\").value = id;\n";
		myScript += "   document.getElementById(\"myForm\").submit();\n";
		myScript += "}\n";
		myScript += "function updateJob(id, myAction)\n";
		myScript += "{\n";
		myScript += "   document.getElementById(\"action_type\").value = myAction;\n";
		myScript += "   document.getElementById(\"offset\").value = 0;\n";
		myScript += "   document.getElementById(\"submit_form\").value = \"0\";\n";
		myScript += "   document.getElementById(\"id\").value = id;\n";
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
		msg += "<td><h4><a href=\"?action_type=update\">Define New Job</a></h4></td>\n";
		msg += "<td align=\"center\"><h4><a href=\"queue?action_type=insert_all\">Queue All Jobs</a></h4></td>\n";
		msg += "<td align=\"right\"><h4><a href=\"?action_type=delete_all\">Delete All Jobs</a></h4></td>\n";
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
		String myScript = "function disableInputFields()\n";
		myScript += "{\n";
		myScript += "	var ver = \"" + UI.gpVersion + "\";\n";
		myScript += "	var val = document.getElementById(\"source_type\").selectedIndex;\n";
		myScript += "	var t = document.getElementById(\"refresh_type\").selectedIndex;\n";
		myScript += "	var a = document.getElementById(\"target_append_only\").selectedIndex;\n";
		myScript += "	if (t == 0)\n";  //refresh_type not set yet.
		myScript += "	{\n";
		myScript += "		document.getElementById(\"target_schema_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"target_table_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"target_append_only\").value = \"true\";\n";
		myScript += "		document.getElementById(\"target_compressed\").value = \"false\";\n";
		myScript += "		document.getElementById(\"target_row_orientation\").value = \"true\";\n";
		myScript += "		document.getElementById(\"source_type\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_server_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_instance_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_port\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_database_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_schema_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_table_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_user_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_pass\").value = \"\";\n";
		myScript += "		document.getElementById(\"column_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"sql_text\").value = \"\";\n";
		myScript += "		document.getElementById(\"snapshot\").value = \"false\";\n";
		myScript += "		document.getElementById(\"schedule_desc\").value = \"\";\n";
		myScript += "		document.getElementById(\"r_target_schema_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_table_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_append_only\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_compressed\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_row_orientation\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_type\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_server_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_instance_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_port\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_database_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_schema_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_table_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_user_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_pass\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_column_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_sql_text\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_snapshot\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_schedule_desc\").style.display = \"none\";\n";
		myScript += "	} else \n";
		myScript += "	if (t != 4) // Not a transform and a value has been picked\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"r_target_schema_name\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_target_table_name\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_source_type\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"r_schedule_desc\").style.display = \"\";\n";
		myScript += "		if (t == 1) //Append\n";
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_column_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_snapshot\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"snapshot\").value = \"false\";\n";
		myScript += "			document.getElementById(\"r_sql_text\").style.display = \"\";\n";
		myScript += "			if (ver == \"AO\")\n";
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_append_only\").style.display = \"\";\n";  
		myScript += "			} else\n";
		myScript += "			if (ver == \"HAWQ\")\n";  
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_append_only\").style.display = \"none\";\n";  
		myScript += "			}\n";
		myScript += "			if (a == 0) //append-optimized\n";
		myScript += "				{\n";
		myScript += "					document.getElementById(\"r_target_compressed\").style.display = \"\";\n";
		myScript += "					document.getElementById(\"r_target_row_orientation\").style.display = \"\";\n";
		myScript += "				} else\n";
		myScript += "				{\n";
		myScript += "					document.getElementById(\"r_target_compressed\").style.display = \"none\";\n";
		myScript += "					document.getElementById(\"r_target_row_orientation\").style.display = \"none\";\n";
		myScript += "				}\n";
		myScript += "		} else \n";
		myScript += "		if (t == 2 || t == 3) //DDL and Refresh\n";
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_column_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_snapshot\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"column_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"r_sql_text\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"snapshot\").value = \"false\";\n";
		myScript += "			if (ver == \"AO\")\n";
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_append_only\").style.display = \"\";\n";  
		myScript += "			} else\n";
		myScript += "			if (ver == \"HAWQ\")\n";  
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_append_only\").style.display = \"none\";\n";  
		myScript += "			}\n";
		myScript += "			if (a == 0) //append-optimized\n";
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_compressed\").style.display = \"\";\n";
		myScript += "				document.getElementById(\"r_target_row_orientation\").style.display = \"\";\n";
		myScript += "			} else\n";
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_compressed\").style.display = \"none\";\n";
		myScript += "				document.getElementById(\"r_target_row_orientation\").style.display = \"none\";\n";
		myScript += "			}\n";
		myScript += "		} else \n";
		myScript += "		if (t == 5) //Replication\n";
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_column_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_snapshot\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_sql_text\").style.display = \"\";\n";
		myScript += "			if (ver == \"AO\")\n";  
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_append_only\").style.display = \"\";\n";  
		myScript += "				if (a == 0) //append-optimized\n";
		myScript += "				{\n";
		myScript += "					document.getElementById(\"r_target_compressed\").style.display = \"\";\n";
		myScript += "					document.getElementById(\"r_target_row_orientation\").style.display = \"\";\n";
		myScript += "				} else\n";
		myScript += "				{\n";
		myScript += "					document.getElementById(\"r_target_compressed\").style.display = \"none\";\n";
		myScript += "					document.getElementById(\"r_target_row_orientation\").style.display = \"none\";\n";
		myScript += "				}\n";
		myScript += "			} else\n";
		myScript += "			if (ver == \"HAWQ\")\n";  
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_target_append_only\").style.display = \"none\";\n";  
		myScript += "				document.getElementById(\"r_target_compressed\").style.display = \"\";\n";
		myScript += "				document.getElementById(\"r_target_row_orientation\").style.display = \"\";\n";
		myScript += "			}\n";
		myScript += "		}\n";
		myScript += "		if (val == 0)\n"; // source type not set yet
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_source_server_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_instance_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_port\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_database_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_schema_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_table_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_user_name\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"r_source_pass\").style.display = \"none\";\n";
		myScript += "			document.getElementById(\"source_server_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_instance_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_port\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_database_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_schema_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_table_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_user_name\").value = \"\";\n";
		myScript += "			document.getElementById(\"source_pass\").value = \"\";\n";
		myScript += "		} else\n";  //Oracle or SQL Server picked
		myScript += "		{\n";
		myScript += "			document.getElementById(\"r_source_server_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_source_database_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_source_schema_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_source_table_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_source_user_name\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_source_pass\").style.display = \"\";\n";
		myScript += "			document.getElementById(\"r_schedule_desc\").style.display = \"\";\n";
		myScript += "			if (val == 1) //Oracle\n";
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_source_instance_name\").style.display = \"none\";\n";
		myScript += "				document.getElementById(\"r_source_port\").style.display = \"\";\n";
		myScript += "				document.getElementById(\"r_snapshot\").style.display = \"none\";\n";
		myScript += "				document.getElementById(\"source_instance_name\").value = \"\";\n";
		myScript += "			} else\n";
		myScript += "			if (val == 2) //SQL Server\n";
		myScript += "			{\n";
		myScript += "				document.getElementById(\"r_source_instance_name\").style.display = \"\";\n";
		myScript += "				document.getElementById(\"r_source_port\").style.display = \"none\";\n";
		myScript += "				document.getElementById(\"r_snapshot\").style.display = \"none\";\n";
		myScript += "				document.getElementById(\"source_port\").value = \"\";\n";
		myScript += "			}\n";
		myScript += "		}\n";
		myScript += "	} else //transform job\n";
		myScript += "	{\n";
		myScript += "		document.getElementById(\"r_sql_text\").style.display = \"\";\n";
		myScript += "		document.getElementById(\"target_schema_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"target_table_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"target_append_only\").value = \"true\";\n";
		myScript += "		document.getElementById(\"target_compressed\").value = \"false\";\n";
		myScript += "		document.getElementById(\"target_row_orientation\").value = \"true\";\n";
		myScript += "		document.getElementById(\"source_type\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_server_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_instance_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_port\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_database_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_schema_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_table_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_user_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"source_pass\").value = \"\";\n";
		myScript += "		document.getElementById(\"column_name\").value = \"\";\n";
		myScript += "		document.getElementById(\"snapshot\").value = \"false\";\n";
		myScript += "		document.getElementById(\"schedule_desc\").value = \"\";\n";
		myScript += "		document.getElementById(\"r_target_schema_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_table_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_append_only\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_compressed\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_target_row_orientation\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_type\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_server_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_instance_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_port\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_database_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_schema_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_table_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_user_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_source_pass\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_column_name\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_snapshot\").style.display = \"none\";\n";
		myScript += "		document.getElementById(\"r_schedule_desc\").style.display = \"\";\n";
		myScript += "	}\n";
		myScript += "}\n";

		return myScript;

	}

	public static String viewUpdate(String id, String refreshType, String targetSchemaName, String targetTableName, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceSchemaName, String sourceTableName, String sourceUserName, String sourcePass, String columnName, String sqlText, boolean snapshot, String scheduleDesc, ArrayList<String> scheduleList)
	{

		String buttonText = "";
		if (id == null)
		{
			id = "";
			buttonText = "Insert";
			targetAppendOnly = true;
			targetCompressed = false;
			targetRowOrientation = true;
			snapshot = false;
		}
		else
		{
			buttonText = "Update";
		}

		targetSchemaName = OutsourcerView.setHTMLField(targetSchemaName);
		targetTableName = OutsourcerView.setHTMLField(targetTableName);
		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceSchemaName = OutsourcerView.setHTMLField(sourceSchemaName);
		sourceTableName = OutsourcerView.setHTMLField(sourceTableName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);
		columnName = OutsourcerView.setHTMLField(columnName);
		if (sqlText == null)
			sqlText = "";
		
		String myScript = getJavaScriptFunctions();
		String onLoad="disableInputFields()";

		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"jobs\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr><td><b>Refresh Type</b></td>";
		msg += "<td><select id=\"refresh_type\" name=\"refresh_type\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"append\"";
		if (refreshType != null && refreshType.equals("append"))
			msg += " selected";
		msg += ">Append</option>\n";
		msg += "<option value=\"ddl\""; 
		if (refreshType != null && refreshType.equals("ddl"))
			msg += " selected";
		msg += ">DDL</option>\n";
		msg += "<option value=\"refresh\"";
		if (refreshType != null && refreshType.equals("refresh"))
			msg += " selected";
		msg += ">Refresh</option>\n";
		msg += "<option value=\"transform\"";
		if (refreshType != null && refreshType.equals("transform"))
			msg += " selected";
		msg += ">Transform</option>\n";
		if (!(UI.gpVersion.equals("HAWQ")))
		{
			msg += "<option value=\"replication\"";
			if (refreshType != null && refreshType.equals("replication"))
				msg += " selected";
			msg += ">Replication</option>\n";
			msg += "</select>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_target_schema_name\"><td><b>Target Schema</b></td>\n";
		msg += "<td><input type=\"text\" id=\"target_schema_name\" name=\"target_schema_name\" value=" + targetSchemaName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_target_table_name\"><td><b>Target Table</b></td>\n";
		msg += "<td><input type=\"text\" id=\"target_table_name\" name=\"target_table_name\" value=" + targetTableName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_target_append_only\"><td><b>";
		if (UI.gpVersion.equals("AO"))
			msg += "Target Append-Optimized";
		else
			msg += "Target Append-Only";
		msg += "</b></td>\n";
		msg += "<td><select id=\"target_append_only\" name=\"target_append_only\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"true\"";
		if (targetAppendOnly == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetAppendOnly == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_target_compressed\"><td><b>Target Compressed</b></td>\n";
		msg += "<td><select id=\"target_compressed\" name=\"target_compressed\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"true\"";
		if (targetCompressed == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetCompressed == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_target_row_orientation\"><td><b>Target Row Orientation</b></td>\n";
		msg += "<td><select id=\"target_row_orientation\" name=\"target_row_orientation\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"true\"";
		if (targetRowOrientation == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetRowOrientation == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_column_name\"><td><b>Column Name</b></td>";
		msg += "<td><input type=\"column_name\" id=\"column_name\" name=\"column_name\" value=" + columnName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_snapshot\"><td><b>Snapshot</b></td>\n";
		msg += "<td><select id=\"snapshot\" name=\"snapshot\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"true\"";
		if (snapshot == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (snapshot == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_sql_text\"><td><b>Transform SQL</b></td>";
		msg += "<td><textarea cols=\"50\" rows=\"10\" id=\"sql_text\" name=\"sql_text\">" + sqlText + "</textarea>\n";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_schedule_desc\"><td><b>Schedule</b></td>";
		msg += "<td><select id=\"schedule_desc\" name=\"schedule_desc\">\n";
		msg += "<option value=\"\"></option>\n";
		for (int i = 0; i < scheduleList.size(); i++)
		{
			msg += "<option value=\"" + scheduleList.get(i) + "\"";
			if (scheduleDesc != null && scheduleDesc.equals(scheduleList.get(i)))
				msg += " selected";
			msg += ">" + scheduleList.get(i) + "</option>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onchange=\"disableInputFields()\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=" + sourceInstanceName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Source Port</b></td>";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" onkeyup=\"this.value=this.value.replace(/[^\\d]/,'')\" value=" + sourcePort + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=" + sourceDatabaseName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_schema_name\"><td><b>Source Schema Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_schema_name\" name=\"source_schema_name\" value=" + sourceSchemaName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_table_name\"><td><b>Source Table Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_table_name\" name=\"source_table_name\" value=" + sourceTableName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=" + sourceUserName + ">";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>";
		msg += "<td><input type=\"password\" id=\"source_pass\" name=\"source_pass\" value=" + sourcePass + ">";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"" + buttonText + "\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"update\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		if (id != null)
			msg += "<input type=\"hidden\" name=\"id\" value=\"" + id + "\">\n";
		msg += "</form>\n";
		return msg;
	}

	public static String viewDelete(String id, String refreshType, String targetSchemaName, String targetTableName, boolean targetAppendOnly, boolean targetCompressed, boolean targetRowOrientation, String sourceType, String sourceServerName, String sourceInstanceName, String sourcePort, String sourceDatabaseName, String sourceSchemaName, String sourceTableName, String sourceUserName, String sourcePass, String columnName, String sqlText, boolean snapshot, String scheduleDesc, ArrayList<String> scheduleList)
	{
		targetSchemaName = OutsourcerView.setHTMLField(targetSchemaName);
		targetTableName = OutsourcerView.setHTMLField(targetTableName);
		sourceServerName = OutsourcerView.setHTMLField(sourceServerName);
		sourceInstanceName = OutsourcerView.setHTMLField(sourceInstanceName);
		sourcePort = OutsourcerView.setHTMLField(sourcePort);
		sourceDatabaseName = OutsourcerView.setHTMLField(sourceDatabaseName);
		sourceSchemaName = OutsourcerView.setHTMLField(sourceSchemaName);
		sourceTableName = OutsourcerView.setHTMLField(sourceTableName);
		sourceUserName = OutsourcerView.setHTMLField(sourceUserName);
		sourcePass = OutsourcerView.setHTMLField(sourcePass);
		columnName = OutsourcerView.setHTMLField(columnName);
		if (sqlText == null)
			sqlText = "";
		String myScript = getJavaScriptFunctions();
		String onLoad="disableInputFields()";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"jobs\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td width=\"30%\"><b>ID</b></td>";
		msg += "<td>" + id + "</td></tr>\n";
		msg += "<tr><td><b>Refresh Type</b></td>";
		msg += "<td><select id=\"refresh_type\" name=\"refresh_type\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"append\"";
		if (refreshType != null && refreshType.equals("append"))
			msg += " selected";
		msg += ">Append</option>\n";
		msg += "<option value=\"ddl\""; 
		if (refreshType != null && refreshType.equals("ddl"))
			msg += " selected";
		msg += ">DDL</option>\n";
		msg += "<option value=\"refresh\"";
		if (refreshType != null && refreshType.equals("refresh"))
			msg += " selected";
		msg += ">Refresh</option>\n";
		msg += "<option value=\"transform\"";
		if (refreshType != null && refreshType.equals("transform"))
			msg += " selected";
		msg += ">Transform</option>\n";
		if (!(UI.gpVersion.equals("HAWQ")))
		{
			msg += "<option value=\"replication\"";
			if (refreshType != null && refreshType.equals("replication"))
				msg += " selected";
			msg += ">Replication</option>\n";
			msg += "</select>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_target_schema_name\"><td><b>Target Schema</b></td>\n";
		msg += "<td><input type=\"text\" id=\"target_schema_name\" name=\"target_schema_name\" value=" + targetSchemaName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_target_table_name\"><td><b>Target Table</b></td>\n";
		msg += "<td><input type=\"text\" id=\"target_table_name\" name=\"target_table_name\" value=" + targetTableName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_target_append_only\"><td><b>";
		if (UI.gpVersion.equals("AO"))
			msg += "Target Append-Optimized";
		else
			msg += "Target Append-Only";
		msg += "</b></td>\n";
		msg += "<td><select id=\"target_append_only\" name=\"target_append_only\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"true\"";
		if (targetAppendOnly == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetAppendOnly == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_target_compressed\"><td><b>Target Compressed</b></td>\n";
		msg += "<td><select id=\"target_compressed\" name=\"target_compressed\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"true\"";
		if (targetCompressed == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetCompressed == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_target_row_orientation\"><td><b>Target Row Orientation</b></td>\n";
		msg += "<td><select id=\"target_row_orientation\" name=\"target_row_orientation\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"true\"";
		if (targetRowOrientation == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (targetRowOrientation == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_column_name\"><td><b>Column Name</b></td>";
		msg += "<td><input type=\"column_name\" id=\"column_name\" name=\"column_name\" value=" + columnName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_snapshot\"><td><b>Snapshot</b></td>\n";
		msg += "<td><select id=\"snapshot\" name=\"snapshot\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"true\"";
		if (snapshot == true)
			msg += " selected";
		msg += ">True</option>\n";
		msg += "<option value=\"false\"";
		if (snapshot == false)
			msg += " selected";
		msg += ">False</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_sql_text\"><td><b>Transform SQL</b></td>";
		msg += "<td><textarea readonly cols=\"50\" rows=\"10\" id=\"sql_text\" name=\"sql_text\">" + sqlText + "</textarea>\n";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_schedule_desc\"><td><b>Schedule</b></td>";
		msg += "<td><select id=\"schedule_desc\" name=\"schedule_desc\"i onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"\"></option>\n";
		for (int i = 0; i < scheduleList.size(); i++)
		{
			msg += "<option value=\"" + scheduleList.get(i) + "\"";
			if (scheduleDesc != null && scheduleDesc.equals(scheduleList.get(i)))
				msg += " selected";
			msg += ">" + scheduleList.get(i) + "</option>\n";
		}
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_type\"><td><b>Source Type</b></td>\n";
		msg += "<td><select id=\"source_type\" name=\"source_type\" onfocus=\"this.defaultIndex=this.selectedIndex;\" onchange=\"this.selectedIndex=this.defaultIndex;\">\n";
		msg += "<option value=\"\"></option>\n";
		msg += "<option value=\"oracle\"";
		if (sourceType != null && sourceType.equals("oracle"))
			msg += " selected";
		msg += ">Oracle</option>\n";
		msg += "<option value=\"sqlserver\"";
		if (sourceType != null && sourceType.equals("sqlserver"))
			msg += " selected";
		msg += ">SQL Server</option>\n";
		msg += "</select></td></tr>\n";
		msg += "<tr id=\"r_source_server_name\"><td><b>Source Server Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_server_name\" name=\"source_server_name\" value=" + sourceServerName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_instance_name\"><td><b>Source Instance Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_instance_name\" name=\"source_instance_name\" value=" + sourceInstanceName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_port\"><td><b>Port</b></td>";
		msg += "<td><input type=\"text\" id=\"source_port\" name=\"source_port\" value=" + sourcePort + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_database_name\"><td><b>Source Database Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_database_name\" name=\"source_database_name\" value=" + sourceDatabaseName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_schema_name\"><td><b>Source Schema Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_schema_name\" name=\"source_schema_name\" value=" + sourceSchemaName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_table_name\"><td><b>Source Table Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_table_name\" name=\"source_table_name\" value=" + sourceTableName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_user_name\"><td><b>Source User Name</b></td>";
		msg += "<td><input type=\"text\" id=\"source_user_name\" name=\"source_user_name\" value=" + sourceUserName + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr id=\"r_source_pass\"><td><b>Source Password</b></td>";
		msg += "<td><input type=\"password\" id=\"source_pass\" name=\"source_pass\" value=" + sourcePass + " readonly>";
		msg += "</td></tr>\n";
		msg += "<tr><td colspan=\"2\" align=\"center\"><input type=\"submit\" value=\"Delete\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"delete\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "<input type=\"hidden\" name=\"id\" value=\"" + id + "\">\n";
		msg += "</form>\n";

		return msg;
	}

	private static String getHeader(String sortBy, String sort)
	{

		String downArrow = "&#8595;";
		String upArrow = "&#8593;";
		String defaultSort = "asc";

		String manageHeader = "<th><b>Manage</b></th>\n";

		String IDHeader = "<th><b>ID</b><button ";
		String IDFocus = "";
		String IDArrow = downArrow;
		String IDSort = defaultSort;

		String refreshTypeHeader = "<th><b>Type</b><button ";
		String refreshTypeFocus = "";
		String refreshTypeArrow = downArrow;
		String refreshTypeSort = defaultSort;

		String sourceHeader = "<th><b>Source</b><button ";
		String sourceFocus = "";
		String sourceArrow = downArrow;
		String sourceSort = defaultSort;

		String targetHeader = "<th><b>Target</b><button ";
		String targetFocus = "";
		String targetArrow = downArrow;
		String targetSort = defaultSort;

		String scheduleHeader = "<th><b>Schedule</b><button ";
		String scheduleFocus = "";
		String scheduleArrow = downArrow;
		String scheduleSort = defaultSort;

		String scheduleNextHeader = "<th><b>Schedule Next</b><button ";
		String scheduleNextFocus = "";
		String scheduleNextArrow = downArrow;
		String scheduleNextSort = defaultSort;

		if (sortBy.equals("id"))
		{
			IDFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				IDSort = "desc";
			else
			{
				IDSort = "asc";
				IDArrow = upArrow;
			}
		}
		else if (sortBy.equals("refresh_type"))
		{
			refreshTypeFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				refreshTypeSort = "desc";
			else
			{
				refreshTypeSort = "asc";
				refreshTypeArrow = upArrow;
			}
		}
		else if (sortBy.equals("source_info"))
		{
			sourceFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				sourceSort = "desc";
			else
			{
				sourceSort = "asc";
				sourceArrow = upArrow;
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
		else if (sortBy.equals("schedule_desc"))
		{
			scheduleFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				scheduleSort = "desc";
			else
			{
				scheduleSort = "asc";
				scheduleArrow = upArrow;
			}
		}
		else if (sortBy.equals("schedule_next"))
		{
			scheduleNextFocus = OutsourcerView.focus;
			if (sort.equals("asc"))
				scheduleNextSort = "desc";
			else
			{
				scheduleNextSort = "asc";
				scheduleNextArrow = upArrow;
			}
		}

		IDHeader += IDFocus + "onclick=\"sortRS('id', '" + IDSort + "')\">" + IDArrow + "</button></th>\n";
		refreshTypeHeader += refreshTypeFocus + "onclick=\"sortRS('refresh_type', '" + refreshTypeSort + "')\">" + refreshTypeArrow + "</button></th>\n";
		sourceHeader += sourceFocus + "onclick=\"sortRS('source_info', '" + sourceSort + "')\">" + sourceArrow + "</button></th>\n";
		targetHeader += targetFocus + "onclick=\"sortRS('target_table_name', '" + targetSort + "')\">" + targetArrow + "</button></th>\n";
		scheduleHeader += scheduleFocus + "onclick=\"sortRS('schedule_desc', '" + scheduleSort + "')\">" + scheduleArrow + "</button></th>\n";
		scheduleNextHeader += scheduleNextFocus + "onclick=\"sortRS('schedule_next', '" + scheduleNextSort + "')\">" + scheduleNextArrow + "</button></th>\n";
		
		String msg = "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr>\n";
		msg += manageHeader + IDHeader + refreshTypeHeader + sourceHeader + targetHeader + scheduleHeader + scheduleNextHeader;
		msg += "</tr>\n";

		return msg;
	}

	public static String viewDelete()
	{
		String myScript = getJavaScriptFunctions();
		String onLoad="";
		String msg = OutsourcerView.viewHeader(myScript, onLoad, action);
		msg += "<form action=\"jobs\" method=\"post\">\n";
		msg += "<table class=\"tftable\" border=\"1\">\n";
		msg += "<tr><td><b>Delete all jobs?</b></td>";
		msg += "<td><input type=\"submit\" value=\"Delete\"></td></tr>\n";
		msg += "</table>\n";
		msg += "<input type=\"hidden\" name=\"action_type\" value=\"delete_all\">\n";
		msg += "<input type=\"hidden\" name=\"submit_form\" value=\"1\">\n";
		msg += "</form>\n";
		return msg;
	}
}
