package com.bt.dataintegration.oozie.workflow.main;

import java.util.LinkedList;

import com.bt.dataintegration.oozie.workflow.tags.ActionShell;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.oozie.workflow.tags.ShellTag;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.constants.*;

public class ShellMain {

	private ShellTag sTag = new ShellTag();
	private ActionShell aShell = new ActionShell();
	private ActionShell shellInit = new ActionShell();
	private ActionShell shellAudit = new ActionShell();
	private ActionShell shellHousekeeping = new ActionShell();
	private ActionShell shellErr = new ActionShell();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	public String tableName = null;

	// private LinkedList<String> argument = new LinkedList<String>();

	public ShellMain() {
		HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties();
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getTableName();
		} else if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getHiveTableName();
		}
	}

	public ActionShell setShellMainInit(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(Constants.SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${shell_file_init}");

		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${shell_file_path}");
			argument.add("${milestone_everytime}");
			argument.add("${targetDirectory}");
			okt.setOkt("SQOOP_IMPORT_FROM_" + tableName);
			//ert.setErt("EMAIL_FAILURE");
			ert.setErt("CAPTURE_ERROR_LOGS_" + tableName);
			shellInit.setName("REFRESH_LAST_MODIFIED_DATE_VALUE");
		}
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${targetDirectory}");
			okt.setOkt("FILESYSTEM_VALIDATIONS");
			//ert.setErt("EMAIL_FAILURE");
			ert.setErt("CAPTURE_ERROR_LOGS_" + tableName);
			shellInit.setName("CAPTURE_DATE_AND_CREATEDIR");
		}

		sTag.setArgument(argument);
		sTag.setSshFile("${shell_file_init}#${shell_file_init}");
		sTag.setCaptureOutput("");		
		
		shellInit.setsTag(sTag);
		shellInit.setOkt(okt);
		shellInit.setErt(ert);

		return shellInit;
	}

	public ActionShell setShellMain(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(Constants.SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${shell_file}");
		
		argument.add("${shell_file_path}");
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirYear']}");
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMonth']}");
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirDate']}");
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirHour']}");
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMinute']}");
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['dateNow']}");
		argument.add("${targetDirectory}");
		
		sTag.setArgument(argument);
		sTag.setSshFile("${shell_file}#${shell_file}");
		sTag.setCaptureOutput("");

		//okt.setOkt("COMPRESS_AVRO_DATA_FOR_" + hconf.getTableName());
		okt.setOkt("HIVE_CREATE_TABLE_" + tableName);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt("CAPTURE_ERROR_LOGS_" + tableName);

		aShell.setName("REFRESH_LAST_MODIFIED_DATE_VALUE_FILE");
		aShell.setsTag(sTag);
		aShell.setOkt(okt);
		aShell.setErt(ert);

		return aShell;
	}
	
	public ActionShell setShellMainAudit(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(Constants.SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${audit_shell_file}");
		
		argument.add("${wf:run()}");
		argument.add("${audit_log_file_path}");
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['start_date']}");
		}
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['start_date']}");
		}
		argument.add("${import_export_flag}");
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${tableName}");
			argument.add("${targetDirectory}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirYear']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMonth']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirDate']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirHour']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMinute']}");
		}
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${hiveTableName}");
			argument.add("${targetDirectoryValid}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_year']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_month']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_day']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_hour']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_minute']}");
		}		
		//argument.add("${targetDirectorySqoop}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirYear']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMonth']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirDate']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirHour']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMinute']}");		
		argument.add("${wf:id()}");
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${hadoop:counters(\"SQOOP_IMPORT_FROM_"+tableName+"\")[\"org.apache.hadoop.mapred.Task$Counter\"][\"MAP_OUTPUT_RECORDS\"]}");
		}
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${hadoop:counters(\"RECORD_VALIDATIONS\")[\"RECORD_WRITTEN\"]}");
		}
		argument.add("${wf:name()}");
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${sourceDirectory}");
			argument.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['raw_dir']}");
			argument.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['archive_dir']}");
		}
		
		sTag.setArgument(argument);
		sTag.setSshFile("${audit_shell_file}#${audit_shell_file}");
		sTag.setCaptureOutput("");
		
		okt.setOkt("HIVE_CREATE_AUDIT_TABLE_" + tableName);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt("CAPTURE_ERROR_LOGS_" + tableName);		
		
		shellAudit.setName("CAPTURE_AUDIT_LOGS_" + tableName);
		shellAudit.setsTag(sTag);
		shellAudit.setOkt(okt);
		shellAudit.setErt(ert);

		return shellAudit;
	}
	
public ActionShell setShellMainHousekeeping(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(Constants.SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${housekeeping_shell_file}");
		
		argument.add("${retention_period_raw_data}");
		//argument.add("${targetDirectorySqoop}");
		//argument.add("${retention_period_processed_data}");
		if(hconf.getImport_export_flag().equalsIgnoreCase("1")){
			argument.add("${targetDirectory}");
		}else if(hconf.getImport_export_flag().equalsIgnoreCase("3")){
			argument.add("${targetDirectory}/archive");
		}

		
		sTag.setArgument(argument);
		sTag.setSshFile("${housekeeping_shell_file}#${housekeeping_shell_file}");
		sTag.setCaptureOutput("");

		okt.setOkt("EMAIL_SUCCESS");
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt("CAPTURE_ERROR_LOGS_" + tableName);

		shellHousekeeping.setName("HOUSEKEEPING");
		shellHousekeeping.setsTag(sTag);
		shellHousekeeping.setOkt(okt);
		shellHousekeeping.setErt(ert);

		return shellHousekeeping;
	}

public ActionShell setShellMainError(HadoopConfig hconf) {
	
	LinkedList<String> argument = new LinkedList<String>();

	sTag.setXmlns(Constants.SHELL_XMLNS);
	sTag.setJobTracker("${jobTracker}");
	sTag.setNameNode("${nameNode}");
	sTag.setExec("${error_shell_file}");
	
	argument.add("${wf:run()}");
	argument.add("${audit_log_file_path}");
	if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['start_date']}");
	}
	if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['start_date']}");
	}
	argument.add("${import_export_flag}");
	if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${tableName}");
		argument.add("${targetDirectory}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirYear']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMonth']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirDate']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirHour']}/${wf:actionData('REFRESH_LAST_MODIFIED_DATE_VALUE')['targetDirMinute']}");
	}
	if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${hiveTableName}");
		argument.add("${targetDirectoryValid}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_year']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_month']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_day']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_hour']}/${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['dir_minute']}");
	}
	argument.add("${wf:id()}");
	//argument.add("${hadoop:counters(\"SQOOP_IMPORT_FROM_"+hconf.getTableName()+"\")[\"org.apache.hadoop.mapred.Task$Counter\"][\"MAP_OUTPUT_RECORDS\"]}");
	argument.add("No Records Logged");
	argument.add("${wf:name()}");
	argument.add("${wf:errorCode(wf:lastErrorNode())}");
	argument.add("${wf:errorMessage(wf:lastErrorNode())}");
	if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${sourceDirectory}");
	}
	
	sTag.setArgument(argument);
	sTag.setSshFile("${error_shell_file}#${error_shell_file}");
	sTag.setCaptureOutput("");

	okt.setOkt("EMAIL_FAILURE");
	ert.setErt("EMAIL_FAILURE");

	shellErr.setName("CAPTURE_ERROR_LOGS_" + tableName);
	shellErr.setsTag(sTag);
	shellErr.setOkt(okt);
	shellErr.setErt(ert);

	return shellErr;
}

}
