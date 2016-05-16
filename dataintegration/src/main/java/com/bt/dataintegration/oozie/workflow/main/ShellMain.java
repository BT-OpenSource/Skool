package com.bt.dataintegration.oozie.workflow.main;

import java.util.LinkedList;

import com.bt.dataintegration.oozie.workflow.tags.ActionShell;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.oozie.workflow.tags.ShellTag;
import com.bt.dataintegration.property.config.HadoopConfig;

import static com.bt.dataintegration.constants.Constants.*;

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
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getTableName();
		} else if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getHiveTableName();
		}
	}

	public ActionShell setShellMainInit(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${shell_file_init}");

		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${shell_file_path}");
			argument.add("${milestone_everytime}");
			argument.add("${targetDirectory}");
			okt.setOkt(ACTION_SQOOP_IMPORT);
			//ert.setErt("EMAIL_FAILURE");
			ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
			shellInit.setName(ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE);
		}
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${targetDirectory}");
			okt.setOkt(ACTION_FILESYSTEM_VALIDATIONS);
			//ert.setErt("EMAIL_FAILURE");
			ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
			shellInit.setName(ACTION_CAPTURE_DATE_AND_CREATEDIR);
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

		sTag.setXmlns(SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${shell_file}");
		
		argument.add("${shell_file_path}");
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}");
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}");
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}");
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}");
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}");
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['dateNow']}");
		argument.add("${targetDirectory}");
		
		sTag.setArgument(argument);
		sTag.setSshFile("${shell_file}#${shell_file}");
		sTag.setCaptureOutput("");

		//okt.setOkt("COMPRESS_AVRO_DATA_FOR_" + hconf.getTableName());
		okt.setOkt(ACTION_HIVE_CREATE_TABLE);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);

		aShell.setName(ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE_FILE);
		aShell.setsTag(sTag);
		aShell.setOkt(okt);
		aShell.setErt(ert);

		return aShell;
	}
	
	public ActionShell setShellMainAudit(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${audit_shell_file}");
		
		argument.add("${wf:run()}");
		argument.add("${audit_log_file_path}");
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['start_date']}");
		}
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['start_date']}");
		}
		argument.add("${import_export_flag}");
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${tableName}");
			argument.add("${targetDirectory}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}");
		}
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${hiveTableName}");
			argument.add("${targetDirectoryValid}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_year']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_month']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_day']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_hour']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_minute']}");
		}		
		//argument.add("${targetDirectorySqoop}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}");		
		argument.add("${wf:id()}");
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${hadoop:counters(\""+ACTION_SQOOP_IMPORT+"\")[\"org.apache.hadoop.mapred.Task$Counter\"][\"MAP_OUTPUT_RECORDS\"]}");
		}
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${hadoop:counters(\""+ACTION_RECORD_VALIDATIONS+"\")[\"RECORD_WRITTEN\"]}");
		}
		argument.add("${wf:name()}");
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${sourceDirectory}");
			argument.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['raw_dir']}");
			argument.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['archive_dir']}");
		}
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			argument.add("${milestone_everytime}");
		}
		sTag.setArgument(argument);
		sTag.setSshFile("${audit_shell_file}#${audit_shell_file}");
		sTag.setCaptureOutput("");
		
		okt.setOkt(ACTION_HIVE_CREATE_AUDIT_TABLE);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);		
		
		shellAudit.setName(ACTION_CAPTURE_AUDIT_LOGS);
		shellAudit.setsTag(sTag);
		shellAudit.setOkt(okt);
		shellAudit.setErt(ert);

		return shellAudit;
	}
	
public ActionShell setShellMainHousekeeping(HadoopConfig hconf) {
		
		LinkedList<String> argument = new LinkedList<String>();

		sTag.setXmlns(SHELL_XMLNS);
		sTag.setJobTracker("${jobTracker}");
		sTag.setNameNode("${nameNode}");
		sTag.setExec("${housekeeping_shell_file}");
		
		argument.add("${retention_period_raw_data}");
		//argument.add("${targetDirectorySqoop}");
		//argument.add("${retention_period_processed_data}");
		if(hconf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT)){
			argument.add("${targetDirectory}");
		}else if(hconf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			argument.add("${targetDirectory}/archive");
		}

		
		sTag.setArgument(argument);
		sTag.setSshFile("${housekeeping_shell_file}#${housekeeping_shell_file}");
		sTag.setCaptureOutput("");

		okt.setOkt(ACTION_EMAIL_SUCCESS);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);

		shellHousekeeping.setName(ACTION_HOUSEKEEPING);
		shellHousekeeping.setsTag(sTag);
		shellHousekeeping.setOkt(okt);
		shellHousekeeping.setErt(ert);

		return shellHousekeeping;
	}

public ActionShell setShellMainError(HadoopConfig hconf) {
	
	LinkedList<String> argument = new LinkedList<String>();

	sTag.setXmlns(SHELL_XMLNS);
	sTag.setJobTracker("${jobTracker}");
	sTag.setNameNode("${nameNode}");
	sTag.setExec("${error_shell_file}");
	
	argument.add("${wf:run()}");
	argument.add("${audit_log_file_path}");
	if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['start_date']}");
	}
	if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['start_date']}");
	}
	argument.add("${import_export_flag}");
	if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${tableName}");
		argument.add("${targetDirectory}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}/${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}");
	}
	if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${hiveTableName}");
		argument.add("${targetDirectoryValid}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_year']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_month']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_day']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_hour']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_minute']}");
	}
	argument.add("${wf:id()}");
	//argument.add("${hadoop:counters(\"SQOOP_IMPORT_FROM_"+hconf.getTableName()+"\")[\"org.apache.hadoop.mapred.Task$Counter\"][\"MAP_OUTPUT_RECORDS\"]}");
	argument.add("No Records Logged");
	argument.add("${wf:name()}");
	argument.add("${wf:errorCode(wf:lastErrorNode())}");
	argument.add("${wf:errorMessage(wf:lastErrorNode())}");
	if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${sourceDirectory}");
	}
	
	if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
		argument.add("${milestone_everytime}");
	}
	
	sTag.setArgument(argument);
	sTag.setSshFile("${error_shell_file}#${error_shell_file}");
	sTag.setCaptureOutput("");

	okt.setOkt(ACTION_EMAIL_FAILURE);
	ert.setErt(ACTION_EMAIL_FAILURE);

	shellErr.setName(ACTION_CAPTURE_ERROR_LOGS);
	shellErr.setsTag(sTag);
	shellErr.setOkt(okt);
	shellErr.setErt(ert);

	return shellErr;
}

}
