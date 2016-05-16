package com.bt.dataintegration.oozie.workflow.main;

import static com.bt.dataintegration.constants.Constants.ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE;

import java.util.LinkedList;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.oozie.workflow.tags.ActionPig;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfiguration;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfigurationProperty;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.oozie.workflow.tags.PigTag;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class PigMain implements Constants {

	private PigTag ptag = new PigTag();
	private ActionPig actPig = new ActionPig();
	private LinkedList<String> params = new LinkedList<String>();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	private GlobalConfiguration configuration = new GlobalConfiguration();
	private GlobalConfigurationProperty prop = new GlobalConfigurationProperty();

	public ActionPig setPigMain(HadoopConfig hconf) {
		
		String tableName = null;
		
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getTableName();
		} else if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getHiveTableName();
		}
		
		
		ptag.setJobTracker("${jobTracker}");
		ptag.setNameNode("${nameNode}");
		
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			
			prop.setName(PIG_CONF_PROP_NAME);
			prop.setValue(PIG_CONF_PROP_VALUE);
			configuration.setProperty(prop);
			ptag.setConf(configuration);
		}
		//ptag.setJobXml(workspacePath + "/hive-site.xml");		
		
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			ptag.setScript("${pigScript}");
			params.add("year=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}");
			params.add("month=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}");
			params.add("date=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}");
			params.add("hour=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}");
			params.add("minute=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}");
			params.add("queueName=${queueName}");
			params.add("tableName=${tableName}");
			params.add("nameNode=${nameNode}");
			params.add("targetDirectory=${targetDirectory}");
		}
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			ptag.setScript("${pig_script_location}");
			params.add("jar_file=${jar_file}");
			params.add("temp_dir=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['temp_dir']}");
			params.add("pig_cols=${mapping_details}");
			params.add("input_date_format=${input_date_format}");
			params.add("valid_final_dir=${targetDirectoryValid}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_year']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_month']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_day']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_hour']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_minute']}");
			params.add("rejected_dir=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['rejected_dir']}/invalid_records");
			params.add("fileTrailerKeyword=${fileTrailerKeyword}");
			params.add("fileHeaderKeyword=${fileHeaderKeyword}");
			params.add("lineNumberData=${lineNumberData}");
		}
		
		ptag.setParam(params);	
		
		//okt.setOkt("HIVE_CREATE_TABLE_" + hconf.getTableName());
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			okt.setOkt(ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE_FILE);
			actPig.setName(ACTION_PIG_COMPRESS);
		}
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			ptag.setFileName("lib/${jar_file}#${jar_file}");
			okt.setOkt(ACTION_HIVE_CREATE_TABLE);
			actPig.setName(ACTION_RECORD_VALIDATIONS);
		}
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		
		actPig.setErt(ert);
		actPig.setOkt(okt);
		actPig.setPtag(ptag);
		//actPig.setRetryInt(RETRY_INTERVAL);
		//actPig.setRetryMax(RETRY_MAX);
		
		return actPig;
	}
}
