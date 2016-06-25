package com.bt.dataintegration.oozie.workflow.main;

import java.util.ArrayList;
import java.util.LinkedList;

import static com.bt.dataintegration.constants.Constants.*;

import com.bt.dataintegration.oozie.workflow.tags.ActionSqoopTag;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfiguration;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfigurationProperty;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.oozie.workflow.tags.SqoopTag;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class SqoopMain{

	private SqoopTag sqoopTag = new SqoopTag();
	private ActionSqoopTag actSqoop = new ActionSqoopTag();
	private LinkedList<String> args = new LinkedList<String>();
	private ArrayList<String> files = new ArrayList<String>();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	private GlobalConfiguration conf = new GlobalConfiguration();
	private GlobalConfigurationProperty prop = new GlobalConfigurationProperty();
	
	public ActionSqoopTag setSqoopMain(HadoopConfig hconf) {
		
		int flag = 0;
		String ieFlag = "";
		ieFlag = hconf.getImport_export_flag();
		String whereClause = "${lastModifiedDateColumn} >= to_date('${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['lowerBound']}','DD-MM-YYYY HH24:MI:SS') "
				+ "and ${lastModifiedDateColumn} < to_date('${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['upperBound']}','DD-MM-YYYY HH24:MI:SS')";
		String sqoopTargetDir = "${targetDirectory}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}";
		
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			
			if(hconf.getDirectMode().equalsIgnoreCase("true")) {
				flag = 1;
			}
		}		
		prop.setName(SQOOP_CREDS_PARAM);
		prop.setValue("${pwd_provider_path}");
		conf.setProperty(prop);
		
		sqoopTag.setXmlns(SQOOP_XMLNS);
		sqoopTag.setJobTracker("${jobTracker}");
		sqoopTag.setNameNode("${nameNode}");
		sqoopTag.setConfiguration(conf);
		
		if(SQOOP_IMPORT.equalsIgnoreCase(ieFlag)) {
			args.add("import");
		} else if(SQOOP_EXPORT.equalsIgnoreCase(ieFlag)) {
			args.add("export");
		}			
		if(flag == 1)
			args.add("--direct");
		args.add("--connect");
		args.add("${DBConnectString}");
		args.add("--username");
		args.add("${userName}");
		args.add("--password-alias");
		args.add("${password_alias}");
		args.add("--table");
		if(SQOOP_IMPORT.equalsIgnoreCase(ieFlag)){
		args.add("${sqoopTableName}");
		args.add("--columns");
		args.add("${sqoopColumns}");
		if(!(hconf.getMapColumnJava().equalsIgnoreCase(""))) {
		
			args.add("--map-column-java");
			args.add("${mapColumnJava}");
		}
		if(!(hconf.getLastModifiedDateColumn().equals(""))){
			args.add("--where");
			args.add("\""+whereClause+"\"");
		}
		args.add("--target-dir");
		args.add(sqoopTargetDir);
		
		if((flag == 0) && (!hconf.getNumOfMappers().equals("1"))) {
			args.add("--split-by");
			args.add("${splitByColumn}");
		}		
		if(hconf.getSqoopFileFormat().contains("text")){
			args.add("--fields-terminated-by");
			args.add("\\001");
		}
		args.add("${sqoopFileFormat}");
		

		okt.setOkt(ACTION_PIG_COMPRESS);
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		actSqoop.setName(ACTION_SQOOP_IMPORT);
		}
		if("2".equalsIgnoreCase(ieFlag)) {
			args.add("${tableName}");
			args.add("--export-dir");
			if("".equalsIgnoreCase(hconf.getExport_user_dir())) {
				args.add(TEMP_FS);
			} else {
				args.add("${export_user_dir}");
			}						
			if((hconf.getUpdate_key_column() != null) && (hconf.getUpdate_mode() != null)) {
				args.add("--update-key");
				args.add("${update_key_column}");
				args.add("--update-mode");
				args.add("${update_mode}");
			}
			args.add("--fields-terminated-by");
			args.add("${fieldSeparator}");
			args.add("--input-null-string");
			args.add("${input_null_string}");
			args.add("--input-null-non-string");
			args.add("${input_null_non_string_column}");
			if("".equalsIgnoreCase(hconf.getExport_user_dir())) {
				okt.setOkt(ACTION_FS_DELETE);
			} else {
				okt.setOkt(ACTION_CAPTURE_AUDIT_LOGS);
			}			
			//ert.setErt("EMAIL_FAILURE");
			ert.setErt(ACTION_CAPTURE_ERROR_LOGS);			
			actSqoop.setName(ACTION_SQOOP_EXPORT_TO_RDBMS_TABLE);
		}
		if(!hconf.getNumOfMappers().equals("0")) {
			args.add("-m");
			args.add("${numOfMappers}");
		}
		files.add("${oraJarPath}");				
		
		sqoopTag.setArgs(args);
		sqoopTag.setFiles(files);
		
		//okt.setOkt("COMPRESS_AVRO_DATA_FOR_" + hconf.getTableName());
		//okt.setOkt("REFRESH_LAST_MODIFIED_DATE_VALUE_FILE");
	
		//ert.setErt("EMAIL_FAILURE");
		
		
		
		actSqoop.setSqTag(sqoopTag);
		actSqoop.setOkTo(okt);
		actSqoop.setErrorTo(ert);
		
		return actSqoop;
	}
}
