package com.bt.dataintegration.oozie.workflow.main;

import java.util.LinkedList;
import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.oozie.workflow.tags.ActionHive2;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.Hive2Tag;
import com.bt.dataintegration.oozie.workflow.tags.HiveConfigProperty;
import com.bt.dataintegration.oozie.workflow.tags.HiveConfiguration;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class Hive2Main implements Constants {

	//private Credentials creds = new Credentials();
	private LinkedList<String> param = new LinkedList<String>();
	private Hive2Tag h2tag = new Hive2Tag();
	private ActionHive2 actHive = new ActionHive2();
	private ActionHive2 addPartition = new ActionHive2();
	private ActionHive2 createAuditTable = new ActionHive2();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	private HiveConfiguration conf = new HiveConfiguration();
	private DIConfig diConf = new DIConfig().getDIConfigProperties();
	//public String tableName = null;
	
	private String exportImportFlag = null;
	
	public Hive2Main() {
		
		HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties();	
		exportImportFlag = hconf.getImport_export_flag();
		/*if(("1".equalsIgnoreCase(exportImportFlag)) || ("2".equalsIgnoreCase(exportImportFlag))) {
			tableName = hconf.getTableName();
		} else if("3".equalsIgnoreCase(exportImportFlag)) {
			tableName = hconf.getHiveTableName();
		}*/
	}

	public ActionHive2 setHive2MainHiveCreate(HadoopConfig hconf) {
		
		HiveConfigProperty cpropShareLib = new HiveConfigProperty();
		HiveConfigProperty cpropMainClass = new HiveConfigProperty();		
		
		LinkedList<HiveConfigProperty> propList = new LinkedList<HiveConfigProperty>();
		
		//String hiveSiteXMLPath = hconf.getNameNode() + "/user/"
			//	+ hconf.getQueueName() + "/" + hconf.getTableName() + "/workspace";
		
		if(diConf.getEnvDetails().equals("1")) {
			
			cpropShareLib.setName(HS2_SHARELIB);
			cpropShareLib.setValue("${oozie_action_sharelib_for_hive}");
			propList.add(cpropShareLib);
			cpropMainClass.setName(HS2_MAIN_CLASS);
			cpropMainClass.setValue("${oozie_launcher_action_main_class}");
			propList.add(cpropMainClass);
			conf.setProperty(propList);
		}		
		
		h2tag.setXmlns(HIVE_XMLNS);
		h2tag.setJobTracker("${jobTracker}");
		h2tag.setNameNode("${nameNode}");
		//h2tag.setJobXml(hiveSiteXMLPath + "/hive-site.xml");
		
		if(diConf.getEnvDetails().equals("1")) {
		
			h2tag.setConfig(conf);
		}		
		/*h2tag.setJdbcUrl(hconf.getHiveJDBCString());
		h2tag.setScript(hiveSiteXMLPath + "/" + hconf.getTableName() +"_CREATE_AVRO_TABLE.hql");*/
		
		//h2tag.setJdbcUrl(hconf.getHiveJDBCString());
		h2tag.setJdbcUrl("${hive2_jdbc_url}");
		// h2tag.setScript(hiveSiteXMLPath + "/" + hconf.getTableName()
		// +"_ADD_PARTITION.hql");		
		if((SQOOP_IMPORT.equalsIgnoreCase(exportImportFlag)) || (FILE_IMPORT.equalsIgnoreCase(exportImportFlag))) {
			h2tag.setScript("${hiveCreateScript}");
			okt.setOkt(ACTION_HIVE_ADD_PARTITION);
			//ert.setErt("EMAIL_FAILURE");
			ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
			actHive.setName(ACTION_HIVE_CREATE_TABLE);
		} else if((SQOOP_EXPORT.equalsIgnoreCase(exportImportFlag)) && ("".equalsIgnoreCase(hconf.getExport_user_dir()))) {
			h2tag.setScript("${hive_export_script_name}");
			okt.setOkt(ACTION_SQOOP_EXPORT_TO_RDBMS_TABLE);
			ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
			actHive.setName(ACTION_HIVE_EXTRACT_DATA);
		}
		
		
		if(SQOOP_IMPORT.equalsIgnoreCase(exportImportFlag)) {
			param.add("targetDirectory=${targetDirectory}");
			param.add("queueName=${queueName}");
			param.add("tableName=${tableName}");
			param.add("hiveTableName=${hiveTableName}");
			param.add("nameNode=${nameNode}");
			param.add("wf_application_path=${wf_application_path}");
			if(hconf.getSqoopFileFormat().contains("text")){
				param.add("hiveTextColumns=${hiveTextColumns}");
			}
		}
		if(FILE_IMPORT.equalsIgnoreCase(exportImportFlag)) {
			param.add("queueName=${queueName}");
			param.add("targetDirectoryValid=${targetDirectoryValid}");
			//param.add("file_hive_cols=${file_hive_cols}");
			param.add("hiveTableName=${hiveTableName}");
		}
		if(SQOOP_EXPORT.equalsIgnoreCase(exportImportFlag) && ("".equalsIgnoreCase(hconf.getExport_user_dir()))) {
			if(!("".equalsIgnoreCase(hconf.getTable_part_colname()))) {
				String part = hconf.getTable_part_colname();
				String[] record = part.split(",");
				for(int i = 0; i < record.length; i++) {
					String[] tokens = record[i].split(":");
					param.add(tokens[0].toUpperCase() + "=${" + tokens[0].toUpperCase() + "}");
				}
			}
			param.add("nameNode=${nameNode}");
			param.add("queueName=${queueName}");
			param.add("HDI_SQOOL_TEMP_DIR=${wf:id()}");
			param.add("hive_database_name=${hive_database_name}");
			param.add("export_hive_table=${export_hive_table}");
			
		}

		h2tag.setParams(param);		
		
		if(diConf.getEnvDetails().equals("2")) {
			
			actHive.setCred("'hive2'");
		} else {
		
			actHive.setCred("hive_credentials");
		}		
		actHive.setErt(ert);
		actHive.setOkt(okt);
		actHive.setHs2Tag(h2tag);		
		//actHive.setCred("hive2");
	
		return actHive;
	}
	
	public ActionHive2 setHive2MainPartition(HadoopConfig hconf) {
		
		HiveConfigProperty cpropShareLib = new HiveConfigProperty();
		HiveConfigProperty cpropMainClass = new HiveConfigProperty();
		
		LinkedList<HiveConfigProperty> propList = new LinkedList<HiveConfigProperty>();
		
		//String hiveSiteXMLPath = hconf.getNameNode() + "/user/"+ hconf.getQueueName() + "/" + hconf.getTableName() + "/workspace";
		
		String targetDir = "hiveAddPart=${targetDirectory}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}";
		
		if(diConf.getEnvDetails().equals("1")) {
			
			cpropShareLib.setName(HS2_SHARELIB);
			cpropShareLib.setValue("${oozie_action_sharelib_for_hive}");
			propList.add(cpropShareLib);
			cpropMainClass.setName(HS2_MAIN_CLASS);
			cpropMainClass.setValue("${oozie_launcher_action_main_class}");
			propList.add(cpropMainClass);
			conf.setProperty(propList);
		}	
		
		h2tag.setXmlns(HIVE_XMLNS);
		h2tag.setJobTracker("${jobTracker}");
		h2tag.setNameNode("${nameNode}");
		//h2tag.setJobXml(hiveSiteXMLPath + "/hive-site.xml");
		if(diConf.getEnvDetails().equals("1")) {
			
			h2tag.setConfig(conf);
		}	
		//h2tag.setJdbcUrl(hconf.getHiveJDBCString());
		h2tag.setJdbcUrl("${hive2_jdbc_url}");
		//h2tag.setScript(hiveSiteXMLPath + "/" + hconf.getTableName() +"_ADD_PARTITION.hql");
		h2tag.setScript("${hiveAddPartScript}");
		
		if(SQOOP_IMPORT.equalsIgnoreCase(exportImportFlag)) {
			param.add("targetDirYear=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}");
			param.add("targetDirMonth=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}");
			param.add("targetDirDate=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}");
			param.add("targetDirHour=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}");
			param.add("targetDirMinute=${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}");
			param.add("queueName=${queueName}");
			param.add("tableName=${tableName}");
			param.add("hiveTableName=${hiveTableName}");
			param.add("nameNode=${nameNode}");
			param.add(targetDir);
		}
		if(FILE_IMPORT.equalsIgnoreCase(exportImportFlag)) {
			param.add("dir_year=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_year']}");
			param.add("dir_month=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_month']}");
			param.add("dir_day=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_day']}");
			param.add("dir_hour=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_hour']}");
			param.add("dir_minute=${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_minute']}");
			param.add("hiveTableName=${hiveTableName}");
			param.add("hiveAddPart=${targetDirectoryValid}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_year']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_month']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_day']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_hour']}/${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['dir_minute']}");
		}
		
		h2tag.setParams(param);
		
		okt.setOkt(ACTION_CAPTURE_AUDIT_LOGS);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		
		if(diConf.getEnvDetails().equals("2")) {
			
			addPartition.setCred("hive2");
		} else {
		
			addPartition.setCred("hive_credentials");
		}	
		addPartition.setErt(ert);
		addPartition.setOkt(okt);
		addPartition.setHs2Tag(h2tag);
		addPartition.setName(ACTION_HIVE_ADD_PARTITION);
		//addPartition.setCred("hive2");
	
		return addPartition;
	}
	
public ActionHive2 setHive2MainCreateAuditTable(HadoopConfig hconf) {
		
		HiveConfigProperty cpropShareLib = new HiveConfigProperty();
		HiveConfigProperty cpropMainClass = new HiveConfigProperty();		
		
		LinkedList<HiveConfigProperty> propList = new LinkedList<HiveConfigProperty>();
		
		//String hiveSiteXMLPath = hconf.getNameNode() + "/user/"
			//	+ hconf.getQueueName() + "/" + hconf.getTableName() + "/workspace";
		
		if(diConf.getEnvDetails().equals("1")) {
			
			cpropShareLib.setName(HS2_SHARELIB);
			cpropShareLib.setValue("${oozie_action_sharelib_for_hive}");
			propList.add(cpropShareLib);
			cpropMainClass.setName(HS2_MAIN_CLASS);
			cpropMainClass.setValue("${oozie_launcher_action_main_class}");
			propList.add(cpropMainClass);
			conf.setProperty(propList);
		}		
		
		h2tag.setXmlns(HIVE_XMLNS);
		h2tag.setJobTracker("${jobTracker}");
		h2tag.setNameNode("${nameNode}");
		//h2tag.setJobXml(hiveSiteXMLPath + "/hive-site.xml");
		
		if(diConf.getEnvDetails().equals("1")) {
		
			h2tag.setConfig(conf);
		}		
		/*h2tag.setJdbcUrl(hconf.getHiveJDBCString());
		h2tag.setScript(hiveSiteXMLPath + "/" + hconf.getTableName() +"_CREATE_AVRO_TABLE.hql");*/
		
		//h2tag.setJdbcUrl(hconf.getHiveJDBCString());
		h2tag.setJdbcUrl("${hive2_jdbc_url}");
		// h2tag.setScript(hiveSiteXMLPath + "/" + hconf.getTableName()
		// +"_ADD_PARTITION.hql");
		h2tag.setScript("${hive_create_audit_table}");
		
		param.add("queueName=${queueName}");
		param.add("audit_table_name=${audit_table_name}");
		param.add("hdi_tab_cols=${hdi_audit_table_cols}");
		param.add("location=${audit_log_path}");
		
		if(FILE_IMPORT.equals(exportImportFlag)){
			param.add("table_name=${hiveTableName}");
		} else {
			param.add("table_name=${tableName}");
		}
		
		h2tag.setParams(param);
		
		if(hconf.isHousekeepRequired() && (SQOOP_IMPORT.equalsIgnoreCase(exportImportFlag))) {
			okt.setOkt(ACTION_HOUSEKEEPING);
		} else {
			okt.setOkt(ACTION_EMAIL_SUCCESS);
		}
		
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		
		if(diConf.getEnvDetails().equals("2")) {
			
			createAuditTable.setCred("'hive2'");
		} else {
		
			createAuditTable.setCred("hive_credentials");
		}		
		createAuditTable.setErt(ert);
		createAuditTable.setOkt(okt);
		createAuditTable.setHs2Tag(h2tag);
		createAuditTable.setName(ACTION_HIVE_CREATE_AUDIT_TABLE);
		//actHive.setCred("hive2");
	
		return createAuditTable;
	}
}
