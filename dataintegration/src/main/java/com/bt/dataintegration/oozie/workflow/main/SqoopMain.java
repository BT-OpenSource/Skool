package com.bt.dataintegration.oozie.workflow.main;

import java.util.ArrayList;
import java.util.LinkedList;

import static com.bt.dataintegration.constants.Constants.*;

import com.bt.dataintegration.oozie.workflow.tags.ActionSqoopImport;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfiguration;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfigurationProperty;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.oozie.workflow.tags.SqoopImport;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class SqoopMain{

	private SqoopImport sqoopImport = new SqoopImport();
	private ActionSqoopImport actSqoop = new ActionSqoopImport();
	private LinkedList<String> args = new LinkedList<String>();
	private ArrayList<String> files = new ArrayList<String>();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	private GlobalConfiguration conf = new GlobalConfiguration();
	private GlobalConfigurationProperty prop = new GlobalConfigurationProperty();
	
	public ActionSqoopImport setSqoopMain(HadoopConfig hconf) {
		
		int flag = 0;
		String whereClause = "${lastModifiedDateColumn} >= to_date('${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['lowerBound']}','DD-MON-YYYY HH24:MI:SS') "
				+ "and ${lastModifiedDateColumn} < to_date('${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['upperBound']}','DD-MON-YYYY HH24:MI:SS')";
		String sqoopTargetDir = "${targetDirectory}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirYear']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMonth']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirDate']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirHour']}/"
				+ "${wf:actionData('"+ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE+"')['targetDirMinute']}";
		
		if(hconf.getDirectMode().equalsIgnoreCase("true")) {
			flag = 1;
		}
		
		prop.setName(SQOOP_CREDS_PARAM);
		prop.setValue("${pwd_provider_path}");
		conf.setProperty(prop);
		
		sqoopImport.setXmlns(SQOOP_XMLNS);
		sqoopImport.setJobTracker("${jobTracker}");
		sqoopImport.setNameNode("${nameNode}");
		sqoopImport.setConfiguration(conf);
		
		args.add("import");		
		if(flag == 1)
			args.add("--direct");
		args.add("--connect");
		args.add("${DBConnectString}");
		args.add("--username");
		args.add("${userName}");
		args.add("--password-alias");
		args.add("${password_alias}");
		args.add("--table");
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
		
		if(!hconf.getNumOfMappers().equals("0")) {
			args.add("-m");
			args.add("${numOfMappers}");
		}
		
		files.add("${oraJarPath}");				
		
		sqoopImport.setArgs(args);
		sqoopImport.setFiles(files);
		
		//okt.setOkt("COMPRESS_AVRO_DATA_FOR_" + hconf.getTableName());
		//okt.setOkt("REFRESH_LAST_MODIFIED_DATE_VALUE_FILE");
		okt.setOkt(ACTION_PIG_COMPRESS);
		//ert.setErt("EMAIL_FAILURE");
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		
		actSqoop.setName(ACTION_SQOOP_IMPORT);
		actSqoop.setSqImport(sqoopImport);
		actSqoop.setOkTo(okt);
		actSqoop.setErrorTo(ert);
		
		return actSqoop;
	}
}
