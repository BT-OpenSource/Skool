package com.bt.dataintegration.FileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Properties;

import jline.internal.InputStreamReader;

import org.apache.log4j.Logger;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class FileProcessImpl implements IFileProcess {

	final static Logger logger = Logger.getLogger(FileProcessImpl.class);

	public String checkHiveTableExists(DIConfig conf) {
		ProcessBuilder pb = null;
		Process p = null;
		int x = 0;
		logger.info("Preparing queries...");
		// String checkTable = "select * from " + hconf.getQueueName() + "." +
		// hconf.getHiveTableName() + " limit 1";
		String checkTable = "describe " + conf.getInstanceName() + "."
				+ conf.getHiveTable();
		// String useDB = "use " + hconf.getQueueName();
		String newTableName = conf.getHiveTable();
		try {

			pb = new ProcessBuilder("hive", "-e", checkTable);
			p = pb.start();
			x = p.waitFor();

			if (x == 0) {
				logger.warn("Table " + conf.getHiveTable()
						+ " already exists in Hive.");
				String choice = "";
				logger.warn("Select an option \n y - Continue \t n - Hault Execution");
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(System.in));
				choice = reader.readLine();
				while ((("y".equalsIgnoreCase(choice)) || ("n"
						.equalsIgnoreCase(choice))) == false) {
					choice = "";
					logger.warn("Select an option \n y - Continue \t n - Hault Execution");
					choice = reader.readLine();
				}

				if (choice.equalsIgnoreCase("y")) {

					logger.warn("Please provide new table name before proceeding");
					reader = new BufferedReader(
							new InputStreamReader(System.in));
					// ///////
					newTableName = "";
					while ((newTableName == null)
							|| ("".equalsIgnoreCase(newTableName))) {
						newTableName = reader.readLine();
						if ((newTableName == null)
								|| ("".equalsIgnoreCase(newTableName))) {
							logger.warn("Table name cannot be null. Please provide new table name before proceeding");
							newTableName = "";
						} else {
							checkTable = "describe " + conf.getHiveTable()
									+ "." + newTableName;
							pb = new ProcessBuilder("hive", "-e", checkTable);
							p = pb.start();
							x = p.waitFor();
							if (x == 0) {
								logger.warn("Hive table with name "
										+ newTableName
										+ " exists.Please provide another name.");
								newTableName = "";
							}
						}
					}
				}
			}

		} catch (Exception e) {
			logger.error("Error at checking hive table exists.");
		}
		return newTableName;
	}

	public LinkedList<String> readFile(File file)
			throws ArrayIndexOutOfBoundsException, IOException {

		LinkedList<String> hiveCols = new LinkedList<String>();

		BufferedReader reader = new BufferedReader(new FileReader(file));
		reader.readLine();
		String line = null;

		while ((line = reader.readLine()) != null) {

			String[] tokens = line.split(",");
			String comment = "";
			String cols = "";
			
			for(int i = 3;i<tokens.length;i++)
				comment= comment + tokens[i].replaceAll("[^a-zA-Z0-9\\s.,_@=\\/:]", "")+",";
			comment = comment.replaceAll("\\'", "`");
			comment = comment.replaceAll("\"", "``");
			if(!("".equalsIgnoreCase(comment))){
				if(comment.length()<256)
					comment = comment.substring(0, comment.length() - 1);
				else
					comment = comment.substring(0, 250);
				cols = tokens[0].toUpperCase() + "^^"+ tokens[1].toUpperCase() + "^^" + tokens[2].toUpperCase()+"^^"+comment;
			}
			else{
				cols = tokens[0].toUpperCase() + "^^"+ tokens[1].toUpperCase() + "^^" + tokens[2].toUpperCase();

			}
			hiveCols.add(cols);
		}
		return hiveCols;
	}

	public void prepareJobProperties(DIConfig conf) {
		logger.info("Preparing job.properties");
		Properties prop = new Properties();
		OutputStream fout = null;
		String fileName = "job.properties";
		StringBuffer colDet = new StringBuffer();

		String workspacePath = "${nameNode}/user/${queueName}/workspace/HDI_FILE_${source_name}_${hiveTableName}";
		String landingDir = "${nameNode}/user/${queueName}/landing/staging/${source_name}/HDI_FILE_${source_name}_${hiveTableName}";

		LinkedList<String> listMap = new LinkedList<String>();
		String mappingSheetName = conf.getMappingSheetname();
		String finalCols = "";
		String hiveCol = "";
		try {
			File file = new File(mappingSheetName);

			listMap = readFile(file);

			//populating for mapping field
			for (String s : listMap) {
				String[] field = s.split("\\^\\^");
				String line = field[0]+"^^"+field[1]+"^^"+field[2];
				colDet.append(line).append("|");
			}

			finalCols = colDet.toString().substring(0, colDet.length() - 1);

			hiveCol = Utility.mapDatatypes(listMap);
			Utility.hiveTextColumns = hiveCol;
		} 
	    catch (ArrayIndexOutOfBoundsException e) {
	        logger.error("Mapping sheet is not proper.");   
	        logger.error(e);
	        DirectoryHandler.cleanUpWorkspaceFile(conf);
	        throw new Error(e);
	    }
		catch (Exception e) {
		     logger.error("Error occured while reading the mapping sheet.");     
		     logger.error(e);
		     DirectoryHandler.cleanUpWorkspaceFile(conf);
		     throw new Error(e);
		}

		try {
			fout = new FileOutputStream(fileName);

			prop.setProperty("import_export_flag", conf.getImport_export_flag());
			prop.setProperty("nameNode", conf.getWorkflowNameNode());
			prop.setProperty("jobTracker", conf.getJobTracker());
			prop.setProperty("queueName", conf.getInstanceName());
			prop.setProperty("oozie.use.system.libpath", "TRUE");
			prop.setProperty("wf_application_path", workspacePath);
			if (conf.isCoordinatorFlag()) {
				prop.setProperty("oozie.coord.application.path", workspacePath);
				prop.setProperty("coordinator_required", "true");
				prop.setProperty("start", conf.getWfStartTime());
				prop.setProperty("end", conf.getWfEndTime());
				prop.setProperty("timezone", conf.getTimeZone());
				prop.setProperty("concurrency", conf.getConcurrency());
				prop.setProperty("throttle", conf.getThrottle());
				prop.setProperty("timeout", conf.getTimeout());
				prop.setProperty("frequency", conf.getFrequency());
			} else {
				prop.setProperty("oozie.wf.application.path", workspacePath);
				prop.setProperty("coordinator_required", "false");
			}
			prop.setProperty("success_emails", conf.getSuccessEmailId());
			prop.setProperty("failure_emails", conf.getFailureEmailId());
			prop.setProperty("hiveTableName", conf.getHiveTable());
			prop.setProperty("source_name", conf.getSourceName());

			// file related properties
			prop.setProperty("file_mask", conf.getFileMask());
			prop.setProperty("delimiter", conf.getFileDelimeter());
			prop.setProperty("control_file_name", conf.getControlFileName());
			prop.setProperty("jar_file", Constants.HDI_FILE_VALIDATE_JAR);
			prop.setProperty("jar_file_location",
					"${wf_application_path}/lib/${jar_file}");
			prop.setProperty("mapping_details", finalCols);
			//prop.setProperty("file_hive_cols", hiveCol);
			prop.setProperty("input_date_format", conf.getFileDateFormat());

			// //file scripts
			prop.setProperty("pig_script_location",
					"${wf_application_path}/pig_record_validator.pig");
			prop.setProperty("hiveCreateScript",
					"${wf_application_path}/HDI_${hiveTableName}_CREATE_TABLE.hql");
			prop.setProperty("hiveAddPartScript",
					"${wf_application_path}/HDI_${hiveTableName}_ADD_PARTITION.hql");
			// prop.setProperty("shell_file", "update_last_col_value.sh");
			prop.setProperty("kerberos_flag", conf.getEnvDetails());
			prop.setProperty("hive2_jdbc_url", conf.getHive2_jdbc_url()
					+ "${queueName}");
			prop.setProperty("hive2_server_principal",
					conf.getHive2_server_principal());
			prop.setProperty("application_nameNode", conf.getAppNameNode());
			prop.setProperty("oozie_action_sharelib_for_hive", "hive2");
			prop.setProperty("oozie_launcher_action_main_class",
					"org.apache.oozie.action.hadoop.Hive2Main");
			prop.setProperty("shell_file_init", "capture_date_createdir.sh");
			prop.setProperty("audit_log_file_path",
					"${nameNode}/user/${queueName}/HDI_AUDIT/audit_logs.txt");
			prop.setProperty("audit_log_path",
					"${nameNode}/user/${queueName}/HDI_AUDIT");
			prop.setProperty("audit_shell_file", "audit_logs.sh");
			prop.setProperty("error_shell_file", "error_logs.sh");
			prop.setProperty("audit_table_name", "HDI_AUDIT");
			prop.setProperty("hive_create_audit_table",
					"HDI_CREATE_AUDIT_TABLE.hql");
			String auditTableCols = Constants.HDI_AUDIT_COLS;
			prop.setProperty("hdi_audit_table_cols", auditTableCols);

			// file directory paths
			prop.setProperty("sourceDirectory", conf.getFileDirectory());
			// prop.setProperty("targetDirectoryTemp", landingDir+"/temp"); //to
			// be deleted in the end of the workflow and created in the first
			// shell action
			prop.setProperty("targetDirectoryValid", landingDir
					+ "/valid_files"); // year,month date to be appended in
										// workflow
			// prop.setProperty("targetDirectoryRejected",
			// landingDir+"/rejected_files"); //year,month date to be appended
			// in workflow and
			// later /invalid_records added for pig invalid output
			prop.setProperty("targetDirectory", landingDir);
			prop.setProperty("fileTrailerPresent",
					String.valueOf(conf.isFileTrailerPresent()));
			prop.setProperty("fileTrailerKeyword", conf.getFileTrailerKeyword());
			prop.setProperty("fileHeaderKeyword", conf.getFileHeaderKeyword());
			prop.setProperty("lineNumberData",
					String.valueOf(conf.getLineNumberData()));
			prop.store(fout, null);

		} catch (IOException e) {
			logger.error("Error Setting properties at JobProperties.storeProperties()");
			logger.error("", e);
			DirectoryHandler.cleanUpWorkspaceFile(conf);
			// DirectoryHandler.cleanUpWorkspace(conf);
			// DirectoryHandler.cleanUpLanding(conf);
			throw new Error(e);
		} finally {
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException e) {
					logger.error("Error Setting properties at JobProperties.storeProperties()");
					logger.error(e);
					DirectoryHandler.cleanUpWorkspaceFile(conf);
					throw new Error(e);
				}
			}
		}

		DirectoryHandler.sendFileToHDFS(conf, "job.properties");
		DirectoryHandler.givePermissionToHDFSFile(conf, "job.properties");
	}
}