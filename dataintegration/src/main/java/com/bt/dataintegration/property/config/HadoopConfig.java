package com.bt.dataintegration.property.config;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

import static com.bt.dataintegration.constants.Constants.*;

/**
 * @author Prabhu Om Abhinav Manish
 *
 */
public class HadoopConfig {

	final static Logger logger = Logger.getLogger(HadoopConfig.class);
	// Set property parameters as POJO
	private String databaseHost;
	private String workspacePath;
	private String tableOwner;
	private String sourceName;
	private String targetDirHour;
	private String tableName;
	private String hiveTableName;
	private String queueName;
	private String directMode;
	private String targetDirMonth;
	private String targetDirYear;
	private String targetDirectory;
	private String userName;
	private String queryMilestone;
	private String DBConnectString;
	private String targetDirMinute;
	private String lastModifiedDateColumn;
	private String numOfMappers;
	private String targetDirDate;
	private String appNameNode;
	private String workflowNameNode;
	private String splitbyColumn;
	private String lastModifiedDateValue;
	private String columnDetails;
	private boolean coordinatorRequired;
	private boolean housekeepRequired;
	// Will be removed in case of kerberos authentication
	private String clusterUsername;
	// private String clusterPassword;
	private String sqoopTargetDir;

	private String mapColumnJava;
	private String sqoopFileFormat;
	private String hiveTextColumn;
	private boolean compressionRequired;
	private String compressionCodec;

	// Export****************************//
	private String import_export_flag;
	private String export_user_dir;
	private String table_part_colname;

	private String stored_procedure_name;
	private String update_key_column;
	private String update_mode;
	private String input_null_string;
	private String input_null_non_string;
	private String staging_table;
	private String clear_staging_table;
	private String batch_mode;

	/**********************************/
	private String envDetails;
	private String fileTrailerKeyword;
	private String lineNumberData;
	private String landingDirectory;

	// SLA info
	private String slaStart;
	private String slaEnd;
	private boolean slaRequired;
	
	public boolean isSlaRequired() {
		return slaRequired;
	}

	public void setSlaRequired(boolean slaRequired) {
		this.slaRequired = slaRequired;
	}

	public String getSlaStart() {
		return slaStart;
	}

	public String getSlaEnd() {
		return slaEnd;
	}

	public void setSlaStart(String slaStart) {
		this.slaStart = slaStart;
	}

	public void setSlaEnd(String slaEnd) {
		this.slaEnd = slaEnd;
	}

	public boolean isCompressionRequired() {
		return compressionRequired;
	}

	public void setCompressionRequired(boolean compressionRequired) {
		this.compressionRequired = compressionRequired;
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	public String getDatabaseHost() {
		return databaseHost;
	}

	public void setDatabaseHost(String databaseHost) {
		this.databaseHost = databaseHost;
	}

	public String getLandingDirectory() {
		return landingDirectory;
	}

	public void setLandingDirectory(String landingDirectory) {
		this.landingDirectory = landingDirectory;
	}

	// ***********EXPORT //
	public String getTable_part_colname() {
		return table_part_colname;
	}

	public void setTable_part_colname(String table_part_colname) {
		this.table_part_colname = table_part_colname;
	}

	public String getExport_user_dir() {
		return export_user_dir;
	}

	public void setExport_user_dir(String export_user_dir) {
		this.export_user_dir = export_user_dir;
	}

	public String getClear_staging_table() {
		return clear_staging_table;
	}

	public void setClear_staging_table(String clear_staging_table) {
		this.clear_staging_table = clear_staging_table;
	}

	public String getStored_procedure_name() {
		return stored_procedure_name;
	}

	public void setStored_procedure_name(String stored_procedure_name) {
		this.stored_procedure_name = stored_procedure_name;
	}

	public String getUpdate_key_column() {
		return update_key_column;
	}

	public void setUpdate_key_column(String update_key_column) {
		this.update_key_column = update_key_column;
	}

	public String getUpdate_mode() {
		return update_mode;
	}

	public void setUpdate_mode(String update_mode) {
		this.update_mode = update_mode;
	}

	public String getInput_null_string() {
		return input_null_string;
	}

	public void setInput_null_string(String input_null_string) {
		this.input_null_string = input_null_string;
	}

	public String getInput_null_non_string() {
		return input_null_non_string;
	}

	public void setInput_null_non_string(String input_null_non_string) {
		this.input_null_non_string = input_null_non_string;
	}

	public String getStaging_table() {
		return staging_table;
	}

	public void setStaging_table(String staging_table) {
		this.staging_table = staging_table;
	}

	public String getBatch_mode() {
		return batch_mode;
	}

	public void setBatch_mode(String batch_mode) {
		this.batch_mode = batch_mode;
	}

	// ****************EXPORT ********//
	public String getWorkspacePath() {
		return workspacePath;
	}

	public String getTableOwner() {
		return tableOwner;
	}

	public String getSourceName() {
		return sourceName;
	}

	public String getTargetDirHour() {
		return targetDirHour;
	}

	public String getTableName() {
		return tableName;
	}

	public String getHiveTableName() {
		return hiveTableName;
	}

	public String getQueueName() {
		return queueName;
	}

	public String getDirectMode() {
		return directMode;
	}

	public String getTargetDirMonth() {
		return targetDirMonth;
	}

	public String getTargetDirYear() {
		return targetDirYear;
	}

	public String getTargetDirectory() {
		return targetDirectory;
	}

	public String getUserName() {
		return userName;
	}

	public String getQueryMilestone() {
		return queryMilestone;
	}

	public String getDBConnectString() {
		return DBConnectString;
	}

	public String getTargetDirMinute() {
		return targetDirMinute;
	}

	public String getLastModifiedDateColumn() {
		return lastModifiedDateColumn;
	}

	public String getNumOfMappers() {
		return numOfMappers;
	}

	public String getTargetDirDate() {
		return targetDirDate;
	}

	public String getAppNameNode() {
		return appNameNode;
	}

	public String getWorkflowNameNode() {
		return workflowNameNode;
	}

	public String getSplitbyColumn() {
		return splitbyColumn;
	}

	public String getLastModifiedDateValue() {
		return lastModifiedDateValue;
	}

	public String getColumnDetails() {
		return columnDetails;
	}

	public boolean isCoordinatorRequired() {
		return coordinatorRequired;
	}

	public boolean isHousekeepRequired() {
		return housekeepRequired;
	}

	public String getClusterUsername() {
		return clusterUsername;
	}

	public String getSqoopTargetDir() {
		return sqoopTargetDir;
	}

	public String getMapColumnJava() {
		return mapColumnJava;
	}

	public String getSqoopFileFormat() {
		return sqoopFileFormat;
	}

	public String getHiveTextColumn() {
		return hiveTextColumn;
	}

	public String getImport_export_flag() {
		return import_export_flag;
	}

	public String getEnvDetails() {
		return envDetails;
	}

	public String getFileTrailerKeyword() {
		return fileTrailerKeyword;
	}

	public String getLineNumberData() {
		return lineNumberData;
	}

	public void setWorkspacePath(String workspacePath) {
		this.workspacePath = workspacePath;
	}

	public void setTableOwner(String tableOwner) {
		this.tableOwner = tableOwner;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public void setTargetDirHour(String targetDirHour) {
		this.targetDirHour = targetDirHour;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setHiveTableName(String hiveTableName) {
		this.hiveTableName = hiveTableName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public void setDirectMode(String directMode) {
		this.directMode = directMode;
	}

	public void setTargetDirMonth(String targetDirMonth) {
		this.targetDirMonth = targetDirMonth;
	}

	public void setTargetDirYear(String targetDirYear) {
		this.targetDirYear = targetDirYear;
	}

	public void setTargetDirectory(String targetDirectory) {
		this.targetDirectory = targetDirectory;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public void setQueryMilestone(String queryMilestone) {
		this.queryMilestone = queryMilestone;
	}

	public void setDBConnectString(String dBConnectString) {
		DBConnectString = dBConnectString;
	}

	public void setTargetDirMinute(String targetDirMinute) {
		this.targetDirMinute = targetDirMinute;
	}

	public void setLastModifiedDateColumn(String lastModifiedDateColumn) {
		this.lastModifiedDateColumn = lastModifiedDateColumn;
	}

	public void setNumOfMappers(String numOfMappers) {
		this.numOfMappers = numOfMappers;
	}

	public void setTargetDirDate(String targetDirDate) {
		this.targetDirDate = targetDirDate;
	}

	public void setAppNameNode(String appNameNode) {
		this.appNameNode = appNameNode;
	}

	public void setWorkflowNameNode(String workflowNameNode) {
		this.workflowNameNode = workflowNameNode;
	}

	public void setSplitbyColumn(String splitbyColumn) {
		this.splitbyColumn = splitbyColumn;
	}

	public void setLastModifiedDateValue(String lastModifiedDateValue) {
		this.lastModifiedDateValue = lastModifiedDateValue;
	}

	public void setColumnDetails(String columnDetails) {
		this.columnDetails = columnDetails;
	}

	public void setCoordinatorRequired(boolean coordinatorRequired) {
		this.coordinatorRequired = coordinatorRequired;
	}

	public void setHousekeepRequired(boolean housekeepRequired) {
		this.housekeepRequired = housekeepRequired;
	}

	public void setClusterUsername(String clusterUsername) {
		this.clusterUsername = clusterUsername;
	}

	public void setSqoopTargetDir(String sqoopTargetDir) {
		this.sqoopTargetDir = sqoopTargetDir;
	}

	public void setMapColumnJava(String mapColumnJava) {
		this.mapColumnJava = mapColumnJava;
	}

	public void setSqoopFileFormat(String sqoopFileFormat) {
		this.sqoopFileFormat = sqoopFileFormat;
	}

	public void setHiveTextColumn(String hiveTextColumn) {
		this.hiveTextColumn = hiveTextColumn;
	}

	public void setImport_export_flag(String import_export_flag) {
		this.import_export_flag = import_export_flag;
	}

	public void setEnvDetails(String envDetails) {
		this.envDetails = envDetails;
	}

	public void setFileTrailerKeyword(String fileTrailerKeyword) {
		this.fileTrailerKeyword = fileTrailerKeyword;
	}

	public void setLineNumberData(String lineNumberData) {
		this.lineNumberData = lineNumberData;
	}

	/**
	 * This method is to get and set the all the sqoop properties properties
	 * which is required to run sqoop job.
	 */

	public HadoopConfig getHadoopConfigProperties() {

		Properties properties = null;
		HadoopConfig confObj = new HadoopConfig();
		try {

			try {
				properties = Utility.readConfigProperties(JOB_PROP_FILE);

			} catch (Exception e) {
				logger.error("Error loading properties");
				logger.error(e);
				throw new Error(e);
			}

			confObj.setQueueName(properties.getProperty("queueName"));
			confObj.setWorkflowNameNode(properties.getProperty("nameNode"));
			confObj.setImport_export_flag(properties
					.getProperty("import_export_flag"));

			confObj.setAppNameNode(properties
					.getProperty("application_nameNode"));
			confObj.setSourceName(properties.getProperty("source_name"));
			confObj.setEnvDetails(properties.getProperty("kerberos_flag"));
			if (properties.getProperty("coordinator_required")
					.equalsIgnoreCase("true")) {
				confObj.setCoordinatorRequired(true);
			} else {
				confObj.setCoordinatorRequired(false);
			}

			String import_flag = confObj.getImport_export_flag();

			if (!(confObj.import_export_flag.equals(SQOOP_EXPORT))) {
				String compReq = properties.getProperty("compression_required");

				if ("true".equals(compReq)) {
					confObj.setCompressionRequired(true);
				} else {
					confObj.setCompressionRequired(false);
				}

				if (confObj.isCompressionRequired() == true) {
					confObj.setCompressionCodec(properties
							.getProperty("compression_codec"));
				}
			}

			if (import_flag.equalsIgnoreCase(SQOOP_IMPORT)) {
				confObj.setDatabaseHost(properties.getProperty("database_host"));
				confObj.setLandingDirectory(properties
						.getProperty("landingDirApp"));

				confObj.setNumOfMappers(properties.getProperty("numOfMappers"));

				confObj.setTableName(properties.getProperty("tableName"));
				confObj.setDBConnectString(properties
						.getProperty("DBConnectString"));
				confObj.setQueryMilestone(properties
						.getProperty("queryMilestone"));
				confObj.setUserName(properties.getProperty("userName"));
				confObj.setColumnDetails(properties
						.getProperty("columnDetails"));
				confObj.setSplitbyColumn(properties
						.getProperty("splitbyColumn"));
				confObj.setDirectMode(properties.getProperty("directMode"));

				confObj.setLastModifiedDateColumn(properties
						.getProperty("lastModifiedDateColumn"));
				confObj.setTargetDirectory(properties
						.getProperty("targetDirectory"));
				confObj.setLastModifiedDateValue(properties
						.getProperty("lastModifiedDateValue"));

				confObj.setTargetDirYear(DirectoryHandler.targetDirYear);
				confObj.setTargetDirMonth(DirectoryHandler.targetDirMonth);
				confObj.setTargetDirHour(DirectoryHandler.targetDirHour);
				confObj.setTargetDirMinute(DirectoryHandler.targetDirMinute);
				confObj.setTargetDirDate(DirectoryHandler.targetDirDate);

				confObj.setTableOwner(properties.getProperty("schema_name"));

				confObj.setTargetDirectory(confObj.getLandingDirectory() + "/"
						+ confObj.getTargetDirYear() + "/"
						+ confObj.getTargetDirMonth() + "/"
						+ confObj.getTargetDirDate() + "/"
						+ confObj.getTargetDirHour() + "/"
						+ confObj.getTargetDirMinute());
				/*
				 * confObj.setSqoopTargetDir(confObj.getAppNameNode() + "/user/"
				 * + confObj.getQueueName() + "/" + confObj.getTableName() +
				 * "/landing/raw_data/"+ confObj.getTargetDirYear() + "/" +
				 * confObj.getTargetDirMonth() + "/" +
				 * confObj.getTargetDirDate() + "/" +
				 * confObj.getTargetDirHour()+"/"+confObj.getTargetDirMinute());
				 */
				confObj.setWorkspacePath(confObj.getAppNameNode() + "/user/"
						+ confObj.getQueueName() + "/workspace/HDI_"
						+ confObj.getSourceName() + "_"
						+ confObj.getTableOwner() + "_"
						+ confObj.getTableName());
				// confObj.setClusterUsername("cloudera");
				// confObj.setClusterPassword("cloudera");
				confObj.setMapColumnJava(properties
						.getProperty("mapColumnJava"));
				confObj.setSqoopFileFormat(properties
						.getProperty("sqoopFileFormat"));
				if (!(confObj.getSqoopFileFormat().contains("avro"))) {
					confObj.setHiveTextColumn(properties
							.getProperty("hiveTextColumns"));
				}

				confObj.setHiveTableName(properties
						.getProperty("hiveTableName"));

				if (properties.getProperty("retention_period_raw_data")
						.equalsIgnoreCase("")) {
					confObj.setHousekeepRequired(false);
				} else {
					confObj.setHousekeepRequired(true);
					;
				}

			}

			else if (import_flag.equalsIgnoreCase(FILE_IMPORT)) {
				confObj.setHiveTableName(properties
						.getProperty("hiveTableName"));
				confObj.setFileTrailerKeyword(properties
						.getProperty("FileTrailerKeyword"));
				confObj.setLineNumberData(properties
						.getProperty("lineNumberData"));
			} else {// confObj.setDirectMode(properties.getProperty("direct_flag"));
				if (null != properties.getProperty("update_mode")) {
					confObj.setUpdate_mode(properties
							.getProperty("update_mode"));
					confObj.setUpdate_key_column(properties
							.getProperty("update_key_column"));
				}

				confObj.setExport_user_dir(properties
						.getProperty("export_user_dir"));

				confObj.setTable_part_colname(properties
						.getProperty("partition_cols_details"));

				confObj.setTableName(properties.getProperty("tableName"));
				// confObj.setInput_null_string(properties.getProperty("input_null_string"));
				// confObj.setInput_null_non_string(properties.getProperty("input-null-non-string"));
				// confObj.setStaging_table(properties.getProperty("staging_table"));
				// confObj.setClear_staging_table(properties.getProperty("clear_staging_table"));
				// confObj.setBatch_mode(properties.getProperty("batch_mode"));
				// confObj.setColumnDetails(properties.getProperty("selected_columns_name"));
				confObj.setAppNameNode(properties
						.getProperty("application_nameNode"));
				confObj.setNumOfMappers(properties.getProperty("numOfMappers"));
				confObj.setTableOwner(properties.getProperty("schema_name"));
			}			

			//SLA information
			String slaStart = properties.getProperty("sla_start");
			String slaEnd = properties.getProperty("sla_end");
			String slaRequired = properties.getProperty("sla_required");
			
			if("true".equalsIgnoreCase(slaRequired)) {
				confObj.setSlaRequired(true);
				if((! "".equalsIgnoreCase(slaStart)) && (! "".equalsIgnoreCase(slaEnd))) {
					confObj.setSlaStart(slaStart);
					confObj.setSlaEnd(slaEnd);
				}
			}					
			// }
		} catch (Exception e) {
			logger.error("Error loading properties at HadoopConfig");
			logger.error(e);
			DirectoryHandler.cleanUpLanding(confObj);
			DirectoryHandler.cleanUpWorkspace(confObj);
			throw new Error(e);
		}
		return confObj;
	}
}
