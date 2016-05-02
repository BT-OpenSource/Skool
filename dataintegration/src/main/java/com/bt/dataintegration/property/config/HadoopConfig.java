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
	private String workspacePath;
	private String tableOwner;
	private String sourceName;
	private String jobTracker;
	private String password;
	private String targetDirHour;
	private String failure_emails;
	private String tableName;
	private String hiveTableName;
	private String queueName;
	private String end;
	private String consDetails;
	private String directMode;
	private String start;
	private String targetDirMonth;
	private String partDetails;
	private String targetDirYear;
	private String queryIncremental;
	private String targetDirectory;
	private String userName;
	private String queryMilestone;
	private String userSelectedColumns;
	private String success_emails;
	private String DBConnectString;
	private String targetDirMinute;
	private String lastModifiedDateColumn;
	private String numOfMappers;
	private String targetDirDate;
	private String bst_timezone;
	private String tableSize;
	private String appNameNode;
	private String workflowNameNode;
	private String splitbyColumn;
	private String lastModifiedDateValue;
	private String columnDetails;
	private String sqoopTargetDirMilestone;
	private boolean coordinatorRequired;
	private boolean housekeepRequired;
    // Will be removed in case of kerberos authentication
	private String clusterUsername;
	//private String clusterPassword;
	private String sqoopTargetDir;

	private String sqoopWhereClause;
	private String mapColumnJava;
    private String sqoopColumns;
    private String hiveJDBCString;
    private String workflowHiveJDBCString;
    private String hive2ServerPrincipal;
    private String sqoopFileFormat;
    private String hiveTextColumn;
    //Export
    private String import_export_flag;
    

  
	private String stored_procedure_name ; 
	private String update_key_column ; 
	private String update_mode;
	private String input_null_string ;
	private String input_null_non_string ;
	private String staging_table;   
	private String clear_staging_table;   
	private String batch_mode;
	private String columns_name ;
	private String envDetails;
	private String fileTrailerKeyword;
	private String lineNumberData;
	
	public String getClear_staging_table() {
		return clear_staging_table;
	}

	
	public String getFileTrailerKeyword() {
		return fileTrailerKeyword;
	}


	public String getLineNumberData() {
		return lineNumberData;
	}


	public void setFileTrailerKeyword(String fileTrailerKeyword) {
		this.fileTrailerKeyword = fileTrailerKeyword;
	}


	public void setLineNumberData(String lineNumberData) {
		this.lineNumberData = lineNumberData;
	}


	public void setClear_staging_table(String clear_staging_table) {
		this.clear_staging_table = clear_staging_table;
	}

	
	public String getImport_export_flag() {
		return import_export_flag;
	}

	public void setImport_export_flag(String import_export_flag) {
		this.import_export_flag = import_export_flag;
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

	/* **********************************************  */
    
    public String getSqoopWhereClause() {
		return sqoopWhereClause;
	}

	public void setSqoopWhereClause(String sqoopWhereClause) {
		this.sqoopWhereClause = sqoopWhereClause;
	}

	public String getMapColumnJava() {
		return mapColumnJava;
	}

	public void setMapColumnJava(String mapColumnJava) {
		this.mapColumnJava = mapColumnJava;
	}

	public String getSqoopColumns() {
		return sqoopColumns;
	}

	public void setSqoopColumns(String sqoopColumns) {
		this.sqoopColumns = sqoopColumns;
	}

	
	public String getSqoopTargetDir() {
		return sqoopTargetDir;
	}

	public void setSqoopTargetDir(String sqoopTargetDir) {
		this.sqoopTargetDir = sqoopTargetDir;
	}

	public String getJobTracker() {
		return jobTracker;
	}

	public String getPassword() {
		return password;
	}

	public String getTargetDirHour() {
		return targetDirHour;
	}

	public String getFailure_emails() {
		return failure_emails;
	}

	public String getTableName() {
		return tableName;
	}

	public String getQueueName() {
		return queueName;
	}

	public String getEnd() {
		return end;
	}

	public String getConsDetails() {
		return consDetails;
	}

	public String getDirectMode() {
		return directMode;
	}

	public String getStart() {
		return start;
	}

	public String getTargetDirMonth() {
		return targetDirMonth;
	}

	public String getPartDetails() {
		return partDetails;
	}

	public String getTargetDirYear() {
		return targetDirYear;
	}

	public String getQueryIncremental() {
		return queryIncremental;
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

	public String getUserSelectedColumns() {
		return userSelectedColumns;
	}

	public String getSuccess_emails() {
		return success_emails;
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

	public String getBst_timezone() {
		return bst_timezone;
	}

	public String getTableSize() {
		return tableSize;
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

	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setTargetDirHour(String targetDirHour) {
		this.targetDirHour = targetDirHour;
	}

	public void setFailure_emails(String failure_emails) {
		this.failure_emails = failure_emails;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public void setConsDetails(String consDetails) {
		this.consDetails = consDetails;
	}

	public void setDirectMode(String directMode) {
		this.directMode = directMode;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public void setTargetDirMonth(String targetDirMonth) {
		this.targetDirMonth = targetDirMonth;
	}

	public void setPartDetails(String partDetails) {
		this.partDetails = partDetails;
	}

	public void setTargetDirYear(String targetDirYear) {
		this.targetDirYear = targetDirYear;
	}

	public void setQueryIncremental(String queryIncremental) {
		this.queryIncremental = queryIncremental;
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

	public void setUserSelectedColumns(String userSelectedColumns) {
		this.userSelectedColumns = userSelectedColumns;
	}

	public void setSuccess_emails(String success_emails) {
		this.success_emails = success_emails;
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

	public void setBst_timezone(String bst_timezone) {
		this.bst_timezone = bst_timezone;
	}

	public void setTableSize(String tableSize) {
		this.tableSize = tableSize;
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

	public String getSqoopTargetDirMilestone() {
		return sqoopTargetDirMilestone;
	}

	public void setSqoopTargetDirMilestone(String sqoopTargetDirMilestone) {
		this.sqoopTargetDirMilestone = sqoopTargetDirMilestone;
	}

	public String getClusterUsername() {
		return clusterUsername;
	}

	/*public String getClusterPassword() {
		return clusterPassword;
	}*/

	public void setClusterUsername(String clusterUsername) {
		this.clusterUsername = clusterUsername;
	}

	/*public void setClusterPassword(String clusterPassword) {
		this.clusterPassword = clusterPassword;
	}*/

	public String getHiveJDBCString() {
		return hiveJDBCString;
	}

	public void setHiveJDBCString(String hiveJDBCString) {
		this.hiveJDBCString = hiveJDBCString;
	}

	
	public String getAppNameNode() {
		return appNameNode;
	}

	public void setAppNameNode(String appNameNode) {
		this.appNameNode = appNameNode;
	}

	public String getWorkflowNameNode() {
		return workflowNameNode;
	}

	public void setWorkflowNameNode(String workflowNameNode) {
		this.workflowNameNode = workflowNameNode;
	}

	
	public String getWorkflowHiveJDBCString() {
		return workflowHiveJDBCString;
	}

	public void setWorkflowHiveJDBCString(String workflowHiveJDBCString) {
		this.workflowHiveJDBCString = workflowHiveJDBCString;
	}

	public String getHive2ServerPrincipal() {
		return hive2ServerPrincipal;
	}

	public void setHive2ServerPrincipal(String hive2ServerPrincipal) {
		this.hive2ServerPrincipal = hive2ServerPrincipal;
	}

	public String getSqoopFileFormat() {
		return sqoopFileFormat;
	}

	public void setSqoopFileFormat(String sqoopFileFormat) {
		this.sqoopFileFormat = sqoopFileFormat;
	}

	
	public String getHiveTableName() {
		return hiveTableName;
	}

	public void setHiveTableName(String hiveTableName) {
		this.hiveTableName = hiveTableName;
	}

	
	public String getTableOwner() {
		return tableOwner;
	}

	public void setTableOwner(String tableOwner) {
		this.tableOwner = tableOwner;
	}

	
	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	
	
	public String getWorkspacePath() {
		return workspacePath;
	}

	public void setWorkspacePath(String workspacePath) {
		this.workspacePath = workspacePath;
	}

	
	public boolean isCoordinatorRequired() {
		return coordinatorRequired;
	}

	public void setCoordinatorRequired(boolean coordinatorRequired) {
		this.coordinatorRequired = coordinatorRequired;
	}

	public boolean isHousekeepRequired() {
		return housekeepRequired;
	}

	public void setHousekeepRequired(boolean housekeepRequired) {
		this.housekeepRequired = housekeepRequired;
	}
	
	public String getEnvDetails() {
		return envDetails;
	}

	public void setEnvDetails(String envDetails) {
		this.envDetails = envDetails;
	}

	
	public String getHiveTextColumn() {
		return hiveTextColumn;
	}


	public void setHiveTextColumn(String hiveTextColumn) {
		this.hiveTextColumn = hiveTextColumn;
	}


	/**
	 * This method is to get and set the all the sqoop properties properties
	 * which is required to run sqoop job.
	 */

	public HadoopConfig getHadoopConfigProperties() {

		Properties properties = null;
		final String conffileName = "job.properties";
		HadoopConfig confObj = new HadoopConfig();
		try {

			//if (new ImplSqoopImport().validateSqoopPropFileAccess()) {
				try {
					properties = Utility.readConfigProperties(conffileName);

				} catch (Exception e) {
					logger.error("Error loading properties");
					logger.error(e);
					throw new Error(e);
				}

				// System.out.println(properties.getProperty("cluster_haas_instance_name"));
				
				confObj.setJobTracker(properties.getProperty("jobTracker"));
				confObj.setFailure_emails(properties.getProperty("failure_emails"));
				confObj.setSuccess_emails(properties.getProperty("success_emails"));
				confObj.setQueueName(properties.getProperty("queueName"));
				confObj.setWorkflowNameNode(properties.getProperty("nameNode"));
                confObj.setImport_export_flag(properties.getProperty("import_export_flag"));
                
                confObj.setAppNameNode(properties.getProperty("application_nameNode"));
                confObj.setSourceName(properties.getProperty("source_name"));
                if(properties.getProperty("kerberos_flag").equals("1")) {
               	 String jdbcStr="jdbc:hive2://tplhc01c002:10000/"+confObj.getQueueName();
               	 confObj.setHiveJDBCString(jdbcStr);
                } else {
               	 confObj.setHiveJDBCString(HIVE2_CON_STRING_VM);
                }
                confObj.setEnvDetails(properties.getProperty("kerberos_flag"));
                confObj.setWorkflowHiveJDBCString(properties.getProperty("hive2_jdbc_url"));
                confObj.setHive2ServerPrincipal(properties.getProperty("hive2_server_principal"));
                if(properties.getProperty("coordinator_required").equalsIgnoreCase("true")){
               	 confObj.setCoordinatorRequired(true);
                }                
                else{
               	 confObj.setCoordinatorRequired(false);
                }
                
                String import_flag=confObj.getImport_export_flag(); 
                
                 if(import_flag.equalsIgnoreCase("1")){
                	 confObj.setNumOfMappers(properties.getProperty("numOfMappers"));
                	 
                	 confObj.setTableName(properties.getProperty("tableName"));
                	 confObj.setDBConnectString(properties
     						.getProperty("DBConnectString"));
                	confObj.setQueryMilestone(properties
    						.getProperty("queryMilestone"));
                	confObj.setPassword(properties.getProperty("password"));
    				confObj.setUserName(properties.getProperty("userName"));
    				confObj.setColumnDetails(properties.getProperty("columnDetails"));
    				confObj.setSplitbyColumn(properties.getProperty("splitbyColumn"));						
    				confObj.setDirectMode(properties.getProperty("directMode"));
    				
    				confObj.setLastModifiedDateColumn(properties.getProperty("lastModifiedDateColumn"));						
    				confObj.setTargetDirectory(properties.getProperty("targetDirectory"));			
    				confObj.setLastModifiedDateValue(properties.getProperty("lastModifiedDateValue"));		
    				confObj.setTargetDirYear(properties.getProperty("targetDirYear"));						
    				confObj.setTargetDirMonth(properties.getProperty("targetDirMonth"));						
    				confObj.setTargetDirHour(properties.getProperty("targetDirHour"));						
    				confObj.setTargetDirMinute(properties.getProperty("targetDirMinute"));						
    				confObj.setTargetDirDate(properties.getProperty("targetDirDate"));	
    				confObj.setTableOwner(properties.getProperty("schema_name"));
    				
    				confObj.setTargetDirectory(confObj.getAppNameNode()
    						+ "/user/" + confObj.getQueueName() + "/landing/staging/"
    						+ confObj.getSourceName()+"/"+ confObj.getTableOwner()+"/HDI_"
    						+ confObj.getTableName() +"/"
    						+ confObj.getTargetDirYear() + "/"
    						+ confObj.getTargetDirMonth() + "/"
    						+ confObj.getTargetDirDate() + "/"
    						+ confObj.getTargetDirHour()+"/"+confObj.getTargetDirMinute());
    				/*confObj.setSqoopTargetDir(confObj.getAppNameNode()
    						+ "/user/" + confObj.getQueueName() + "/"
    						+ confObj.getTableName() + "/landing/raw_data/"+ confObj.getTargetDirYear() + "/"
    	    						+ confObj.getTargetDirMonth() + "/"
    	    						+ confObj.getTargetDirDate() + "/"
    	    						+ confObj.getTargetDirHour()+"/"+confObj.getTargetDirMinute());*/
    				confObj.setWorkspacePath(confObj.getAppNameNode()
    						+ "/user/" + confObj.getQueueName() + "/workspace/HDI_"+confObj.getSourceName()+"_"+confObj.getTableOwner()+"_"+confObj.getTableName());
    				//confObj.setClusterUsername("cloudera");
    				//confObj.setClusterPassword("cloudera");
    				confObj.setSqoopColumns(properties.getProperty("sqoopColumns"));
                    confObj.setSqoopWhereClause(properties.getProperty("sqoopWhereClause"));
                    confObj.setMapColumnJava(properties.getProperty("mapColumnJava"));
                    confObj.setSqoopFileFormat(properties.getProperty("sqoopFileFormat"));
                     if(!(confObj.getSqoopFileFormat().contains("avro"))){
                    	 confObj.setHiveTextColumn(properties.getProperty("hiveTextColumns"));
                     }
                    	 
                    
                     confObj.setHiveTableName(properties.getProperty("hiveTableName"));
                    
                     
                     if(properties.getProperty("retention_period_raw_data").equalsIgnoreCase("")){
                    	 confObj.setHousekeepRequired(false);
                     }
                     else{
                    	 confObj.setHousekeepRequired(true);;
                     }
    			}
                	
                 else if(import_flag.equalsIgnoreCase("3")){
                	 confObj.setHiveTableName(properties.getProperty("hiveTableName"));
                	 confObj.setFileTrailerKeyword(properties.getProperty("FileTrailerKeyword"));
                	 confObj.setLineNumberData(properties.getProperty("lineNumberData"));
                 }
                else{
                confObj.setDirectMode(properties.getProperty("direct_flag"));
                confObj.setUpdate_mode(properties.getProperty("update_mode"));
                confObj.setUpdate_key_column(properties.getProperty("update_key_column"));
                confObj.setInput_null_string(properties.getProperty("input_null_string"));
                confObj.setInput_null_non_string(properties.getProperty("input-null-non-string"));
                confObj.setStaging_table(properties.getProperty("staging_table"));
                confObj.setClear_staging_table(properties.getProperty("clear_staging_table"));
                confObj.setBatch_mode(properties.getProperty("batch_mode"));
                confObj.setColumnDetails(properties.getProperty("selected_columns_name"));
                confObj.setAppNameNode(properties.getProperty("application_nameNode"));
                }
                 
				
			//}
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
