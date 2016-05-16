/*
 * Author: 609298143 (Manish Bajaj)
 * 			609349708 (Abhinav Meghmala)
 */

package com.bt.dataintegration.property.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import jline.internal.InputStreamReader;

import org.apache.log4j.Logger;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.utilities.Utility;

//import com.bt.tab.hadoop.hive.jdbc.test.HiveConnect;

public class DIConfig implements Constants{
	final static Logger logger = Logger.getLogger(DIConfig.class);
	private String envDetails;
	private String tableName;
	private String query;
	private List<String> columnDetails;
	private List<String> maskCols;
	private String sourceHostName;
	private String sourcePort;
	private String sourceSid;
	private String sourceUsername;
	private String sourcePassword;
	private String DBDriver;
	private String tableOwner;
	private int RDBMSDetail; // 1- Oracle and 2- MySQL
	private boolean DBCatalog;
	private String wfStartTime;
	private String wfEndTime;
	private String timeZone;
	private String successEmailId;
	private String failureEmailId;
	private boolean direct;
	private String lastModifiedDateColumn;
	private String appNameNode;
	private String workflowNameNode;
	private String hive2_jdbc_url;
	private String hive2_server_principal;
	private String jobTracker;
	private String sqoopFileFormat;
	private String sourceName;
	
	private String hiveJdbcDriver;
	private String connString;
	private String serviceName;
	private String clusterUsername;
	private String clusterPassword;
	//private String landingDirectory;
	//private String workspaceDirectory;
	private boolean coordinatorFlag;
	private String instanceName;
	
	private boolean incremental;
	private String incrementalMode;
	private String checkColumn;
	private String lastValue;
	private String splitByColumn;
	private int numOfMapper;
	private int retentionRawData;
	private int retentionProcessedData;
	//For Table Export
	
	private String exportDir;
	private String updateKeyColumn ; 
	private String updateMode;
	private String input_null_string ;
	private String input_null_non_string ;
	private String stagingTable;   
	private String import_export_flag;
	private boolean stagingRequired;
	private boolean updateDatabase;
	private String fieldSeparator;
	private String lineSeparator;
	//private String direct_flag ;
	//private String num_mappers;  
	//private String stored_procedure_name ; 
	//private String clear_staging_table;   
	//private String batch_mode;
	//private String columns_name ;
	
	
	
	private String concurrency ;
	private String throttle ;
	private String frequency ;
	private String timeout ;
	
	//attributes required for Filesystem
	private String hiveTable;
	private String fileDirectory;
	private String fileDelimeter;
	private String fileMask;
	private String fileDateFormat;
	private double threshold;
	private String controlFileName;
	private String mappingSheetname;
	private boolean fileTrailerPresent;
	private String fileTrailerKeyword;
	private String fileHeaderKeyword;
	private int lineNumberData;
	private String interimLandingDir;
	private String landingDirectory;
	public DIConfig() {
		super();
	}
	



	public String getLandingDirectory() {
		return landingDirectory;
	}




	public void setLandingDirectory(String landingDirectory) {
		this.landingDirectory = landingDirectory;
	}




	public String getInterimLandingDir() {
		return interimLandingDir;
	}




	public void setInterimLandingDir(String interimLandingDir) {
		this.interimLandingDir = interimLandingDir;
	}




	public String getFileHeaderKeyword() {
		return fileHeaderKeyword;
	}


	public void setFileHeaderKeyword(String fileHeaderKeyword) {
		this.fileHeaderKeyword = fileHeaderKeyword;
	}


	public boolean isFileTrailerPresent() {
		return fileTrailerPresent;
	}


	public void setFileTrailerPresent(boolean fileTrailerPresent) {
		this.fileTrailerPresent = fileTrailerPresent;
	}


	public String getFileTrailerKeyword() {
		return fileTrailerKeyword;
	}

	public int getLineNumberData() {
		return lineNumberData;
	}

	public void setFileTrailerKeyword(String fileTrailerKeyword) {
		this.fileTrailerKeyword = fileTrailerKeyword;
	}

	public void setLineNumberData(int lineNumberData) {
		this.lineNumberData = lineNumberData;
	}

	public String getMappingSheetname() {
		return mappingSheetname;
	}

	public void setMappingSheetname(String mappingSheetname) {
		this.mappingSheetname = mappingSheetname;
	}

	public String getImport_export_flag() {
		return import_export_flag;
	}

	public void setImport_export_flag(String import_export_flag) {
		this.import_export_flag = import_export_flag;
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
	
	public String getExportDir() {
		return exportDir;
	}

	public void setExportDir(String exportDir) {
		this.exportDir = exportDir;
	}

	public String getUpdateKeyColumn() {
		return updateKeyColumn;
	}

	public void setUpdateKeyColumn(String updateKeyColumn) {
		this.updateKeyColumn = updateKeyColumn;
	}

	public String getUpdateMode() {
		return updateMode;
	}

	public void setUpdateMode(String updateMode) {
		this.updateMode = updateMode;
	}

	public String getStagingTable() {
		return stagingTable;
	}

	public void setStagingTable(String stagingTable) {
		this.stagingTable = stagingTable;
	}

	
	public String getTableName() {
		return tableName;
	}

	public String getQuery() {
		return query;
	}

	public List<String> getColumnDetails() {
		return columnDetails;
	}

	public List<String> getMaskCols() {
		return maskCols;
	}

	public String getSourceHostName() {
		return sourceHostName;
	}

	public String getSourcePort() {
		return sourcePort;
	}

	public String getSourceSid() {
		return sourceSid;
	}

	public String getHiveJdbcDriver() {
		return hiveJdbcDriver;
	}

	public String getConnString() {
		return connString;
	}

	public String getServiceName() {
		return serviceName;
	}

	public String getClusterUsername() {
		return clusterUsername;
	}

	public String getClusterPassword() {
		return clusterPassword;
	}


	public String getInstanceName() {
		return instanceName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setColumnDetails(List<String> columnDetails) {
		this.columnDetails = columnDetails;
	}

	public void setMaskCols(List<String> maskCols) {
		this.maskCols = maskCols;
	}

	public void setSourceHostName(String sourceHostName) {
		this.sourceHostName = sourceHostName;
	}

	public void setSourcePort(String sourcePort) {
		this.sourcePort = sourcePort;
	}

	public void setSourceSid(String sourceSid) {
		this.sourceSid = sourceSid;
	}

	public void setHiveJdbcDriver(String hiveJdbcDriver) {
		this.hiveJdbcDriver = hiveJdbcDriver;
	}

	public void setConnString(String connString) {
		this.connString = connString;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public void setClusterUsername(String clusterUsername) {
		this.clusterUsername = clusterUsername;
	}

	public void setClusterPassword(String clusterPassword) {
		this.clusterPassword = clusterPassword;
	}


	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}
	public void setSourceUsername(String sourceUsername) {
		this.sourceUsername = sourceUsername;
	}
	public String getSourceUsername() {
		return this.sourceUsername;
	}
	public void setSourcePassword(String sourcePassword) {
		this.sourcePassword = sourcePassword;
	}
	public String getSourcePassword() {
		return this.sourcePassword;
	}
	
	
	public boolean isIncremental() {
		return incremental;
	}

	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}

	public String getIncrementalMode() {
		return incrementalMode;
	}

	public void setIncrementalMode(String incrementalMode) {
		this.incrementalMode = incrementalMode;
	}

	public String getCheckColumn() {
		return checkColumn;
	}

	public void setCheckColumn(String checkColumn) {
		this.checkColumn = checkColumn;
	}

	

	public String getLastValue() {
		return lastValue;
	}

	public void setLastValue(String lastValue) {
		this.lastValue = lastValue;
	}

	public String getSplitByColumn() {
		return splitByColumn;
	}

	public void setSplitByColumn(String splitByColumn) {
		this.splitByColumn = splitByColumn;
	}

	public int getNumOfMapper() {
		return numOfMapper;
	}

	public void setNumOfMapper(int numOfMapper) {
		this.numOfMapper = numOfMapper;
	}

	
	public String getDBDriver() {
		return DBDriver;
	}

	public void setDBDriver(String dBDriver) {
		DBDriver = dBDriver;
	}

	public int getRDBMSDetail() {
		return RDBMSDetail;
	}

	public void setRDBMSDetail(int rDBMSDetail) {
		RDBMSDetail = rDBMSDetail;
	}

	public boolean isDBCatalog() {
		return DBCatalog;
	}

	public void setDBCatalog(boolean dBCatalog) {
		DBCatalog = dBCatalog;
	}

	
	public String getWfStartTime() {
		return wfStartTime;
	}

	public void setWfStartTime(String wfStartTime) {
		this.wfStartTime = wfStartTime;
	}

	public String getWfEndTime() {
		return wfEndTime;
	}

	public void setWfEndTime(String wfEndTime) {
		this.wfEndTime = wfEndTime;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	public String getSuccessEmailId() {
		return successEmailId;
	}

	public void setSuccessEmailId(String successEmailId) {
		this.successEmailId = successEmailId;
	}

	public String getFailureEmailId() {
		return failureEmailId;
	}

	public void setFailureEmailId(String failureEmailId) {
		this.failureEmailId = failureEmailId;
	}

	
	public boolean isDirect() {
		return direct;
	}

	public void setDirect(boolean direct) {
		this.direct = direct;
	}

	
	public String getLastModifiedDateColumn() {
		return lastModifiedDateColumn;
	}

	public void setLastModifiedDateColumn(String lastModifiedDateColumn) {
		this.lastModifiedDateColumn = lastModifiedDateColumn;
	}
	
	

	public String getEnvDetails() {
		return envDetails;
	}

	public void setEnvDetails(String envDetails) {
		this.envDetails = envDetails;
	}


	public String getHive2_jdbc_url() {
		return hive2_jdbc_url;
	}

	public void setHive2_jdbc_url(String hive2_jdbc_url) {
		this.hive2_jdbc_url = hive2_jdbc_url;
	}

	public String getHive2_server_principal() {
		return hive2_server_principal;
	}

	public void setHive2_server_principal(String hive2_server_principal) {
		this.hive2_server_principal = hive2_server_principal;
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

	public String getJobTracker() {
		return jobTracker;
	}

	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	public String getSqoopFileFormat() {
		return sqoopFileFormat;
	}

	public void setSqoopFileFormat(String sqoopFileFormat) {
		this.sqoopFileFormat = sqoopFileFormat;
	}

	public String getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(String concurrency) {
		this.concurrency = concurrency;
	}

	public String getThrottle() {
		return throttle;
	}

	public void setThrottle(String throttle) {
		this.throttle = throttle;
	}

	public String getTimeout() {
		return timeout;
	}

	public void setTimeout(String timeout) {
		this.timeout = timeout;
	}

	public String getFrequency() {
		return frequency;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	

	public boolean isCoordinatorFlag() {
		return coordinatorFlag;
	}

	public void setCoordinatorFlag(boolean coordinatorFlag) {
		this.coordinatorFlag = coordinatorFlag;
	}

	
	public int getRetentionRawData() {
		return retentionRawData;
	}

	public void setRetentionRawData(int retentionRawData) {
		this.retentionRawData = retentionRawData;
	}

	public int getRetentionProcessedData() {
		return retentionProcessedData;
	}

	public void setRetentionProcessedData(int retentionProcessedData) {
		this.retentionProcessedData = retentionProcessedData;
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

	
	public boolean isStagingRequired() {
		return stagingRequired;
	}

	public void setStagingRequired(boolean stagingRequired) {
		this.stagingRequired = stagingRequired;
	}

	public boolean isUpdateDatabase() {
		return updateDatabase;
	}

	public void setUpdateDatabase(boolean updateDatabase) {
		this.updateDatabase = updateDatabase;
	}

	public String getFieldSeparator() {
		return fieldSeparator;
	}

	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}
	
	public String getHiveTable() {
		return hiveTable;
	}

	public void setHiveTable(String hiveTable) {
		this.hiveTable = hiveTable;
	}

	public String getFileDirectory() {
		return fileDirectory;
	}

	public void setFileDirectory(String fileDirectory) {
		this.fileDirectory = fileDirectory;
	}

	public String getFileDelimeter() {
		return fileDelimeter;
	}

	public void setFileDelimeter(String fileDelimeter) {
		this.fileDelimeter = fileDelimeter;
	}

	public String getFileMask() {
		return fileMask;
	}

	public void setFileMask(String fileMask) {
		this.fileMask = fileMask;
	}
	
	

	public double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}

	
	public String getFileDateFormat() {
		return fileDateFormat;
	}

	public void setFileDateFormat(String fileDateFormat) {
		this.fileDateFormat = fileDateFormat;
	}
	
	public String getControlFileName() {
		return controlFileName;
	}

	public void setControlFileName(String controlFileName) {
		this.controlFileName = controlFileName;
	}

	public DIConfig getDIConfigProperties() {
		
		Properties properties = null;
		InputStream ips = null;
		//final String conffileName = "configuration.properties"; 
		DIConfig confObj = new DIConfig();
		
		try {
			
			properties = new Properties();
			//ips = DIConfig.class.getClassLoader().getResourceAsStream(conffileName);
			ips = new FileInputStream(CONFIGURATION_PROPERTIES_FILE);
			properties.load(ips);
			
		} catch (Exception e) {
			logger.error("Error loading properties");
			logger.error(e);
			throw new Error(e);
			
		}

			confObj.setImport_export_flag(properties.getProperty("import_export_file_flag"));
			if(confObj.getImport_export_flag() == null || "".equals(confObj.getImport_export_flag())){
				logger.error("please provide import_export_flag");
				throw new Error();
			}	
			
			try{
				confObj.setInstanceName(properties.getProperty("hdfs_instance_name").toUpperCase());
				}catch(NullPointerException e){
					logger.error("please provide hdfs_instance_name");
					throw new Error(e);
				}
			if("".equals(confObj.getInstanceName())){
				logger.error("please provide cluster_haas_instance_name");
				throw new Error();
			}
			
			confObj.setSourceName(properties.getProperty("source_name"));
			if(confObj.getSourceName() == null || "".equals(confObj.getSourceName())){
				logger.error("please provide source_name");
				throw new Error();
			}
			
			confObj.setConcurrency(properties.getProperty("concurrency"));
            if(confObj.getConcurrency() == null || "".equals(confObj.getConcurrency())){
                           logger.info("By default setting concurrency to 1");
                           confObj.setConcurrency("1");
            }
            confObj.setThrottle(properties.getProperty("throttle"));
            if(confObj.getThrottle() == null || "".equals(confObj.getThrottle())){
                           logger.info("By default setting throttle to 1");
                           confObj.setThrottle("1");
            }
            confObj.setTimeout(properties.getProperty("timeout"));
            if(confObj.getTimeout() == null || "".equals(confObj.getTimeout())){
                           logger.info("By default setting timeout to 0");
                           confObj.setTimeout("0");
            }
            confObj.setFrequency(properties.getProperty("frequency"));
            if(confObj.getFrequency() == null || "".equals(confObj.getFrequency())){
                           logger.info("By default setting frequency to 14400");
                           confObj.setFrequency("1440");
            }
            String coordFlag = properties.getProperty("coordinator_required");
            if(coordFlag == null || "".equals(coordFlag)){
                           logger.info("By default setting coordinator_required to false");
                           confObj.setCoordinatorFlag(false);
            }
            else{
            	if("true".equalsIgnoreCase(coordFlag))
            		confObj.setCoordinatorFlag(true);
            	else if("false".equalsIgnoreCase(coordFlag))
            		confObj.setCoordinatorFlag(false);
            }
            
            /*confObj.setAppNameNode(properties.getProperty("appNameNode"));
			if(confObj.getAppNameNode() == null || "".equals(confObj.getAppNameNode())){
				logger.error("please provide appNameNode");
				throw new Error();
			}
			confObj.setWorkflowNameNode(properties.getProperty("workflowNameNode"));
			if(confObj.getWorkflowNameNode() == null || "".equals(confObj.getWorkflowNameNode())){
				logger.error("please provide workflowNameNode");
				throw new Error();
			}*/
            confObj.setAppNameNode(properties.getProperty("NameNode"));
			if(confObj.getAppNameNode() == null || "".equals(confObj.getAppNameNode())){
				logger.error("please provide NameNode");
				throw new Error();
			}
			confObj.setWorkflowNameNode(confObj.getAppNameNode());
			
			if(confObj.isCoordinatorFlag()){
				confObj.setWfStartTime(properties.getProperty("workflow_start_time"));
				if(confObj.getWfStartTime() == null || "".equals(confObj.getWorkflowNameNode())){
					logger.error("please provide workflow_start_time");
					throw new Error();
				}
				
				confObj.setWfEndTime(properties.getProperty("workflow_end_time"));
				if(confObj.getWfEndTime() == null || "".equals(confObj.getWorkflowNameNode())){
					logger.error("please provide workflow_end_time");
					throw new Error();
				}
				
				confObj.setTimeZone(properties.getProperty("time_zone"));
				if(confObj.getTimeZone() == null || "".equals(confObj.getTimeZone())){
					logger.error("please provide time_zone");
					throw new Error();
				}
			}
			confObj.setSuccessEmailId(properties.getProperty("success_email_id"));
			if(confObj.getSuccessEmailId() == null || "".equals(confObj.getSuccessEmailId())){
				logger.error("please provide success_email_id");
				throw new Error();
			}
			
			confObj.setFailureEmailId(properties.getProperty("failure_email_id"));
			if(confObj.getFailureEmailId() == null || "".equals(confObj.getFailureEmailId())){
				logger.error("please provide failure_email_id");
				throw new Error();
			}
			
			confObj.setJobTracker(properties.getProperty("jobTracker"));
			if(confObj.getJobTracker() == null || "".equals(confObj.getJobTracker())){
				logger.error("please provide jobTracker");
				throw new Error();
			}
			
			 confObj.setEnvDetails(properties.getProperty("Environment_Details"));
				if(confObj.getEnvDetails() == null || "".equals(confObj.getEnvDetails())){
					logger.info("By default environement is considered to be cluster");
					confObj.setEnvDetails("1");
				}
				
				confObj.setHive2_jdbc_url(properties.getProperty("hive2_jdbc_url"));
				if(confObj.getHive2_jdbc_url() == null || "".equals(confObj.getHive2_jdbc_url())){
					logger.error("please provide hive2_jdbc_url");
					throw new Error();
				}
				confObj.setHive2_server_principal(properties.getProperty("hive2_server_principal"));
				if(confObj.getHive2_server_principal() == null || "".equals(confObj.getHive2_server_principal())){
					logger.error("please provide hive2_server_principal");
					throw new Error();
				}
				
				confObj.setHiveJdbcDriver(HIVE_JDBC_DRIVER_NAME);
				
				confObj.setInterimLandingDir(properties.getProperty("interim_landing_directory"));
				String landingDirApp = "";
				
				
			//***************common properties set for both import and export********************//
			
            if(!confObj.getImport_export_flag().equalsIgnoreCase("3")){
            	
            	String interimDir = confObj.getInterimLandingDir();
				if("".equals(interimDir)){
					landingDirApp=confObj.getAppNameNode()+"/user/"+confObj.getInstanceName()+"/"+confObj.getSourceName()+"/"+confObj.getTableOwner()+"/HDI_"+confObj.getTableName();
				}else{
					landingDirApp=confObj.getAppNameNode()+"/user/"+confObj.getInstanceName()+"/"+interimDir+"/"+confObj.getSourceName()+"/"+confObj.getTableOwner()+"/HDI_"+confObj.getTableName();
				}
            	confObj.setLandingDirectory(landingDirApp);
				confObj.setSourceHostName(properties.getProperty("database_host"));
				if(confObj.getSourceHostName() == null || "".equals(confObj.getSourceHostName())){
					logger.error("please provide source_database_host");
					throw new Error();
				}	
				
				confObj.setSourcePort(properties.getProperty("datebase_port"));
				if(confObj.getSourcePort() == null || "".equals(confObj.getSourcePort())){
					logger.error("please provide source_datebase_port");
					throw new Error();
				}
				
				try{
					confObj.setSourceSid(properties.getProperty("database_sid_or_servicename").toUpperCase());
				}
				catch(NullPointerException e){
					logger.error("please provide source_database_sid");
					throw new Error(e);
				}
				if("".equals(confObj.getSourceSid())){
					logger.error("please provide source_database_sid");
					throw new Error();
				}
				try{
				confObj.setSourceUsername(properties.getProperty("database_username").toUpperCase());
				}catch(NullPointerException e){
					logger.error("please provide source_database_username");
					throw new Error(e);
				}
				if("".equals(confObj.getSourceUsername())){
					logger.error("please provide source_database_username");
					throw new Error();
				}
				try{
					confObj.setTableOwner(properties.getProperty("database_schemaname").toUpperCase());
				}catch(NullPointerException e){
					logger.error("please provide source_database_schemaname");
					throw new Error(e);
				}
				if("".equals(confObj.getTableOwner())){
					logger.error("please provide source_database_schemaname");
					throw new Error();
				}
				
				// Password will be taken form the user and will be stored in encrypted fashion in HDFS
				/*confObj.setSourcePassword(properties.getProperty("database_password"));
				if(confObj.getSourcePassword() == null || "".equals(confObj.getSourcePassword())){
					logger.error("please provide source_database_password");
					throw new Error();
				}*/				
				
				try{
				confObj.setTableName(properties.getProperty("database_tablename").toUpperCase());
				}catch(NullPointerException e){
					logger.error("please provide source_database_tablename");
					throw new Error(e);
				}
				
				if("".equals(confObj.getTableName())){
					logger.error("please provide database_tablename");
					throw new Error();
				}
				
				confObj.setDBDriver(properties.getProperty("DBDriver"));
				if(confObj.getDBDriver() == null || "".equals(confObj.getDBDriver())){
					logger.error("please provide DBDriver");
					throw new Error();
				}
				try{
				confObj.setNumOfMapper(Integer.parseInt(properties.getProperty("no_of_mappers")));
				}catch(NullPointerException e){
					/*logger.error("please provide split_by_column");
					throw new Error(e);*/
					//logger.info("by default");
					confObj.setNumOfMapper(0);
				}
				catch(IllegalArgumentException e){
					if(!(properties.getProperty("no_of_mappers")).equalsIgnoreCase("")){
						logger.error("Please provide number for mappers in integers. To have default value leave the field blank");
						logger.error("",e);
						throw new Error(e);
					}
					else{
						confObj.setNumOfMapper(0);
					}
						
				}
				if("".equals(confObj.getNumOfMapper())){
					confObj.setNumOfMapper(0);
				}
				
				try{
					confObj.setRDBMSDetail(Integer.parseInt(properties.getProperty("RDBMS")));
				}catch(NullPointerException e){
						logger.error("please provide RDBMS detail");
						throw new Error(e);
				}
				if("".equals(confObj.getRDBMSDetail())){
					logger.error("please provide RDBMS detail");
					throw new Error();
				}
				
				String fileFormat = properties.getProperty("FileFormat");
				if(fileFormat == null || "".equals(fileFormat)){
					logger.info("By default keeping the file format as avro");
					fileFormat = "1";
				}
				if(fileFormat.equalsIgnoreCase("1")){
					confObj.setSqoopFileFormat("avrodatafile");
				}
				else if(fileFormat.equalsIgnoreCase("2")){
					confObj.setSqoopFileFormat("textfile");
				}
				else if(fileFormat.equalsIgnoreCase("3")){
					confObj.setSqoopFileFormat("sequencefile");
				}
				else if(fileFormat.equalsIgnoreCase("4")){
					confObj.setSqoopFileFormat("parquetfile");
				}
				
	           /* try{
	    			confObj.setRetentionProcessedData(Integer.parseInt(properties.getProperty("retention_period_processed_data")));
	    		}catch(NullPointerException e){
					logger.error("please provide split_by_column");
					throw new Error(e);
					//logger.info("by default");
					confObj.setRetentionProcessedData(-1);
	    		}
	            catch(IllegalArgumentException e){
	            	if(!(properties.getProperty("retention_period_processed_data")).equalsIgnoreCase("")){
		            	logger.error("Please provide retention_period_processed_data in integer");
		            	logger.error("",e);
		            	throw new Error(e);
		            }else{
		        		confObj.setRetentionProcessedData(-1);
		        	}
	            }*/
	            
	            
				//properties only for import
				if(confObj.getImport_export_flag().equalsIgnoreCase("1")){
					
					try{
					confObj.setSplitByColumn(properties.getProperty("split_by_column").toUpperCase());
					}catch(NullPointerException e){
						/*logger.error("please provide split_by_column");
						throw new Error(e);*/
						confObj.setSplitByColumn("");
					}
					
					try{
					confObj.setLastModifiedDateColumn(properties.getProperty("last_modified_date_column").toUpperCase());
					}catch(NullPointerException e){
						logger.error("please provide last_modified_date_column");
						throw new Error(e);
					}
					if("".equals(confObj.getLastModifiedDateColumn())){
						logger.warn("As last_modified_date_column is empty. The full table will be imported each time.");
						//throw new Error();
					}
					
					String userSelectedCols = null;
					try{
					userSelectedCols = properties.getProperty("user_selected_columns").toUpperCase();
					}catch(NullPointerException e){
						//logger.info("please provide user_selected_columns");
						userSelectedCols = "";
					}
					
					String[] userColumns = userSelectedCols.split(",");
					columnDetails = new LinkedList<String>();
					for(String col : userColumns){
						columnDetails.add(col);
					}
					confObj.setColumnDetails(columnDetails);
				
					
					
					try{
		    			confObj.setRetentionRawData(Integer.parseInt(properties.getProperty("retention_period_raw_data")));
		    		}catch(NullPointerException e){
						/*logger.error("please provide split_by_column");
						throw new Error(e);*/
						//logger.info("by default");
						confObj.setRetentionRawData(-1);
		    		}
		            catch(IllegalArgumentException e){
		            	if(!(properties.getProperty("retention_period_raw_data")).equalsIgnoreCase("")){
			            	logger.error("Please provide retention_period_raw_data in integer");
			            	logger.error("",e);
			            	throw new Error(e);
		            	}else{
		            		confObj.setRetentionRawData(-1);
		            	}
		            }
					
					if("".equals(confObj.getRetentionRawData())){
						confObj.setRetentionRawData(-1);
					}
					
					
				}
				if(confObj.getImport_export_flag().equalsIgnoreCase("2")){
					
					 String stagingFlag = properties.getProperty("push_to_staging_table");
			            if(stagingFlag == null || "".equals(stagingFlag)){
			                logger.info("By default setting push_to_staging_table to false");
			                confObj.setStagingRequired(false);
			            }
			            else{
			            	if("true".equalsIgnoreCase(stagingFlag)){
			            		confObj.setStagingRequired(true);
			            		try{
			            			confObj.setStagingTable(properties.getProperty("staging_table").toUpperCase());
			            			}
			            			catch(NullPointerException e){
			            				logger.error("please provide staging_table");
			            				throw new Error(e);
			            			}
			            	}
			            	else if("false".equalsIgnoreCase(stagingFlag))
			            		confObj.setStagingRequired(false);
			            }
			            
			            try{
	            			confObj.setExportDir(properties.getProperty("export_dir").toUpperCase());
	            		}
	            		catch(NullPointerException e){
	           				logger.error("please provide export directory");
	           				throw new Error(e);
	            		} 
			            if("".equals(confObj.getExportDir())){
			            	logger.error("please provide export directory");
	           				throw new Error();
			            }
			            String updateFlag = properties.getProperty("update_database");
			            if(updateFlag == null || "".equals(updateFlag)){
			                logger.info("By default setting update_database to false");
			                confObj.setUpdateDatabase(false);
			            }
			            else{
			            	if("true".equalsIgnoreCase(updateFlag)){
			            		confObj.setUpdateDatabase(true);
			            		try{
			            			confObj.setUpdateKeyColumn(properties.getProperty("update_key_column").toUpperCase());
			            			}
			            			catch(NullPointerException e){
			            				logger.error("please provide update_key_column");
			            				throw new Error(e);
			            			}
			            		try{
			            			confObj.setUpdateMode(properties.getProperty("update_mode").toUpperCase());
			            			}
			            			catch(NullPointerException e){
			            				logger.error("please provide update_mode");
			            				throw new Error(e);
			            			}
			            	}
			            	else if("false".equalsIgnoreCase(updateFlag))
			            		confObj.setUpdateDatabase(false);
			            }
			            
			            confObj.setFieldSeparator(properties.getProperty("fields_terminated_by"));
						if(confObj.getFieldSeparator() == null || "".equalsIgnoreCase(confObj.getFieldSeparator())){
							logger.info("By default fields_terminated_by is set to be , (comma)");
							confObj.setFieldSeparator(",");
						}
						
						confObj.setLineSeparator(properties.getProperty("lines_terminated_by"));
						if(confObj.getLineSeparator() == null || "".equalsIgnoreCase(confObj.getLineSeparator())){
							logger.info("By default lines_terminated_by is set to be cluster");
							confObj.setLineSeparator("\n");
						}
						
					//confObj.setClear_staging_table(properties.getProperty("clear_staging-table"));
					//confObj.setBatch_mode(properties.getProperty("batch_mode"));
					//confObj.setStored_procedure_name(properties.getProperty("stored_procedure_name"));
					//confObj.setDirect_flag(properties.getProperty("direct_flag"));//
					//confObj.setColumns_name(properties.getProperty("columns_name"));
				}
            }
            else{
            	String baseDir = properties.getProperty("file_base_directory");
				if(baseDir == null || "".equals(baseDir)){
					logger.error("please provide file_base_directory");
					throw new Error();
				}
				else{
					if(!baseDir.endsWith("/")){
						baseDir = baseDir+"/";
					}
					confObj.setFileDirectory(baseDir);
				}
				
				confObj.setFileDateFormat(properties.getProperty("file_date_format"));
				if(confObj.getFileDateFormat()== null || "".equals(confObj.getFileDateFormat())){
					logger.warn("file_date_format is not set. So by default file_date_format is set to yyyy-MM-dd HH:mm:ss");
					confObj.setFileDateFormat("yyyy-MM-dd HH:mm:ss");
				}
				
				String delimiter = (properties.getProperty("file_delimiter"));
				if(delimiter == null || "".equals(delimiter)){
					logger.error("please provide file_delimiter");
					throw new Error();
				}
				String x ="";
				for(char c : delimiter.toCharArray()){
					x = x+"\\\\"+c;
				}
				
				confObj.setFileDelimeter(x);
				Utility.delimiter = x;
				confObj.setHiveTable(properties.getProperty("file_hive_table_name"));
				if(confObj.getHiveTable() == null || "".equals(confObj.getHiveTable())){
					logger.error("please provide file_hive_table_name");
					throw new Error();
				}
				
				/*String threshold = properties.getProperty("record_threshold");
				if(threshold == null){
					logger.info("record_threshold not provided so by default threshold is set to 100%");
					threshold = "0";
				}
				try{
					confObj.setThreshold(Double.parseDouble(threshold));
				}
				catch(NumberFormatException e){
					logger.error("Please provide record_threshold in decimal values only.");
				}*/
				
				confObj.setFileMask(properties.getProperty("file_mask"));
				confObj.setControlFileName(properties.getProperty("control_file_name"));
				confObj.setMappingSheetname(properties.getProperty("mapping_sheet_name"));
				if(confObj.getFileMask() == null || "".equals(confObj.getFileMask())){
					logger.warn("file_mask is not provided. This means all the files in the base directory will be processed at once.");
					logger.warn("Select an option \n y - Continue \t n - Hault Execution");
					BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
					try {
						String choice = reader.readLine();
						while((("y".equalsIgnoreCase(choice)) || ("n".equalsIgnoreCase(choice))) == false){
							choice = "";
							logger.warn("Select an option \n y - Continue \t n - Hault Execution");
							choice = reader.readLine();
							
						}
						if("n".equalsIgnoreCase(choice)){
							throw new Error();
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						logger.error(e);
						throw new Error();
					}
				}
				try{
					confObj.setLineNumberData(Integer.parseInt(properties.getProperty("line_number")));
					}catch(NullPointerException e){
						logger.error("please provide line_number");
						throw new Error(e);
					}
					catch(IllegalArgumentException e){
						logger.error("Please provide number for line_number in integers.");
						logger.error("",e);
						throw new Error(e);
					}
					if("".equals(confObj.getNumOfMapper())){
						logger.warn("line_number is taken as 0 by default.");
						confObj.setLineNumberData(0);
					}
					
				    String ftPresent = properties.getProperty("file_trailer_present");
					if(ftPresent == null || "".equals(ftPresent)){
						logger.error("please provide file_trailer_present");
						throw new Error();
					}
					else{
						if("true".equalsIgnoreCase(ftPresent)){
							confObj.setFileTrailerPresent(true);
						}
						else if("false".equalsIgnoreCase(ftPresent)){
							confObj.setFileTrailerPresent(false);
						}
						else{
							logger.error("provide file_trailer_present as only true/false");
							throw new Error();
						}
					}
					if(confObj.isFileTrailerPresent()){
						confObj.setFileTrailerKeyword(properties.getProperty("file_trailer_keyword"));
						if(confObj.getFileTrailerKeyword() == null || "".equals(confObj.getFileTrailerKeyword())){
							logger.error("please provide file_trailer_keyword");
							throw new Error();
						}
					}
					else{
						confObj.setFileTrailerKeyword("FILETRAILER_NOT_APPLICABLE");
					}
					confObj.setFileHeaderKeyword(properties.getProperty("file_header"));
					if(confObj.getFileHeaderKeyword() == null || "".equals(confObj.getFileHeaderKeyword())){
						confObj.setFileHeaderKeyword("FILEHEADER_NOT_APPLICABLE");
					}
            }
			
		return confObj;
	}
}
