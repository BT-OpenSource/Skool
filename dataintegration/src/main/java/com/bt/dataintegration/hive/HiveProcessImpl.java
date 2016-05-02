/*
 * @Author: 609349708 (Abhinav Meghmala)
 */

package com.bt.dataintegration.hive;

import java.beans.Statement;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jline.internal.InputStreamReader;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class HiveProcessImpl implements IHiveProcess, Constants {
	
	private String hiveCreateQuery = "";
	private String partFileName = "";
	private String auditFileName = "";
	final static Logger logger = Logger.getLogger(HiveProcessImpl.class);	

	public String validateHiveTablePrivs(HadoopConfig hconf) {
		
		logger.info("***********************");
		logger.info("*****LOGS FOR HIVE*****");
		logger.info("***********************");
		
		ProcessBuilder pb = null;
		Process p = null;
		int x = 0;
		boolean status = false;
		logger.info("Preparing queries...");
		//String checkTable = "select * from " + hconf.getQueueName() + "." + hconf.getHiveTableName() + " limit 1";
		String checkTable = "describe " + hconf.getQueueName() + "." + hconf.getHiveTableName();
		String useDB = "use " + hconf.getQueueName();
		String dropTable = "";
		String createTable = "";		
		String addPartition = "";
		String newTableName =hconf.getHiveTableName();
		try{
			
			pb = new ProcessBuilder("hive","-e",checkTable);
			p = pb.start();
			x = p.waitFor();
			
			if(x == 0) {
				//logger.error("Error checking table @HIVE", new SQLException("Unable to check " + hconf.getHiveTableName() + " in Hive"));
				logger.warn("Table " + hconf.getHiveTableName() + " already exists in Hive.");
				String choice = "";
				logger.warn("Select an option \n y - Continue \t n - Hault Execution");
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				choice = reader.readLine();
				while((("y".equalsIgnoreCase(choice)) || ("n".equalsIgnoreCase(choice))) == false){
					choice = "";
					logger.warn("Select an option \n y - Continue \t n - Hault Execution");
					choice = reader.readLine();
				}
				String filename = "job.properties";
				if(choice.equalsIgnoreCase("y")) {
					
					logger.warn("Please provide new table name before proceeding");
					reader = new BufferedReader(new InputStreamReader(System.in));
					/////////
					newTableName = "";
					while((newTableName == null) || ("".equalsIgnoreCase(newTableName))){
						newTableName = reader.readLine();
						if((newTableName == null) || ("".equalsIgnoreCase(newTableName))){
							logger.warn("Table name cannot be null. Please provide new table name before proceeding");
							newTableName = "";
						}
						else{
							checkTable = "describe " + hconf.getQueueName() + "." + newTableName;
							pb = new ProcessBuilder("hive","-e",checkTable);
							p = pb.start();
							x = p.waitFor();
							if(x == 0){
								logger.warn("Hive table with name "+newTableName+" exists.Please provide another name.");
								newTableName="";
							}
						}
					}
					
					
					Properties prop = new Properties();
					prop.load(new FileInputStream(filename));
					prop.setProperty("hiveTableName", newTableName);
					prop.store(new FileOutputStream(filename), null);
					hconf.setHiveTableName(newTableName);
					reader.close();
				}
				
				else if(choice.equalsIgnoreCase("n")) {
					reader.close();
					logger.info("Cleaning workspace...");
					DirectoryHandler.cleanUpWorkspace(hconf);
					logger.info("Cleaning Landing...");
					DirectoryHandler.cleanUpLanding(hconf);
					throw new Error("Halting execution...");							
				}
			}
				logger.debug("Executing command: " + useDB);
				pb = new ProcessBuilder("hive","-e",useDB);
				p = pb.start();
				x = p.waitFor();
				
				if(x == 0) {
					logger.debug("Connected to database !");
					logger.debug("Creating table: " + hconf.getHiveTableName() + " in Hive...");
					createTable = queryBuilder(hconf);
					logger.debug("Executing command: " + createTable);
					pb = new ProcessBuilder("hive","-e",createTable);
					p = pb.start();
					x = p.waitFor();
					
					if(x == 0) {
						logger.debug("Command completed successfully. \n Adding partition...");
						addPartition = partitionBuilder(hconf);
						logger.debug("Executing command: " + addPartition);
						pb = new ProcessBuilder("hive","-e",addPartition);
						p = pb.start();
						x = p.waitFor();
						
						if(x == 0) {
							logger.debug("Command completed successfully !");
							try {								
								dropTable = "drop table " + hconf.getQueueName() + "." + hconf.getHiveTableName();
								pb = new ProcessBuilder("hive","-e",dropTable);
								p = pb.start();
								x = p.waitFor();
								
								if(x == 0) {
									logger.debug("Table dropped in Hive.");
									status = true;
								} else {
									logger.debug("failed to drop table in hive.");
									status = false;
								}
							} catch (Exception e2) {
								logger.error("Error @HIVE", new SQLException("Failed to drop table " + hconf.getHiveTableName()));
								//throw new Error("Failed to drop table in Hive");
							} 		
						} else {
							logger.debug("Command failed to add partition to Hive table " + hconf.getHiveTableName());
						}
					} else {
						logger.debug("Command failed to create table in Hive.");
					}
				} else {
					logger.debug("Command failed to use database " + hconf.getQueueName());
				}				
			
			
		} catch(Exception e) {
			logger.error("Error @Hive", e);
		} finally {
							
		}	
		if(status == true) {
			logger.info("Hive table priviliges successfully validated !");
		} else {
			logger.warn("Failed to either create/add partition to table " + hconf.getHiveTableName() + "\n Please check logs for more details.");
			logger.info("Workflow generation will not be affected. \n Application in progress...");						
		}	
		return newTableName;
	}	

	public String queryBuilder(HadoopConfig hconf) {
		
		String hiveDirectory = "/user/" + hconf.getQueueName() + "/landing/staging/"
				+ hconf.getSourceName()+"/"+ hconf.getTableOwner()+"/HDI_"
				+ hconf.getTableName();
		String query = "";
		if(hconf.getSqoopFileFormat().contains("avro")){
			query = "create external table if not exists " + hconf.getQueueName() + "."
					+ hconf.getHiveTableName()
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+ " ROW FORMAT SERDE " + "'" + SERDE_FORMAT + "'"
					+ " STORED AS INPUTFORMAT " + "'" + SERDE_IP_FORMAT + "'"
					+ " OUTPUTFORMAT " + "'" + SERDE_OP_FORMAT + "'" + " LOCATION '"
					+ /*hconf.getWorkflowNameNode() +*/ hiveDirectory + "'" + " TBLPROPERTIES " + "('"
					+ AVRO_SCHEMA_URL + "'='" /*+ hconf.getWorkflowNameNode()*/
					+ hconf.getWorkspacePath() + "/HDI_" + hconf.getTableName()
					+ ".avsc" + "')";
		}
		else if(hconf.getSqoopFileFormat().contains("text")){
			query = "create external table if not exists " + hconf.getQueueName() + "."
					+ hconf.getHiveTableName()+"("+hconf.getHiveTextColumn()+")"
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+ " LOCATION '"
					+ hiveDirectory + "'";
		}
		else if(hconf.getSqoopFileFormat().contains("parquet")){
			query = "create external table if not exists " + hconf.getQueueName() + "."
					+ hconf.getHiveTableName()+"("+hconf.getHiveTextColumn()+")"
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+" STORED AS PARQUET"
					+ " LOCATION '"
					+ hiveDirectory + "'";
		}
		return query;
	}	

	public String partitionBuilder(HadoopConfig hconf) {

		String query = "alter table " + hconf.getQueueName() + "." + hconf.getHiveTableName()
				+ " add if not exists partition (part_year='"
				+ hconf.getTargetDirYear() + "',part_month='"
				+ hconf.getTargetDirMonth() + "',part_day='"
				+ hconf.getTargetDirDate() + "',part_hour='"
				+ hconf.getTargetDirHour() + "',part_minute='"
				+ hconf.getTargetDirMinute() + "')" + " location '"
				+ /*hconf.getWorkflowNameNode() +*/ hconf.getTargetDirectory() + "'";

		return query;
	}
	
	public String partitionBuilderWorkflow(HadoopConfig hconf) {

		String query = "alter table ${hiveTableName} add if not exists partition (part_year='${targetDirYear}',"
				+ "part_month='${targetDirMonth}',part_day='${targetDirDate}',part_hour='${targetDirHour}',"
				+ "part_minute='${targetDirMinute}')" + " location '${hiveAddPart}'";

		return query;
	}

	public void writeHiveCreateQuery(String query, HadoopConfig hconf) {

		hiveCreateQuery = "HDI_"+hconf.getTableName() + "_CREATE_AVRO_TABLE.hql";
		File file = new File(hconf.getTableName()+"/"+hiveCreateQuery);
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(file);
			writer.println(query);
		} catch (Exception e) {

			logger.error("Unable to open file for write");
			logger.trace(e);
		} finally {

			if (writer != null)
				writer.close();
		}
	}

	public void writeHivePartitionQuery(String query, HadoopConfig hconf) {

		partFileName = "HDI_"+hconf.getTableName() + "_ADD_PARTITION.hql";
		File file = new File(hconf.getTableName()+"/"+partFileName);
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(file);
			writer.println(query);
		} catch (Exception e) {

			logger.error("Unable to open file for write");
			logger.trace(e);
		} finally {

			if (writer != null)
				writer.close();
		}
	}
	
	public void writeAuditTableCreateQuery(String query, HadoopConfig hconf) {
		
		auditFileName = "HDI_CREATE_AUDIT_TABLE.hql";
		File file = new File(hconf.getTableName()+"/"+auditFileName);
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(file);
			writer.println(query);
		} catch (Exception e) {

			logger.error("Unable to open file for write");
			logger.trace(e);
		} finally {

			if (writer != null)
				writer.close();
		}
	}
	
	public void copyHiveScriptsToHDFS(HadoopConfig hconf) {
		logger.info("Writing query to file...");	
		String wfCreateQuery = queryBuilderWorkflow(hconf);
		writeHiveCreateQuery(wfCreateQuery, hconf);
		
		String wfPartitionQuery = partitionBuilderWorkflow(hconf);
		writeHivePartitionQuery(wfPartitionQuery, hconf);
		
		String wfAuditQuery = createAuditTableQueryBuilder();
		writeAuditTableCreateQuery(wfAuditQuery, hconf);
		
		logger.info("Copying Hive Script files to HDFS Workspace...");
		DirectoryHandler.sendFileToHDFS(hconf, hiveCreateQuery);
		DirectoryHandler.sendFileToHDFS(hconf, partFileName);
		DirectoryHandler.sendFileToHDFS(hconf, auditFileName);
		
		//The below operations are handled in the Directory Handler class by sendFileToHDFS method
		/*String putCreateTable = "hadoop fs -put "
				+ hiveCreateQuery + " "
				+ hconf.getAppNameNode() + "/user/"
				+ hconf.getQueueName() + "/"
				+hconf.getTableName() + "/workspace";
		
		String addPartition = "hadoop fs -put "
				+ partFileName + " "
				+ hconf.getAppNameNode() + "/user/"
				+ hconf.getQueueName() + "/"
				+hconf.getTableName() + "/workspace";
		
		String putCreateAuditTable = "hadoop fs -put "
				+ auditFileName + " "
				+ hconf.getAppNameNode() + "/user/"
				+ hconf.getQueueName() + "/"
				+hconf.getTableName() + "/workspace";
		
		int shellout = 1;
		logger.info("Copying " + hiveCreateQuery + "to HDFS workspace...");
		logger.debug(putCreateTable);
		shellout = Utility.executeSSH(putCreateTable);
		if(shellout !=0){
			throw new Error();
		}
		
		logger.info("Copying " + partFileName + "to HDFS workspace...");
		logger.debug(addPartition);
		shellout = Utility.executeSSH(addPartition);
		if(shellout !=0){
			throw new Error();
		}
		
		logger.info("Copying " + putCreateAuditTable + "to HDFS workspace...");
		logger.debug(putCreateAuditTable);
		shellout = Utility.executeSSH(putCreateAuditTable);
		if(shellout !=0){
			throw new Error();
		}	*/	
	}	
	
	public String queryBuilderWorkflow(HadoopConfig hconf) {
		String workflowCreateQuery = "";
		if(hconf.getSqoopFileFormat().contains("avro")){
			workflowCreateQuery = "create external table if not exists "
					+ "${hiveTableName}"
					+ " COMMENT '" + " This table is created from source table " + hconf.getHiveTableName() + " under schema " + hconf.getUserName()
					+ "'"
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+ " ROW FORMAT SERDE " + "'" + SERDE_FORMAT + "'"
					+ " STORED AS INPUTFORMAT " + "'" + SERDE_IP_FORMAT + "'"
					+ " OUTPUTFORMAT " + "'" + SERDE_OP_FORMAT + "'" + " LOCATION "
					+ "'" + "${targetDirectory}" 
					+ "'" + " TBLPROPERTIES " + "('"
					+ AVRO_SCHEMA_URL + "'='" + "${wf_application_path}/HDI_${tableName}"
					+ ".avsc" + "')";
		}
		else if(hconf.getSqoopFileFormat().contains("text")){
			workflowCreateQuery = "create external table if not exists "
					+ "${hiveTableName}"+"(${hiveTextColumns})"
					+ " COMMENT '" + " This table is created from source table " + hconf.getHiveTableName() + " under schema " + hconf.getUserName()
					+ "'"
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+ " LOCATION '${targetDirectory}'" ;
		}
		else if(hconf.getSqoopFileFormat().contains("parquet")){
			workflowCreateQuery = "create external table if not exists "
					+ "${hiveTableName}"+"(${hiveTextColumns})"
					+ " COMMENT '" + " This table is created from source table " + hconf.getHiveTableName() + " under schema " + hconf.getUserName()
					+ "'"
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+" STORED AS PARQUET"
					+ " LOCATION '${targetDirectory}'" ;
		}
		return workflowCreateQuery;
	}
	
	public String createAuditTableQueryBuilder() {
		
		String createAuitTableQuery ="create external table if not exists ${audit_table_name} (${hdi_tab_cols}) row format delimited fields terminated by ',' location '${location}'";
		
		return createAuitTableQuery;
	}
	
	public void hiveScriptsFile(HadoopConfig conf){
		String createTableName="HDI_"+conf.getHiveTableName()+"_CREATE_TABLE.hql";
		String addPartName="HDI_"+conf.getHiveTableName()+"_ADD_PARTITION.hql";
		String auditTableFileName="HDI_CREATE_AUDIT_TABLE.hql";
		DirectoryHandler.createNewFile(conf, createTableName, createTableFile());
		DirectoryHandler.createNewFile(conf, addPartName, addPartFile());
		DirectoryHandler.createNewFile(conf, auditTableFileName, createAuditTableQueryBuilder());
		
		DirectoryHandler.sendFileToHDFS(conf, createTableName);
		DirectoryHandler.sendFileToHDFS(conf, addPartName);
		DirectoryHandler.sendFileToHDFS(conf, auditTableFileName);
		
		DirectoryHandler.givePermissionToHDFSFile(conf, createTableName);
		DirectoryHandler.givePermissionToHDFSFile(conf, addPartName);
		DirectoryHandler.givePermissionToHDFSFile(conf, auditTableFileName);
	}
	
	public String createTableFile(){
		String cmd = "create external table if not exists ${hiveTableName} ("+Utility.hiveTextColumns+") partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String) row format delimited fields terminated by '\\001' location '${targetDirectoryValid}'";
		
		return cmd;
	}
	
	public String addPartFile(){
		String cmd = "alter table ${hiveTableName} add if not exists partition (part_year='${dir_year}',part_month='${dir_month}',part_day='${dir_day}',part_hour='${dir_hour}',part_minute='${dir_minute}') location '${hiveAddPart}'";
		
		return cmd;
	}
}