/*
 * @Author: 609349708 (Abhinav Meghmala)
 */

package com.bt.dataintegration.hive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import jline.internal.InputStreamReader;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.log4j.Logger;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

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
		
		String hiveDirectory = hconf.getLandingDirectory();
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

		hiveCreateQuery = "HDI_"+hconf.getTableName() + "_CREATE_TABLE.hql";
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
		
		String createAuitTableQuery ="create external table if not exists ${audit_table_name} (${hdi_tab_cols}) partitioned by (table_name String) row format delimited fields terminated by ',' location '${location}';\n"+
		"alter table ${audit_table_name} add if not exists partition (table_name = '${table_name}') location '${location}/${table_name}';";
		
		return createAuitTableQuery;
	}
	
	public void hiveScriptsFile(HadoopConfig conf){
		String createTableName="HDI_"+conf.getHiveTableName()+"_CREATE_TABLE.hql";
		String addPartName="HDI_"+conf.getHiveTableName()+"_ADD_PARTITION.hql";
		String auditTableFileName="HDI_CREATE_AUDIT_TABLE.hql";
		DirectoryHandler.createNewFile(conf, auditTableFileName, createAuditTableQueryBuilder());
		DirectoryHandler.sendFileToHDFS(conf, auditTableFileName);
		DirectoryHandler.givePermissionToHDFSFile(conf, auditTableFileName);
		
		if(!SQOOP_EXPORT.equalsIgnoreCase(conf.getImport_export_flag())){
			DirectoryHandler.createNewFile(conf, createTableName, createTableFile());
			DirectoryHandler.createNewFile(conf, addPartName, addPartFile());
			DirectoryHandler.sendFileToHDFS(conf, createTableName);
			DirectoryHandler.sendFileToHDFS(conf, addPartName);
			DirectoryHandler.givePermissionToHDFSFile(conf, createTableName);
			DirectoryHandler.givePermissionToHDFSFile(conf, addPartName);
	    }
	}
	
	public String createTableFile(){
		String cmd = "create external table if not exists ${hiveTableName} ("+Utility.hiveTextColumns+") partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String) row format delimited fields terminated by '\\001' location '${targetDirectoryValid}'";
		
		return cmd;
	}
	
	public String addPartFile(){
		String cmd = "alter table ${hiveTableName} add if not exists partition (part_year='${dir_year}',part_month='${dir_month}',part_day='${dir_day}',part_hour='${dir_hour}',part_minute='${dir_minute}') location '${hiveAddPart}'";
		
		return cmd;
	}
	
	public void validateAndcreateHiveQuery(DIConfig conf){
		String query="";
		LinkedList<String> li=new LinkedList<String>();
		Map<String,String> hivepartition=new HashMap<String, String>();
		LinkedHashMap<String,String> partitiondetails=new LinkedHashMap<String, String>();
		
		LinkedList<String> datetimestamp=new LinkedList<String>();
		StringBuilder sb=new StringBuilder();
		StringBuilder sb1=new StringBuilder();
		String tempDir="'"+"${nameNode}/user/${queueName}/${HDI_SQOOL_TEMP_DIR}"+"'";
		sb.append("INSERT OVERWRITE  DIRECTORY ").append(tempDir).append(" select ");
		try {
			String[]str =null;
			if("".equalsIgnoreCase(conf.getDate_timestamp_column())){
			}else{
				str=conf.getDate_timestamp_column().split(",");
				for(String colname:str){
					datetimestamp.add(colname.toUpperCase());
				}
			}
			
			
			HiveConf hconf = new HiveConf();
			hconf.addResource(new FileInputStream(HIVE_SITE_XML));
			//hconf.addResource("hive-site.xml");
			HiveMetaStoreClient client = new HiveMetaStoreClient(hconf);
			Table table = client.getTable(conf.getInstanceName(), conf.getExport_hive_table());
			logger.info("Displaying database and table details..."+table.getDbName()+"     "+table.getTableName());
			if(table.getPartitionKeys().size()>0){
				logger.info("Displaying table partition details...");
			for(FieldSchema fs:table.getPartitionKeys()){
				logger.info(fs.getName());
				hivepartition.put(fs.getName().toUpperCase(),fs.getType());
			}
			}else
				logger.info("No partition key found in table!!! ");
			//System.out.println("Displaying database and table details...");
			//System.out.println(table.getDbName());
			//System.out.println(table.getTableName());
			List<FieldSchema> fields = client.getFields(conf.getInstanceName(), conf.getExport_hive_table());
			ListIterator<FieldSchema> itr = fields.listIterator();
			while(itr.hasNext()) {
				//System.out.println(itr.next());
				FieldSchema schema = itr.next();
				
				li.add(schema.getName().toUpperCase());
				//System.out.println(schema.getName() + ":" + schema.getType());				
			}
			
			if("".equalsIgnoreCase(conf.getTable_part_colname())&&hivepartition.size()>0 ){
				String choice="y";
				BufferedReader rdr = null;
				try {
					
					rdr = new BufferedReader(new InputStreamReader(System.in));
					logger.warn("The table has partition key but no partition key declared by you and in this case if you continue then full table will get exported !!\n To proceeds Please press 'A' to add partion key and value(eg part_year:2016,part_month:05)  'y' to continue  and 'n' to halt the execution");
					//boolean status = rdr.ready();
					//System.out.println(status);
					choice = rdr.readLine();
					while((("y".equalsIgnoreCase(choice)) || ("n".equalsIgnoreCase(choice))||("a".equalsIgnoreCase(choice))) == false){
						choice = "";
						logger.warn("Select an option \n A - To add partition \t y - Continue \t n - Hault Execution");
						choice = rdr.readLine();
					}
					if("a".equalsIgnoreCase(choice)){
						logger.info("Please provide partition key column and value (Comma separated values in case of multiple partition columns eg part_year:2016,part_month:04).Partition column should be from  "+hivepartition.toString());
						//reader = new BufferedReader(new InputStreamReader(System.in));
						String partitionColumn = "";
						while((partitionColumn == null) || ("".equalsIgnoreCase(partitionColumn))){
							partitionColumn = rdr.readLine();
							if((partitionColumn == null) || ("".equalsIgnoreCase(partitionColumn))){
								logger.warn("partition key column cannot be null. Please provide partition key with value  (Comma separated values in case of multiple columns).Partition column should be from  "+hivepartition.toString());;
								partitionColumn = "";
							}
							else{
								String[] partCols = partitionColumn.toUpperCase().split(",");
								for(String part:partCols){
									partitiondetails.put(part.split(":")[0].toUpperCase(), part.split(":")[1]);
								}
								
								for (Map.Entry<String, String> partDetails : partitiondetails.entrySet()) {
									if(!hivepartition.containsKey(partDetails.getKey())){
										logger.info("Please provide partition key column and value (Comma separated values in case of multiple partition columns eg part_year:2016,part_month:04).Partition column should be from  "+hivepartition.toString());
										partitionColumn = "";
									}
								}
							}
						}
						//reader.close();
						conf.setTable_part_colname(partitionColumn);
						
						
					}
					else if(choice.equalsIgnoreCase("n")) {
						//reader.close();
						DirectoryHandler.cleanUpWorkspaceExport(conf);					
						throw new Error("Halting execution...");							
					}else {
						if(choice.equalsIgnoreCase("y")){
						logger.info("Full table will get exported");	
						}
					}
					
				} catch (Exception e) {
					logger.error("Error @validateAndcreateHiveQuery", e);
				} finally {
					if(rdr != null)
						rdr.close();
				}
				
			}
			
			if(!"".equalsIgnoreCase(conf.getTable_part_colname())){
				partitiondetails.clear();
				String[] partdetails=conf.getTable_part_colname().split(",");
				//System.out.println(partdetails.length);
				
				for(String part:partdetails){
					partitiondetails.put(part.split(":")[0].toUpperCase(), part.split(":")[1]);
				}
				}
			
			
			if(str!=null){
				for(String columnname:str){
					if(!li.contains(columnname.toUpperCase())){
						logger.error("column name is not matching with column name of hive table "+" "+"column of hive table are "+li.toString());
						throw new Error("Mismatch between column name given by user and column name of hive table");
					}
				}
			}
			if(partitiondetails.size()!=0){
				for(Entry<String, String> et:partitiondetails.entrySet()){
					if(!hivepartition.containsKey(et.getKey())){
					logger.error("partition name given is not matching with table parttion name or partition does not exist "+et.getKey());	
					throw new Error("Partition key doesnot match! please check and try again ");
					}
				}
			}
			
		for(String columnname:li){
			if(datetimestamp.contains(columnname)){
				sb.append("from_unixtime(unix_timestamp("+ columnname + ", 'yyyy-MM-dd HH:mm:ss')),");
			}else
				sb.append(columnname+",");
			
		}
		
		if("".equalsIgnoreCase(conf.getTable_part_colname())){
			query=sb.toString().substring(0, sb.toString().lastIndexOf(","))+" from ${hive_database_name}.${export_hive_table};";	
		}else	
	     {
			sb1.append(" where ");
			for(Entry<String, String> et:partitiondetails.entrySet()){
				
				sb1.append(et.getKey()+"="+"'${"+et.getKey()+"}'"+" and ");	
				
					
			}
			query=sb.toString().substring(0, sb.toString().lastIndexOf(","))+" from ${hive_database_name}.${export_hive_table} "+sb1.toString().substring(0, sb1.lastIndexOf(" and ")) +";";	
		}
		//String query=sb.toString().substring(0, sb.toString().lastIndexOf(","))+" from "+conf.getHive_database_name()+"."+conf.getExport_hive_table()+""	
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("Displaying hive export query "+query);
		conf.setHive_export_query(query);
		String Query="USE ${queueName};"+"\n" +"set mapred.job.queue.name=${queueName};"+"\n"+conf.getHive_export_query();
		DirectoryHandler.createNewFile(conf,conf.getExport_hive_table()+".hql" ,Query);
		DirectoryHandler.sendFileToHDFS(conf,conf.getExport_hive_table()+".hql");
		DirectoryHandler.givePermissionToHDFSFile(conf,conf.getExport_hive_table()+".hql");
	}
}