/*
 *v1 Author: 609298143 (Manish Bajaj)
 * 			609349708 (Abhinav Meghmala)
 */

package com.bt.dataintegration.property.config;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.hive.HiveProcessImpl;
//import com.bt.dataintegration.oozie.sqoop.export.workflow.ExportWorkflowXMLCodegen;
import com.bt.dataintegration.oozie.coordinator.xmlcodegen.CoordinatorXMLCodegen;
import com.bt.dataintegration.oozie.workflow.xmlcodegen.WorkflowXMLCodegen;
import com.bt.dataintegration.pig.PigCompressionImpl;
import com.bt.dataintegration.shell.IShell;
import com.bt.dataintegration.shell.ShellImpl;
import com.bt.dataintegration.sqoop.ImplSqoopImport;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class DIConfigService {
	
	final static Logger logger = Logger.getLogger(DIConfigService.class);
	private static boolean status = false;

	// Calling DICOnfig
	public static void main(String[] args) {

		String log4JPropertyFile = "log4j.properties";
		Properties p = new Properties();
		String pwd = "";
		
		try{
			pwd = args[0];
		}catch(NullPointerException e){
			logger.error("Please provide database password as argument");
			logger.error(e);
			throw new Error(e);
		}
		
		try {
		    p.load(new FileInputStream(log4JPropertyFile));
		    PropertyConfigurator.configure(p);
		   // logger.info("Wow! I'm configured!");
		} catch (IOException e) {
		    logger.error("log4j.properties file not found");

		}
		DIConfig servConf = null;
		DBConnectImpl oraImpl = new DBConnectImpl();
		TableProperties tabProp = new TableProperties();
		LinkedHashMap<String, List<String>> oraTabMap = new LinkedHashMap<String, List<String>>();
		Map<String, List<String>> tabPart = new HashMap<String, List<String>>();
		Map<String, String> colCons = new HashMap<String, String>();
		try {
			
			
			servConf = new DIConfig().getDIConfigProperties();
			servConf.setSourcePassword(pwd);
			String cmd1 = "rm part-m-00000.avro";
			Utility.executeSSH(cmd1);
			
			cmd1 = "mkdir "+servConf.getTableName();
			Utility.executeSSH(cmd1);
			
			Connection con = oraImpl.connect(servConf);
			boolean valTabStats;

			boolean catCheck = oraImpl.validateCatalogPermission(con, servConf);
				
			valTabStats = oraImpl.validateTable(con,servConf);
			
			double tabSize = 0;
          
	       //tabSize = oraImpl.getTableSize(con,servConf);
	       tabProp.setTableSize(tabSize);
	       tabPart = oraImpl.getTablePartition(con,servConf);
		   tabProp.setColPartition(tabPart);
		   
               
			oraTabMap = oraImpl.getColumnDetails(con,servConf);
		
			logger.info("Setting table properties...");
					
					

					tabProp.setColDetails(oraTabMap);
					
					colCons = oraImpl.getTableConstraints(con,servConf);
					tabProp.setColCons(colCons);
					 if(servConf.getImport_export_flag().equalsIgnoreCase("2") && !colCons.isEmpty()){
					   logger.warn("Table having constraints! While exporting data to same table may  fails if 'update_key_column' is not provided.Provide the same in configuration file and start the application again! "); 
					 }

					DirectoryHandler.createWorkflowPath(servConf);
					DirectoryHandler.createAuditLogPath(servConf);
					
					
					//Password will be taken form the user and will be stored in encrypted fashion in HDFS
					//DirectoryHandler.createPasswordDirectory(servConf);
					//oraImpl.storePassword(servConf);
				
					oraImpl.storeTableMetadata(tabProp, servConf);
					ImplSqoopImport implsqoop = new ImplSqoopImport();
					HadoopConfig conf = new HadoopConfig().getHadoopConfigProperties();
					implsqoop.sqoopImport(conf);
					if(!(conf.getSqoopFileFormat().contains("parquet"))){
						PigCompressionImpl pig = new PigCompressionImpl();
						pig.compressData(conf);
					}
					IShell shell = new ShellImpl();
					shell.shellToHDFS(conf);
					
					HiveProcessImpl implHive = new HiveProcessImpl();	
					String hiveTableName = implHive.validateHiveTablePrivs(conf);
					
					conf.setHiveTableName(hiveTableName);
					implHive.copyHiveScriptsToHDFS(conf);
					
					WorkflowXMLCodegen codegen = new WorkflowXMLCodegen();
					//codegen.copyHivesiteXmlToHDFS(conf);					
					codegen.generateXML(conf);
					
					if(conf.isCoordinatorRequired()) {
					
						CoordinatorXMLCodegen xmlCodegen = new CoordinatorXMLCodegen();
						xmlCodegen.generateXML(conf);
					}					
					
					logger.info("Workflow.xml generated !");
					String cmd = "hadoop fs -put ojdbc6-11.2.0.3.jar " + conf.getWorkspacePath();
					int shellout = 1;
					shellout = Utility.executeSSH(cmd);
					if(shellout !=0){
						throw new Error();
					}									
					logger.info("Cleaning up landing directory");
					DirectoryHandler.cleanUpLanding(conf);
					
					 logger.info("Cleaning up working directory");
					 String cleanCmd = "rm QueryResult.avsc";
					 Utility.executeSSH(cleanCmd);
					 
					 cleanCmd = "rm QueryResult.java";
					 Utility.executeSSH(cleanCmd);
					 
					 cleanCmd = "mv job.properties "+servConf.getTableName()+"/job.properties";
					 Utility.executeSSH(cleanCmd);
					 
					 cmd = "cp configuration.properties "+servConf.getTableName()+"/configuration.properties";
					 Utility.executeSSH(cmd);
		//		}
		//	}

		} catch (Exception e) {

			//System.out.println("Error ar DIConfigService.main()");
			logger.error("Error ar DIConfigService");
			logger.info("Cleaning up landing directory");
			DirectoryHandler.cleanUpLanding(servConf);
			logger.info("Cleaning up workspace directory");
			DirectoryHandler.cleanUpWorkspace(servConf);
			throw new Error(e);
		}
	}
}
