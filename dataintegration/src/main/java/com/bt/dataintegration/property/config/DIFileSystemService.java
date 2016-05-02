package com.bt.dataintegration.property.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jline.internal.InputStreamReader;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.bt.dataintegration.FileSystem.FileProcessImpl;
import com.bt.dataintegration.FileSystem.IFileProcess;
import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.hive.HiveProcessImpl;
import com.bt.dataintegration.oozie.coordinator.xmlcodegen.CoordinatorXMLCodegen;
import com.bt.dataintegration.oozie.workflow.xmlcodegen.WorkflowXMLCodegen;
import com.bt.dataintegration.pig.PigCompressionImpl;
import com.bt.dataintegration.shell.IShell;
import com.bt.dataintegration.shell.ShellImpl;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class DIFileSystemService {
	final static Logger logger = Logger.getLogger(DIFileSystemService.class);
	public static void main(String args[]){
		String log4JPropertyFile = "log4j.properties";
		Properties p = new Properties();

		try {
		    p.load(new FileInputStream(log4JPropertyFile));
		    PropertyConfigurator.configure(p);
		   // logger.info("Wow! I'm configured!");
		} catch (IOException e) {
		    logger.error("log4j.properties file not found");

		}
		DIConfig servConf = null;
		try {
			
			servConf = new DIConfig().getDIConfigProperties();
			
			String cmd1 = "mkdir "+servConf.getHiveTable();
			Utility.executeSSH(cmd1);
			
			IFileProcess fileimpl = new FileProcessImpl();
			
			String hiveTableName = fileimpl.checkHiveTableExists(servConf);
			servConf.setHiveTable(hiveTableName.toUpperCase());
			
			DirectoryHandler.checkFileDirectoryExists(servConf.getFileDirectory());
			DirectoryHandler.createFileWorkflowPath(servConf);
			DirectoryHandler.createFileTargetPath(servConf);
			
					
			
			fileimpl.prepareJobProperties(servConf);
			
			HadoopConfig conf = new HadoopConfig().getHadoopConfigProperties();
			
			PigCompressionImpl pig = new PigCompressionImpl();
			pig.generatePigFile(conf);
			
			IShell shell = new ShellImpl();
			shell.shellToHDFSFile(conf);
			
			HiveProcessImpl implHive = new HiveProcessImpl();
			implHive.hiveScriptsFile(conf);
			
			String workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTableName()+"/";
			
			String cmd = "hadoop fs -mkdir -p "+workspacePath+"lib";
			int shellout = 1;
			shellout = Utility.executeSSH(cmd);
			if(shellout !=0){
				throw new Error();
			}	
			
			cmd = "hadoop fs -put file-validations-v1.jar " + workspacePath+"lib";
			shellout = 1;
			shellout = Utility.executeSSH(cmd);
			if(shellout !=0){
				logger.error("Error in sending file system validation jar to HDFS");
				throw new Error();
			}	
		
			
			WorkflowXMLCodegen codegen = new WorkflowXMLCodegen();
			codegen.generateXML(conf);
			
			if(servConf.isCoordinatorFlag()) {
				CoordinatorXMLCodegen ccodegen = new CoordinatorXMLCodegen();
				ccodegen.generateXML(conf);
			}
			
			cmd = "mv job.properties "+servConf.getHiveTable()+"/job.properties";
			Utility.executeSSH(cmd);
			 
			cmd = "cp configuration.properties "+servConf.getHiveTable()+"/configuration.properties";
			Utility.executeSSH(cmd); 
			
			cmd = "cp "+servConf.getMappingSheetname()+" "+servConf.getHiveTable()+"/"+servConf.getMappingSheetname();
			Utility.executeSSH(cmd); 
			 
		} catch (Exception e) {

			//System.out.println("Error ar DIFileSystemService.main()");
			logger.error("Error ar DIFileSystemService");
			DirectoryHandler.cleanUpWorkspaceFile(servConf);	
			throw new Error(e);
		}
		
	}
}
