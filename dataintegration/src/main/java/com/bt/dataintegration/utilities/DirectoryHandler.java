package com.bt.dataintegration.utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import static com.bt.dataintegration.constants.Constants.*;

import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.HadoopConfig;

public class DirectoryHandler {
	final static Logger logger = Logger.getLogger(DirectoryHandler.class);
	public static String targetDirYear = "";
	public static String targetDirMonth = "";
	public static String targetDirDate = "";
	public static String targetDirHour = "";
	public static String targetDirMinute = ""; 
	public static String targetDirMonthWords = ""; 
	
	static{
		Date date = new Date();
		Calendar cal;

		cal = Calendar.getInstance();
		cal.setTime(date);
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_YEAR);
		targetDirYear = sdf.format(date);
		
		sdf = new SimpleDateFormat(DATE_FORMAT_MONTH);
		targetDirMonth = sdf.format(date);
		
		targetDirMonthWords= new SimpleDateFormat(DATE_FORMAT_MONTH_WORDS).format(cal.getTime());
		
		sdf = new SimpleDateFormat(DATE_FORMAT_DAY);
		targetDirDate = sdf.format(date);
		
		sdf = new SimpleDateFormat(DATE_FORMAT_HOUR);
		targetDirHour = sdf.format(date);
		
		sdf = new SimpleDateFormat(DATE_FORMAT_MINUTE);
		targetDirMinute = sdf.format(date);
}
	
public static void createWorkflowPath(DIConfig conf){
	  int shellout = 1;
	String workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
	String landingDir = conf.getLandingDirectory();
	String cmd="";
   
		logger.info("Creating workspace directory");
		cmd = "hadoop fs -mkdir -p "+workspacePath;
		shellout = Utility.executeSSH(cmd);
		if(shellout !=0){
			logger.error("Failed to create directory:: "+workspacePath);
			throw new Error("Failed to create directory. Please delete the above directory and re-run the application");
		}	
		
		logger.info("Creating landing directory");
		cmd = "hadoop fs -mkdir -p "+landingDir;
		shellout = Utility.executeSSH(cmd);
		if(shellout !=0){
			logger.error("Directory already exists:: "+landingDir);
			throw new Error("Failed to create directory. Please delete the above directory and re-run the application");
		}

}


public static void createExportWorkflowPath(DIConfig conf){
	  int shellout = 1;
	
	String workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT";
	String cmd="";
  
		logger.info("Creating workspace directory");
		cmd = "hadoop fs -mkdir -p "+workspacePath;
		shellout = Utility.executeSSH(cmd);
		if(shellout !=0){
			logger.error("Failed to create directory:: "+workspacePath);
			throw new Error("Failed to create directory. Please check logs");
		}	
		
		
}

public static void createPasswordDirectory(DIConfig conf){
	String passwordPath= conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_Password_Repository";
	String cmd="";
	int shellout = 1;
	cmd = "hadoop fs -mkdir -p "+passwordPath;
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		logger.info("Directory already exists:: "+passwordPath);
		//throw new Error("Failed to create directory. Please check logs");
	}
}

public static void createAuditLogPath(DIConfig conf){
	logger.info("Creating/Checking Audit log path");
	String cmd = "hadoop fs -ls "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT";
	int shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout == 0){
		cmd = "hadoop fs -ls "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT/"+AUDIT_FILE;
		shellout = Utility.executeSSH(cmd);
		if(shellout != 0){
			cmd = "hadoop fs -touchz "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT/"+AUDIT_FILE;
			shellout = Utility.executeSSH(cmd);
			cmd = "hadoop fs -chmod 660 "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT/"+AUDIT_FILE;
			shellout = Utility.executeSSH(cmd);
		}
		else{
			cmd = "hadoop fs -chmod 660 "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT/"+AUDIT_FILE;
			shellout = Utility.executeSSH(cmd);
		}
	}
	else{
		cmd = "hadoop fs -mkdir -p "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT";
		shellout = Utility.executeSSH(cmd);
		cmd = "hadoop fs -touchz "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT/"+AUDIT_FILE;
		shellout = Utility.executeSSH(cmd);
		cmd = "hadoop fs -chmod 660 "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_AUDIT/"+AUDIT_FILE;
		shellout = Utility.executeSSH(cmd);
	}
}

public static void sendFileToHDFS(Object obj,String fileName){
	String workspacePath="";
	String workspaceFilename = fileName;
	int flag = 0;
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"/";
			if(!(fileName.contains("job") || fileName.contains("unix_date")))
				fileName = conf.getTableName()+"/"+fileName;
		}else if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_EXPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT/";
			if(!(fileName.contains("job") || fileName.contains("unix_date")))
				fileName = conf.getTableName()+"/"+fileName;
		}else if(conf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTable()+"/";
			if(!(fileName.contains("job") || fileName.contains("unix_date")))
				fileName = conf.getHiveTable()+"/"+fileName;
			flag =1;
		}
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"/";
			if(!(fileName.contains("job") || fileName.contains("unix_date")))
				fileName = conf.getTableName()+"/"+fileName;
		}else if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_EXPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT/";
			if(!(fileName.contains("job") || fileName.contains("unix_date")))
				fileName = conf.getTableName()+"/"+fileName;
		}else if(conf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTableName()+"/";
			if(!(fileName.contains("job") || fileName.contains("unix_date")))
				fileName = conf.getHiveTableName()+"/"+fileName;
			flag =1;
		}
	}
	
	if(fileName.contains("/")){
		workspaceFilename = (fileName.split("\\/"))[1];
	}
	
	String cmd = "hadoop fs -put "+fileName+" "+workspacePath+workspaceFilename;
	logger.debug("command to store "+workspaceFilename+" file -- " + cmd);
	int shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		logger.error("Error in sending file to HDFS "+ workspaceFilename);
		if(obj instanceof DIConfig){
			if(flag == 1){
				cleanUpWorkspaceFile((DIConfig)obj);
			}
			else{
				cleanUpWorkspace((DIConfig)obj);
				cleanUpLanding((DIConfig)obj);
			}
		}
		else if(obj instanceof HadoopConfig){
			if(flag == 1){
				cleanUpWorkspaceFile((HadoopConfig)obj);
			}
			else{
				cleanUpWorkspace((HadoopConfig)obj);
				cleanUpLanding((HadoopConfig)obj);
			}
		}
		throw new Error();
	}
}
public static void givePermissionToHDFSFile(Object obj,String fileName){
	String workspacePath="";
	int flag = 0;
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"/";
		}else if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_EXPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT/";
		}else if(conf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTable()+"/";
			flag =1;
		}
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"/";
		}else if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_EXPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT/";
		}else if(conf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTableName()+"/";
			flag =1;
		}
	}
	//String workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI-"+conf.getSourceName()+"-"+conf.getTableOwner()+"-"+conf.getTableName()+"/";
	if(fileName.contains("/")){
		fileName = (fileName.split("\\/"))[1];
	}
	String cmd = "hadoop fs -chmod 660 "+workspacePath+fileName;
	logger.debug("command to give permission to "+fileName+" file -- " + cmd);
	int shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		logger.error("Error in giving permission to file "+ fileName);
		if(obj instanceof DIConfig){
			if(flag == 1){
				cleanUpWorkspaceFile((DIConfig)obj);
			}
			else{
				cleanUpWorkspace((DIConfig)obj);
				cleanUpLanding((DIConfig)obj);
			}
		}
		else if(obj instanceof HadoopConfig){
			if(flag == 1){
				cleanUpWorkspaceFile((HadoopConfig)obj);
			}
			else{
				cleanUpWorkspace((HadoopConfig)obj);
				cleanUpLanding((HadoopConfig)obj);
			}
		}
		throw new Error();
	}
}

public static void createNewFile(Object obj,String fileName,String fileContent){
	PrintWriter out = null;
	int flag = 0;
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT) || conf.getImport_export_flag().equalsIgnoreCase(SQOOP_EXPORT))
			fileName = conf.getTableName()+"/"+fileName;
		else if(conf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			fileName = conf.getHiveTable()+"/"+fileName;
			flag =1;
		}	
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		if(conf.getImport_export_flag().equalsIgnoreCase(SQOOP_IMPORT) || conf.getImport_export_flag().equalsIgnoreCase(SQOOP_EXPORT))
			fileName = conf.getTableName()+"/"+fileName;
		else if(conf.getImport_export_flag().equalsIgnoreCase(FILE_IMPORT)){
			fileName = conf.getHiveTableName()+"/"+fileName;
			flag =1;
		}
	}
	File file = new File(fileName);
		try {
			out = new PrintWriter(file);
			out.println(fileContent);
		} catch (FileNotFoundException e) {
			
			logger.error("Error loading "+fileName);
			logger.error("", e);
			if(obj instanceof DIConfig){
				if(flag == 1){
					cleanUpWorkspaceFile((DIConfig)obj);
				}
				else{
					cleanUpWorkspace((DIConfig)obj);
					cleanUpLanding((DIConfig)obj);
				}
			}
			else if(obj instanceof HadoopConfig){
				if(flag == 1){
					cleanUpWorkspaceFile((HadoopConfig)obj);
				}
				else{
					cleanUpWorkspace((HadoopConfig)obj);
					cleanUpLanding((HadoopConfig)obj);
				}
			}
			throw new Error(e);
		}
		finally{
			out.close();
		}
}

public static void cleanUpWorkspace(Object obj){
	String workspacePath="";
	String landingDir="";
	String passFileName = "";
	String cmd = "";
	String jobPropName = "";
	int shellout =1;
	
	logger.info("Cleaning up the workspace directory structure");
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
		landingDir=conf.getLandingDirectory();
		passFileName = conf.getTableName()+"/"+conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
		jobPropName = conf.getTableName()+"/"+JOB_PROP_FILE;
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
		landingDir=conf.getLandingDirectory();
		passFileName = conf.getTableName()+"/"+conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
		jobPropName = conf.getTableName()+"/"+JOB_PROP_FILE;
	}
	cmd = "hadoop fs -rm -r "+workspacePath;
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		logger.error("Error in cleaning up workspace "+workspacePath);
		throw new Error();
	}
	
	
	PrintWriter out = null;
	/*try {
			
			out = new PrintWriter(passFileName);
			out.println("");
		} catch (FileNotFoundException e) {
			logger.error("Password file not found");
			logger.error("", e);
			throw new Error(e);
	} 
		finally{
		out.close();
		}
	try {
			out = new PrintWriter(jobPropName);
			out.println("");
		} catch (FileNotFoundException e) {
			logger.error("job.properties file not found");
			logger.error("", e);
			throw new Error(e);
	} 
		finally{
		out.close();
		}*/
}

public static void cleanUpLanding(Object obj){
	logger.info("Cleaning up the landing directory structure");
	String workspacePath="";
	String landingDir="";
	String passFileName = "";
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
		landingDir=conf.getLandingDirectory();
		passFileName = conf.getTableName()+"/"+conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
		landingDir=conf.getLandingDirectory();
		passFileName = conf.getTableName()+"/"+conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
	}
	String cmd = "hadoop fs -rm -r "+landingDir;
	int shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		logger.error("Error in cleaning up landing directory "+landingDir);
		throw new Error();
	}
	PrintWriter out = null;
	/*try {
			
			out = new PrintWriter(passFileName);
			out.println("");
		} catch (FileNotFoundException e) {
			logger.error("Password file not found");
			logger.error("", e);
		//cleanUpWorkspace(conf);
		System.exit(0);
	} 
		finally{
		out.close();
		}*/
}

public static void cleanUpWorkspaceExport(Object obj){
	String workspacePath="";
	String passFileName = "";
	String cmd = "";
	String jobPropName = "";
	int shellout =1;
	logger.info("Cleaning up the workspace directory structure");
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT";
		passFileName = conf.getTableName()+"/"+conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
		jobPropName = conf.getTableName()+"/"+JOB_PROP_FILE;
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName()+"_EXPORT";
		passFileName = conf.getTableName()+"/"+conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
		jobPropName = conf.getTableName()+"/"+JOB_PROP_FILE;
	}
	cmd = "hadoop fs -rm -r "+workspacePath;
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		throw new Error();
	}
	
	
	PrintWriter out = null;
	try {
			
			out = new PrintWriter(passFileName);
			out.println("");
		} catch (FileNotFoundException e) {
			logger.error("Password file not found");
			logger.error("", e);
		//cleanUpWorkspace(conf);
		System.exit(0);
	} 
		finally{
		out.close();
		}
	try {
			out = new PrintWriter(jobPropName);
			out.println("");
		} catch (FileNotFoundException e) {
			logger.error(JOB_PROP_FILE+" file not found");
			logger.error("", e);
		//cleanUpWorkspace(conf);
		System.exit(0);
	} 
		finally{
		out.close();
		}
}



public static void checkFileDirectoryExists(String dirPath){
	String cmd = "hadoop fs -ls "+dirPath;
	int shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout == 0){
		logger.info("Base directory exists");
	}
	else{
		logger.error("Base directory does not exist.");
		throw new Error();
	}
}

public static void createFileWorkflowPath(DIConfig conf){
	String fileWorkspace = "/user/"+conf.getInstanceName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTable();
	String cmd = "hadoop fs -mkdir -p "+fileWorkspace;
	int shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout != 0){
		logger.error("Error in creating file workspace directory "+fileWorkspace );
		throw new Error();
	}
}

public static void createFileTargetPath(DIConfig conf){
	/*String fileTargetTemp = "/user/"+conf.getInstanceName()+"/landing/staging/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTable()+"/temp";
	String cmd = "hadoop fs -mkdir -p "+fileTargetTemp;
	int shellout = 1;
	shellout = Utility.executeSSH(cmd);*/
	String fileTarget = "";
	String interimDir = conf.getInterimLandingDir();
	if("".equals(interimDir)){
		fileTarget=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+conf.getSourceName()+"/HDI_FILE_"+conf.getHiveTable();
	}else{
		fileTarget=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+interimDir+"/"+conf.getSourceName()+"/HDI_FILE_"+conf.getHiveTable();
	}
	
	String fileTargetValid = fileTarget+"/valid_files";
	String cmd = "hadoop fs -mkdir -p "+fileTargetValid;
	int shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout != 0){
		logger.error("Error in creating valid target directory "+fileTargetValid );
		cleanUpWorkspaceFile(conf);
		throw new Error();
	}
	
	String fileTargetInvalid = fileTarget+"/rejected_files";
	cmd = "hadoop fs -mkdir -p "+fileTargetInvalid;
	shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout != 0){
		logger.error("Error in creating invalid target directory "+fileTargetInvalid );
		cleanUpWorkspaceFile(conf);
		throw new Error();
	}
	
	String fileTargetRaw = fileTarget+"/raw";
	cmd = "hadoop fs -mkdir -p "+fileTargetRaw;
	shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout != 0){
		logger.error("Error in creating invalid target directory "+fileTargetRaw );
		cleanUpWorkspaceFile(conf);
		throw new Error();
	}
	
	String fileTargetArchive = fileTarget+"/archive";
	cmd = "hadoop fs -mkdir -p "+fileTargetArchive;
	shellout = 1;
	shellout = Utility.executeSSH(cmd);
	if(shellout != 0){
		logger.error("Error in creating invalid target directory "+fileTargetArchive );
		cleanUpWorkspaceFile(conf);
		throw new Error();
	}
}

public static void cleanUpWorkspaceFile(Object obj){
	String workspacePath="";
	String cmd = "";
	String jobPropName = "";
	int shellout =1;
	logger.info("Cleaning up the workspace directory structure");
	if(obj instanceof DIConfig){
		DIConfig conf = (DIConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTable()+"/";
		jobPropName = conf.getTableName()+"/"+JOB_PROP_FILE;
	}
	else if(obj instanceof HadoopConfig){
		HadoopConfig conf = (HadoopConfig)obj;
		workspacePath=conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/workspace/HDI_FILE_"+conf.getSourceName()+"_"+conf.getHiveTableName()+"/";
		jobPropName = conf.getTableName()+"/"+JOB_PROP_FILE;
	}
	cmd = "hadoop fs -rm -r "+workspacePath;
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		logger.error("Error in cleaning up workspace "+ workspacePath);
		throw new Error();
	}
	
	
	/*PrintWriter out = null;
	
	try {
			out = new PrintWriter(jobPropName);
			out.println("");
		} catch (FileNotFoundException e) {
			logger.error("job.properties file not found");
			logger.error("", e);
		//cleanUpWorkspace(conf);
		throw new Error(e);
	} 
		finally{
		out.close();
		}*/
}

/*public static void createTargetDirectory(DIConfig conf){
	int shellout = 1;
	if(conf.getEnvDetails().equalsIgnoreCase("1")){
		nameNode = NAMENODE;
	}
	else
	{
		nameNode = NAMENODE_VM;
	}
	String cmd = "hadoop fs -mkdir -p "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+conf.getTableName()+"/landing/DELTA_DATA/"+targetDirYear+"/"+targetDirMonth+"/"+targetDirDate+"/"+targetDirHour;
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		throw new Error();
	}
}
*/
/*public static void createTargetDirectorySqoop(DIConfig conf){
	int shellout =1;
	if(conf.getEnvDetails().equalsIgnoreCase("1")){
		nameNode = NAMENODE;
	}
	else
	{
		nameNode = NAMENODE_VM;
	}
	String cmd = "hadoop fs -mkdir -p "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+conf.getTableName()+"/landing";
	shellout = Utility.executeSSH(cmd);
	if(shellout !=0){
		throw new Error();
	}
}
*/
//one more method required to create target directory dynamically in oozie
/*public static void updateJobPropFile(){
	Properties jobProp = Utility.readConfigProperties("job.properties");
	jobProp.setProperty("lastModifiedDateValueLowerBound", jobProp.getProperty("lastModifiedDateValueUpperBound"));
	String current_date = targetDirDate+"-"+targetDirMonth+"-"+targetDirYear+" "+targetDirHour+":"+targetDirMinute;
	jobProp.setProperty("lastModifiedDateValueUpperBound", "TO_DATE('"+current_date+"','dd-mm-yyyy hh24:mi')");
	jobProp.setProperty("targetDirYear", targetDirYear);
	jobProp.setProperty("targetDirMonth", targetDirMonth);
	jobProp.setProperty("targetDirDate", targetDirDate);
	jobProp.setProperty("targetDirHour", targetDirHour);
	jobProp.setProperty("targetDirMinute", targetDirMinute);
	OutputStream fout = null;
	try {
		fout = new FileOutputStream("job.properties");
		try {
			jobProp.store(fout, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

}
*/


}
