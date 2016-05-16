package com.bt.dataintegration.sqoop;

import java.io.FileInputStream;
import static com.bt.dataintegration.constants.Constants.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.sqoop.tool.ImportTool;

import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.FileLayout;

/**
 * @author Prabhu Om Manish Abhinav
 *
 */
@SuppressWarnings("deprecation")
public class ImplSqoopImport implements ISqoopImport {

	Logger logger = Logger.getLogger(ImplSqoopImport.class);
	

	/**
	 * This method to validate properties file and its access which is created
	 * by previous module(ORACLE) a.
	 *
	 */

	public boolean validateSqoopPropFileAccess() {
		String conffile = JOB_PROP_FILE;
		if (Utility.readConfigProperties(conffile) != null) {
			logger.info("able to access prop file");
			return true;
		} else{
			logger.error("Not able to access properties file");
			StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
			for(StackTraceElement stackTrace: stackTraceElements){
			    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
			}
			throw new Error();
		}
	}

	

	public String generateImportCommand() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean validateTargetDirectory() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * This method is to get and set the all the sqoop optios from properties file
	 * which is required to run sqoop job.
	 * @throws FileNotFoundException 
	 */
	
	
	public int sqoopImport(HadoopConfig conf) throws FileNotFoundException  {
		
		//String createCreds = null;				
		int retval = 0;
		String sqoopCmd="";
		String credsFileName = "hdi_" +conf.getDatabaseHost()+"_"+conf.getSourceName() + "_" + conf.getTableOwner() + ".pswd";
		if(conf.getSqoopFileFormat().contains("avro")){
			sqoopCmd="sqoop import "
					+ SQ_IMPORT_PARAM_CREDS +"="+JCEKS+ "/" + conf.getQueueName() + "/HDI_Password_Repository/"
					+ credsFileName
					+" -D mapreduce.job.queuename="+conf.getQueueName()+" --connect "
	                +conf.getDBConnectString()
	                +" --username "+conf.getUserName()
	                +" --password-alias "+credsFileName
	                +" --query "+"\""+conf.getQueryMilestone()+"\""
	                //+"--fields-terminated-by '\001'"
	                +" --target-dir " + conf.getTargetDirectory() 
	                +" "+conf.getSqoopFileFormat()+ " -m 1";
		}
		else if(conf.getSqoopFileFormat().contains("text")){
			sqoopCmd="sqoop import "
					+ SQ_IMPORT_PARAM_CREDS + "="+JCEKS+"/" + conf.getQueueName() + "/HDI_Password_Repository/"
					+ credsFileName
					+" -D mapreduce.job.queuename="+conf.getQueueName()+" --connect "
	                +conf.getDBConnectString()
	                +" --username "+conf.getUserName()
	                +" --password-alias "+credsFileName
	                +" --fields-terminated-by '\001'"
	                +" --query "+"\""+conf.getQueryMilestone()+"\""
	                +" --target-dir " + conf.getTargetDirectory() 
	                +" "+conf.getSqoopFileFormat()+ " -m 1";
		}
		else if(conf.getSqoopFileFormat().contains("parquet")){
			sqoopCmd="sqoop import "
					+ SQ_IMPORT_PARAM_CREDS +"="+JCEKS+ "/" + conf.getQueueName() + "/HDI_Password_Repository/"
					+ credsFileName
					+" -D mapreduce.job.queuename="+conf.getQueueName()+" --connect "
	                +conf.getDBConnectString()
	                +" --username "+conf.getUserName()
	                +" --password-alias "+credsFileName
	                +" --query "+"\""+conf.getQueryMilestone()+"\""
	                +" --target-dir " + conf.getTargetDirectory() 
	                +" "+conf.getSqoopFileFormat()+ " -m 1";
		}
		//System.out.println(sqoopCmd);
		logger.debug("Sqoop Command " + sqoopCmd);
		int output = Utility.executeSSH(sqoopCmd);
		if(output !=0){
			logger.error("Error at sqoop. Please check logs");
  			//logger.error("", e);
  			DirectoryHandler.cleanUpWorkspace(conf);
			DirectoryHandler.cleanUpLanding(conf);
			throw new Error();
		}
		return retval;
		
		
	}

	
	/**
	 * This method is to validate the file format. 
	 * It will return true when file format is avro else it will return false.
	 */
	
	public boolean validateFileFormat(Path location, Configuration conf){
		   FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(location.toUri(), conf);
			FileStatus[] items = fileSystem.listStatus(location);
		    for(FileStatus item: items) {
		    	if(item.getPath().getName().startsWith("_")) {
		            continue;
		          }

		    	else if(item.getPath().getName().contains(".avro")) {
		    	logger.debug(item.getPath().getName());
		    	logger.info("Created File format is AVRO !");
		    	return true;
		    
		      }else
		    	  logger.debug(item.getPath().getName());
		    	  logger.error("Created File Format is not correct");
		    	  
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.trace(e.getMessage());
		}
		return false;
		    

		
	}

}