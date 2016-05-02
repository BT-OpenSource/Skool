package com.bt.haas.httpfs.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bt.haas.httpfs.client.impl.HTTPFSConnectionFactory;
import com.bt.haas.httpfs.client.impl.IHTTPFSConnection;
import com.bt.haas.httpfs.utils.ChangePassword;
import com.bt.haas.httpfs.utils.ConfigProperties;

/**
 * 
 * @author 606723501
 *
 */
public class HaaSHTTPFSClient {
	
	private static final Logger logger = LoggerFactory.getLogger(HaaSHTTPFSClient.class);
	
  /**
   * 
   * @param args
   */
  @SuppressWarnings("static-access")
public static void main(String[] args) {
	  
	  boolean changePwd = false;
	  // 
	  try {
		if(ConfigProperties.getPassword("haas.password").isEmpty()){
			ConfigProperties.unloadPropertiesFile();
			ChangePassword chgPassword = new ChangePassword();
			chgPassword.promptPassword();
			changePwd = true;
		}
	} catch (Exception e) {
		logger.error("Cannot set the password !!!!",e);
	}
	// Making sure that we exit after password change 
	if(changePwd){
		logger.info("Password changed successfully !!!!!");
		System.exit(0);
	}
		
   
	 Options options = new Options();
	 // Help or usage 
	 options.addOption("help", false, "Help or Usage method");
	 
	 //File and Directory Operations
	 options.addOption("ls", true, "List directory contents");
	 
	 Option copyfromLocal = OptionBuilder.withArgName("localFile> <hdfsLocation").withValueSeparator(' ').hasArgs(2)
			 .withDescription("Put the file from local location to destination location <source> <destination>.").create("copyFromLocal");
	 options.addOption(copyfromLocal);
	 
	 Option copyFromLocalDir = OptionBuilder.withArgName("locallocation> <hdfsLocation").withValueSeparator(' ').hasArgs(2)
			 .withDescription("Put the files from local directory to destination location <source> <destination>").create("copyFromLocalDir");
	 options.addOption(copyFromLocalDir);
	 
	 Option copyToLocal = OptionBuilder.withArgName("hdfsLocation> <localFile").withValueSeparator(' ').hasArgs(2)
			 .withDescription("Put the files from local directory to destination location <source> <destination>").create("copyToLocal");
	 options.addOption(copyToLocal);
	 
	 Option cat = OptionBuilder.withArgName("hdfsLocation ").withValueSeparator(' ').hasArgs(2)
			 .withDescription("mention file name with location").create("cat");
	 options.addOption(cat);
	 
	 options.addOption("mkdir", true, "create a directory");
	 
	 options.addOption("rm", true, "Delete the file/directory");
	 
	 Option rn = OptionBuilder.withArgName("currentFileName> <NewFileName").withValueSeparator(' ').hasArgs(2)
			 .withDescription("Rename the file/directory <Current> <New>").create("rn");
	 options.addOption(rn);
	 
	 options.addOption("status", true, "Get status of File/Directory");
	 
	 //Other File System Operations
	 options.addOption("home", false, "Get the home directory");
	 
	 Option chmod = OptionBuilder.withArgName("mode> <Path").withValueSeparator(' ').hasArgs(2)
			 .withDescription("Rename the file/directory <mode> <Path>").create("chmod");
	 options.addOption(chmod);
	 
	 Option chgrp = OptionBuilder.withArgName("owner> <group> <path").withValueSeparator(' ').hasArgs(3)
			 .withDescription("Rename the file/directory <owner> <group> <Path>").create("chgrp");
	 options.addOption(chgrp);
	 
     logger.info("Command Issued : "+args[0]);

	 // Time to parse the commands issued by user
	 CommandLineParser parser = new BasicParser();
	 CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		}catch (ParseException pe){ 
			usage(options); 
			return; 
			
	 }
		
	
	String path = null;
	String source = null;
	String destination = null;
	int mode = -1;
	String owner = null;
	String group = null;
	String command = null;
	
	if (cmd.hasOption("ls")) {
		command = "ls";
		path = cmd.getOptionValue("ls");
	}else if(cmd.hasOption("copyFromLocal")){
		command = "copyFromLocal";
		String[] copyFromLocalArgs = cmd.getOptionValues("copyFromLocal");
		source = copyFromLocalArgs[0];
		path = copyFromLocalArgs[1];
		logger.info("source : "+source+" "+new File(source).exists());
		logger.info("path : "+path);
		if(!new File(source).isFile()){
			throw new IllegalArgumentException("Source should be file");
		}
	}else if(cmd.hasOption("copyFromLocalDir")){
		command = "copyFromLocalDir";
		String[] copyFromLocalDirArgs = cmd.getOptionValues("copyFromLocalDir");
		source = copyFromLocalDirArgs[0];
		path = copyFromLocalDirArgs[1];
	}else if(cmd.hasOption("copyToLocal")){
		command = "copyToLocal";
		String[] copyToLocalArgs = cmd.getOptionValues("copyToLocal");
		path = copyToLocalArgs[0];
		destination = copyToLocalArgs[1];
	}else if(cmd.hasOption("cat")){
		command = "cat";
		path = cmd.getOptionValue("cat");
	}else if(cmd.hasOption("mkdir")){
		command = "mkdir";
		path = cmd.getOptionValue("mkdir");
	}else if(cmd.hasOption("rm")){
		command = "rm";
		path = cmd.getOptionValue("rm");
	}else if(cmd.hasOption("rn")){
		command = "rn";
		String[] rnArgs = cmd.getOptionValues("rn");
		source = rnArgs[0];
		destination = rnArgs[1];
	}else if(cmd.hasOption("status")){
		command = "status";
		path = cmd.getOptionValue("status");
	}else if(cmd.hasOption("home")){
		command = "home";
		path = cmd.getOptionValue("home");
	}else if(cmd.hasOption("chmod")){
		command = "chmod";
		String[] chmodArgs = cmd.getOptionValues("chmod");
		mode = Integer.parseInt(chmodArgs[0]);
		path = chmodArgs[1];
	}else if(cmd.hasOption("chgrp")){
		command = "chgrp";
		String[] chgrpArgs = cmd.getOptionValues("chgrp");
		owner = chgrpArgs[0];
		group = chgrpArgs[1];
		path = chgrpArgs[2];
	}else if(cmd.hasOption("help")){
		command = "help";
	}
	
	
	
	IHTTPFSConnection conn = HTTPFSConnectionFactory.getConnection();
	logger.info("Command "+ command +" Started  .... ");

	
    try {
		if(command.equals("ls")){
			conn.listStatus(path);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("copyFromLocal")){
			File file = new File(source);
			FileInputStream is = new FileInputStream(file);
			conn.create(path+IHTTPFSConnection.PATH_SEPERATOR+file.getName(), is);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("copyFromLocalDir")){
			File[] sourceFiles =new File(source).listFiles();
			for (int i = 0; i < sourceFiles.length; i++) {
				try {
					FileInputStream is = new FileInputStream(sourceFiles[i]);
					conn.create(path+IHTTPFSConnection.PATH_SEPERATOR+sourceFiles[i].getName(), is);
					//printResponse(conn.getOutputResponseCode());
				} catch (Exception e) {
					logger.error(e.getMessage(),e);
					printFailure(e.getMessage());
				}
			}
		}else if(command.equals("copyToLocal")){
			File destFile = new File(destination);
			if(!destFile.exists()){
				destFile.createNewFile();
			}
			FileOutputStream os = new FileOutputStream(destFile);
			
			conn.open(path, os);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("cat")){
			conn.open(path, System.out);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("mkdir")){
			conn.mkdirs(path);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("rm")){
			conn.delete(path);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("rn")){
			conn.rename(source, destination);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("status")){
			conn.getFileStatus(path);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("home")){
			conn.getHomeDirectory();
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("chmod")){
			conn.setPermission(path,mode);
			printResponse(conn.getOutputResponseCode());
		}else if(command.equals("chgrp")){
			conn.setOwner(path, owner, group);
			printResponse(conn.getOutputResponseCode());
		}else{
			usage(options);
		}
		  
		
	}catch (UnknownHostException e) {
		logger.error(e.getMessage(),e);
		printFailure(e.getMessage());
	} catch (MalformedURLException e) {
		logger.error(e.getMessage(),e);
		printFailure(e.getMessage());
	} catch (IOException e) {
		logger.error(e.getMessage(),e);
		printFailure(e.getMessage());
	} catch (AuthenticationException e) {
		logger.error(e.getMessage(),e);
		printFailure(e.getMessage());
	}
  }
  
	private static void usage(Options options) {
		// Use the inbuilt formatter class
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("HaaSHTTPFSClient", options);
	}
	
	private static void printFailure(String msg) {
		logger.error("Operation Failed : "+ msg);
		System.exit(1);
	}
	
	private static void printResponse(String msg) {
		if(msg.equals(String.valueOf(IHTTPFSConnection.ERROR_RESPONSE_CODE_BAD_REQUEST)) ||
		   msg.equals(String.valueOf(IHTTPFSConnection.ERROR_RESPONSE_CODE_FILE_NOT_FOUND)) ||
		   msg.equals(String.valueOf(IHTTPFSConnection.ERROR_RESPONSE_CODE_UNAUTHORISED)) ||
		   msg.equals(String.valueOf(IHTTPFSConnection.ERROR_RESPONSE_CODE_FORBIDDEN)) ||
		   msg.equals(String.valueOf(IHTTPFSConnection.ERROR_RESPONSE_CODE_INTERNAL_SERVER_ERROR))){
			printFailure(msg);
		}
		logger.info("HTTPFS Operation Success : "+msg);
		System.exit(0);
	}
	

}
