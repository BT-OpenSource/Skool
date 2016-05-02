package com.bt.dataintegration.property.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jline.internal.InputStreamReader;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class DIExportService {

	final static Logger logger = Logger.getLogger(DIExportService.class);
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
		DBConnectImpl oraImpl = new DBConnectImpl();
		TableProperties tabProp = new TableProperties();
		LinkedHashMap<String, List<String>> oraTabMap = new LinkedHashMap<String, List<String>>();
		Map<String, List<String>> tabPart = new HashMap<String, List<String>>();
		Map<String, String> colCons = new HashMap<String, String>();
		StringBuffer s= new StringBuffer();
		Set<String> updateCols = null;
		try {
			
			servConf = new DIConfig().getDIConfigProperties();
			
			String cmd1 = "mkdir "+servConf.getTableName();
			Utility.executeSSH(cmd1);
			
			Connection con = oraImpl.connect(servConf);
			boolean valTabStats;

			boolean catCheck = oraImpl.validateCatalogPermission(con, servConf);
				
			valTabStats = oraImpl.validateTable(con,servConf);
			valTabStats = oraImpl.validateStagingTable(con,servConf);
			
			oraTabMap = oraImpl.getColumnDetails(con,servConf);
			
			colCons = oraImpl.getTableConstraints(con,servConf);
			
			for (Map.Entry<String, String> oraTable : colCons.entrySet()) {
				if("P".equalsIgnoreCase(oraTable.getValue())){
					s.append(oraTable.getKey()).append(",");
				}
			}
			BufferedReader reader = null;
			if(!colCons.isEmpty()){
				if(servConf.isUpdateDatabase() == false){
					logger.warn("The table has primary key constraint.To proceed you need to provide update key column.\n Please press 'y' to continue and 'n' to halt the execution");
					reader = new BufferedReader(new InputStreamReader(System.in));
					String choice = reader.readLine();
					while((("y".equalsIgnoreCase(choice)) || ("n".equalsIgnoreCase(choice))) == false){
						choice = "";
						logger.warn("Select an option \n y - Continue \t n - Hault Execution");
						choice = reader.readLine();
					}
					if("y".equalsIgnoreCase(choice)){
						logger.info("Please provide update key column (Comma separated values in case of multiple columns). "+s.toString()+" These columns must be included in the values");
						reader = new BufferedReader(new InputStreamReader(System.in));
						String updateColumn = "";
						while((updateColumn == null) || ("".equalsIgnoreCase(updateColumn))){
							updateColumn = reader.readLine();
							if((updateColumn == null) || ("".equalsIgnoreCase(updateColumn))){
								logger.warn("Update key column cannot be null. Please provide update key column (Comma separated values in case of multiple columns). "+s.toString()+" These columns must be included in the values");
								updateColumn = "";
							}
							else{
								String[] updCols = updateColumn.toUpperCase().split(",");
								updateCols = new HashSet<String>(Arrays.asList(updCols));
								for (Map.Entry<String, String> oraTable : colCons.entrySet()) {
									if(!updateCols.contains(oraTable.getKey())){
										logger.warn("Please provide update key column (Comma separated values in case of multiple columns). "+s.toString()+" These columns must be included in the values");
										updateColumn = "";
									}
								}
							}
						}
						reader.close();
						servConf.setUpdateDatabase(true);
						servConf.setUpdateMode("updateonly");
						servConf.setUpdateKeyColumn(updateColumn);
					}
					else if(choice.equalsIgnoreCase("n")) {
						reader.close();
						DirectoryHandler.cleanUpWorkspaceExport(servConf);					
						throw new Error("Halting execution...");							
					}
				}
				else{
					String[] updCols = servConf.getUpdateKeyColumn().split(",");
					updateCols = new HashSet<String>(Arrays.asList(updCols));
					for (Map.Entry<String, String> oraTable : colCons.entrySet()) {
						if("P".equalsIgnoreCase(oraTable.getValue())){
							if(!updateCols.contains(oraTable.getKey())){
								logger.warn(s.toString()+" These columns must be included in the update_key_column");
								DirectoryHandler.cleanUpWorkspaceExport(servConf);	
								throw new Error("Halting execution...");
							}
						}
					}
				}
			}
			
			DirectoryHandler.createExportWorkflowPath(servConf);
			DirectoryHandler.createAuditLogPath(servConf);
			DirectoryHandler.createPasswordDirectory(servConf);
			
			oraImpl.storePassword(servConf);
		
			oraImpl.storeTableMetadataForExport(servConf);
			
			String cleanCmd = "mv job.properties "+servConf.getTableName()+"/job.properties";
			Utility.executeSSH(cleanCmd);
			
			HadoopConfig conf = new HadoopConfig().getHadoopConfigProperties();
			String cmd = "hadoop fs -put ojdbc6-11.2.0.3.jar " + conf.getWorkspacePath();
			int shellout = 1;
			shellout = Utility.executeSSH(cmd);
			if(shellout !=0){
				throw new Error();
			}
			
			 
			 cleanCmd = "cp configuration.properties "+servConf.getTableName()+"/configuration.properties";
			 Utility.executeSSH(cleanCmd);
		} catch (Exception e) {

			System.out.println("Error ar DIConfigService.main()");
			logger.error("Error ar DIConfigService.main()");
			DirectoryHandler.cleanUpWorkspaceExport(servConf);	
			throw new Error(e);
		}
		
	}
}
