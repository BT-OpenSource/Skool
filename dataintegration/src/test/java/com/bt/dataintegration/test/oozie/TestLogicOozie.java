package com.bt.dataintegration.test.oozie;

import java.io.FileInputStream;
import java.util.Properties;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.DIConfigService;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.property.config.TableProperties;

public class TestLogicOozie {

	final static Logger logger = Logger.getLogger(TestLogicOozie.class);
	private static Properties prop = new Properties();	
	
	public static HashMap<String, String> validateJobPropertiesFile() {
		
		boolean status = false;
		DIConfig servConf;
		DBConnectImpl oraImpl = new DBConnectImpl();
		TableProperties tabProp = new TableProperties();
		HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties();
		LinkedHashMap<String, List<String>> oraTabMap = new LinkedHashMap<String, List<String>>();
		Map<String, List<String>> tabPart = new HashMap<String, List<String>>();
		Map<String, String> colCons = new HashMap<String, String>();
		HashMap<String, String> jobProp = new HashMap<String, String>();
		
		try {				
			//Checking directory before tests
			String cmd = "hadoop fs -ls /user/" + hconf.getQueueName() + "/" + hconf.getTableName();
			int shellOut = Utility.executeSSH(cmd);
			if(shellOut == 0) {
				logger.info("Directory already exists in HDFS.\n Deleting directory before running test cases...");
				String delDir = "hadoop fs -rm -r /user/" + hconf.getQueueName() + "/" + hconf.getTableName();
				Utility.executeSSH(delDir);
			}
			
			servConf = new DIConfig().getDIConfigProperties();			
			Connection con = oraImpl.connect(servConf);
			boolean valTabStats;
				boolean catCheck = oraImpl.validateCatalogPermission(con, servConf);				
				valTabStats = oraImpl.validateTable(con,servConf);
				double tabSize = 0;
                if(servConf.getImport_export_flag().equalsIgnoreCase("1")){
				       tabSize = oraImpl.getTableSize(con,servConf);
				       tabProp.setTableSize(tabSize);
				       tabPart = oraImpl.getTablePartition(con,servConf);
					   tabProp.setColPartition(tabPart);
                }				
					oraTabMap = oraImpl.getColumnDetails(con,servConf);				
					logger.info("Setting table properties...");
					
					tabProp.setTableSize(tabSize);

					tabProp.setColDetails(oraTabMap);
					tabPart = oraImpl.getTablePartition(con,servConf);
					tabProp.setColPartition(tabPart);
					colCons = oraImpl.getTableConstraints(con,servConf);
					tabProp.setColCons(colCons);					 
					DirectoryHandler.createWorkflowPath(servConf);
					//oraImpl.storePassword(servConf);
					oraImpl.storeTableMetadata(tabProp, servConf);	
										
					prop.load(new FileInputStream("job.properties"));					
										
					for(String key : prop.stringPropertyNames()) {
						String value = prop.getProperty(key);
						jobProp.put(key, value);
					}					

		} catch (Exception e) {

			System.out.println("Error ar TestLogicOozie.validateJobPropertiesFile()");
			logger.error("Error ar TestLogicOozie.validateJobPropertiesFile()");
			e.printStackTrace();
		}
		
		return jobProp;
	}
}
