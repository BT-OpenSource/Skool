package com.bt.dataintegration.test.combined;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.TableProperties;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class TestLogic {
	private DBConnectImpl dbCon = new DBConnectImpl();
	private Connection con;
	protected boolean validateHostConnection(DIConfig conf) {
		if(dbCon.validateHostConnection(conf))
			return true;
		else 
			return false;
	}
	
	protected boolean testJdbcConnection(DIConfig conf) {
		con = dbCon.connect(conf);
		if(con == null)
			return false;
		else 
			return true;
	}
	
	protected boolean validateCatalogPermission(DIConfig conf) throws SQLException{
		con = dbCon.connect(conf);
		if(dbCon.validateCatalogPermission(con,conf))
			return true;
		else
			return false;
	}
	
	protected boolean testTableExistence(DIConfig conf) throws Exception {
		con = dbCon.connect(conf);
		return dbCon.validateTable(con, conf);
		
	}
	
	protected boolean validateTablePrecision(DIConfig conf) throws SQLException {
		con = dbCon.connect(conf);
		if(dbCon.getColumnDetails(con, conf) == null)
			return false;
		else 
			return true;
	}
	protected boolean testTableMetadataExistence(DIConfig conf) throws SQLException {
		con = dbCon.connect(conf);
		if(dbCon.validateTabMetadata(con, conf) == 1)
			return true;
		else
			return false;
	}
	protected boolean validateTableSize(DIConfig conf) throws SQLException {
		con = dbCon.connect(conf);
		if(dbCon.getTableSize(con, conf) == 0.0)
			return false;
		else 
			return true;
		
	}
	protected boolean validateTablePartition(DIConfig conf) throws SQLException {
		con = dbCon.connect(conf);
		if(dbCon.getTablePartition(con, conf) == null)
			return false;
		else 
			return true;
	}
	protected boolean validateTableSchema(DIConfig conf) throws SQLException {
		con = dbCon.connect(conf);
		if(dbCon.getColumnDetails(con, conf) == null)
			return false;
		else 
			return true;
	}
	protected boolean validateTableConstraints(DIConfig conf) throws SQLException{
		con = dbCon.connect(conf);
		if(dbCon.getTableConstraints(con, conf) == null)
			return false;
		else 
			return true;
	}
	protected boolean testTableMetadataStorage() {
		return true;
	}
	protected boolean validateWorkflowPath(DIConfig conf){
		DirectoryHandler.createWorkflowPath(conf);
		String workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
		String cmd = "hadoop fs -ls "+workspacePath;
		int output = Utility.executeSSH(cmd);
		if(output != 0)
			return false;
		else
			return true;
	}
	
	protected boolean validatePassFile(DIConfig conf){
		dbCon.storePassword(conf);
		String cmd = "hadoop fs -ls "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+conf.getTableName()+"/workspace/"+conf.getTableName()+"_pfile.txt";	
		int output = Utility.executeSSH(cmd);
		if(output!= 0)
			return false;
		else
			return true;
	}
	
	protected boolean validateJobPropFile(DIConfig conf) throws SQLException{
		con = dbCon.connect(conf);
		String workspacePath=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/workspace/HDI_"+conf.getSourceName()+"_"+conf.getTableOwner()+"_"+conf.getTableName();
		TableProperties tabprop = new TableProperties();
		tabprop.setColDetails(dbCon.getColumnDetails(con, conf));
		tabprop.setColCons(dbCon.getTableConstraints(con, conf));
		tabprop.setColPartition(dbCon.getTablePartition(con, conf));
		tabprop.setTableSize(dbCon.getTableSize(con, conf));
		dbCon.storeTableMetadata(tabprop, conf);
		String cmd = "hadoop fs -ls "+workspacePath+"/job.properties";	
		int output = Utility.executeSSH(cmd);
		if(output!= 0)
			return false;
		else
			return true;
	}
	protected String getPassword(DIConfig conf){
		String pwd ="";
		Properties properties = null;
		final String conffileName = "password.properties";
		try {
			properties = Utility.readConfigProperties(conffileName);

		} catch (Exception e) {
			throw new Error(e);
		}
		pwd = properties.getProperty("database_password");
		return pwd;
	}
}
