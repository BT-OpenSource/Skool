/*
 * Author: 609298143 (Manish Bajaj)
 */

package com.bt.dataintegration.test.db;

import java.sql.Connection;
import java.sql.SQLException;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.TableProperties;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

//Sample Test Bed Logic
//Test Logic will be validated from src/java/main DB Module
public class TestLogicDBAuth {

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

	protected boolean testTableExistence(DIConfig conf) throws Exception {
		con = dbCon.connect(conf);
		return dbCon.validateTable(con, conf);
		
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

	protected boolean validateTablePrecision(DIConfig conf) throws SQLException {
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
		return false;
	}
	
	protected boolean validateCatalogPermission(DIConfig conf) throws SQLException{
		con = dbCon.connect(conf);
		if(dbCon.validateCatalogPermission(con,conf))
			return true;
		else
			return false;
	}
	
	protected boolean validateWorkflowPath(DIConfig conf){
		DirectoryHandler.createWorkflowPath(conf);
		String cmd = "hadoop fs -ls "+conf.getAppNameNode()+"/user/cloudera/"+conf.getInstanceName();
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
		TableProperties tabprop = new TableProperties();
		tabprop.setColDetails(dbCon.getColumnDetails(con, conf));
		tabprop.setColCons(dbCon.getTableConstraints(con, conf));
		tabprop.setColPartition(dbCon.getTablePartition(con, conf));
		tabprop.setTableSize(dbCon.getTableSize(con, conf));
		dbCon.storeTableMetadata(tabprop, conf);
		String cmd = "hadoop fs -ls "+conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+conf.getTableName()+"/workspace/job.properties";	
		int output = Utility.executeSSH(cmd);
		if(output!= 0)
			return false;
		else
			return true;
	}
}
