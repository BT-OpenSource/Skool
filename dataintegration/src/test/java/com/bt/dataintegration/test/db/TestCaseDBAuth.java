/*
 * Author: 609298143 (Manish Bajaj)
 */

package com.bt.dataintegration.test.db;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.bt.dataintegration.property.config.DIConfig;

public class TestCaseDBAuth {

	DIConfig dbConf = new DIConfig();
	private TestLogicDBAuth dbLogic = new TestLogicDBAuth();

	@Before
	public void validateGetDIConfigProperties(){
		this.dbConf = dbConf.getDIConfigProperties();
		
	}
	//Metadata Details Check
	@Test
	public void validateRDBMSDetail() {
		//System.out.println(this.dbConf.getRDBMSDetail());
		Assert.assertNotNull("RDBMS detail not provided",
				dbConf.getRDBMSDetail());
	}

	//Database Host Name Check from properties file
	@Test
	public void validateDBHost() {

		Assert.assertNotNull(dbConf.getSourceHostName());
	}

	//Check database connectivity with the Host Name
	//Status returned as TRUE/FALSE
	@Test
	public void validateHostConnection() {

		Assert.assertTrue(dbLogic.validateHostConnection(dbConf));
	}

	//Database port check from properties file 
	@Test
	public void validateDBPort() {

		Assert.assertNotNull(dbConf.getSourcePort());
	}

	//Validate driver class to be compatible with the JAR available in dependency
	//Should be able to validate the driver class
	//This is mandatory for making database connection
	@Test
	public void validateDriverClass() {

		if (dbConf.getRDBMSDetail() == 1)
			Assert.assertEquals("oracle.jdbc.driver.OracleDriver",
					dbConf.getDBDriver());
		else if (dbConf.getRDBMSDetail() == 2)
			Assert.assertEquals("com.mysql.jdbc.Driver", dbConf.getDBDriver());
	}

	//Try connect to database with the entire connection string
	@Test
	public void testJdbcConnection() {

		Assert.assertTrue(dbLogic.testJdbcConnection(dbConf));
	}

	//Table name should not be null in the properties file
	@Test
	public void validateTableName() {

		Assert.assertNotNull(dbConf.getTableName());
	}

	//Table should exist in the source database
	//Checked by firing SQL query against the database
	@Test
	public void testTableExistence() throws Exception {
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.testTableExistence(dbConf));
	}

	//Table should have metadata entry in the database
	//Checked by firing SQL query against the database
	@Test
	public void testTableMetadataExistence() throws SQLException {
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.testTableMetadataExistence(dbConf));
	}

	//Checked by firing SQL query against the database
	//required to set number of mappers in sqoop import
	@Test
	public void validateTableSize() throws SQLException {
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.validateTableSize(dbConf));
	}

	//To check if table has or has not any partitions available
	//Checked by firing SQL query against the database
	@Test
	public void validateTablePartition() throws SQLException {
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.validateTablePartition(dbConf));
	}

	//Checked by firing SQL query against the database
	//Required in creating mapping rules to create table in Hive
	@Test
	public void validateTableSchema() throws SQLException {
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.validateTableSchema(dbConf));
	}

	//Check the precision for number datatype in database
	@Test
	public void validateTablePrecision() throws SQLException{
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.validateTablePrecision(dbConf));
	}

	//Required for detecting primary key in the database to be used as a split column in sqoop if needed
	@Test
	public void validateTableConstraints() throws SQLException {
		if(dbLogic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(dbLogic.validateTableConstraints(dbConf));
	}

	//Tobe validated at sqoop
	@Ignore
	public void testTableMetadataStorage() {

		Assert.assertTrue(dbLogic.testTableMetadataStorage());
	}
	@Test
	public void validateCatalogPermission() throws SQLException{
		Assert.assertTrue(dbLogic.validateCatalogPermission(dbConf));
	}
	
	@Test
	public void validateWorkflowPath(){
		Assert.assertTrue(dbLogic.validateWorkflowPath(dbConf));
	}
	
	@Test
	public void validatePassFile(){
		Assert.assertTrue(dbLogic.validatePassFile(dbConf));
	}
	
	@Test
	public void validateJobPropFile() throws SQLException{
		Assert.assertTrue(dbLogic.validateJobPropFile(dbConf));
	}
}
