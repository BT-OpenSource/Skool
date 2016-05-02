package com.bt.dataintegration.test.combined;

import static com.bt.dataintegration.constants.Constants.AVRO_SCHEMA_URL;
import static com.bt.dataintegration.constants.Constants.SERDE_FORMAT;
import static com.bt.dataintegration.constants.Constants.SERDE_IP_FORMAT;
import static com.bt.dataintegration.constants.Constants.SERDE_OP_FORMAT;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.bt.dataintegration.hive.HiveProcessImpl;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.sqoop.ImplSqoopImport;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestCases {
	DIConfig dbConf = new DIConfig();
	TestLogic logic = new TestLogic();
	private ImplSqoopImport  implsqoop=new ImplSqoopImport();
	private HadoopConfig conf =new HadoopConfig();
	HiveProcessImpl impl = new HiveProcessImpl();
	ProcessBuilder pb = null;
	Process p = null;
	
	@Before
	public void validateGetDIConfigProperties(){
		this.dbConf = dbConf.getDIConfigProperties();
		//code to set password
		String pwd = logic.getPassword(dbConf);
		dbConf.setSourcePassword(pwd);
	}
	//Metadata Details Check
	@Test
	public void _01_validateRDBMSDetail() {
		//System.out.println(this.dbConf.getRDBMSDetail());
		Assert.assertNotNull("RDBMS detail not provided",
				dbConf.getRDBMSDetail());
	}

	//Database Host Name Check from properties file
	@Test
	public void _02_validateDBHost() {

		Assert.assertNotNull(dbConf.getSourceHostName());
	}

	//Database port check from properties file 
	@Test
	public void _03_validateDBPort() {

		Assert.assertNotNull(dbConf.getSourcePort());
	}
	
	//Validate driver class to be compatible with the JAR available in dependency
	//Should be able to validate the driver class
	//This is mandatory for making database connection
	@Test
	public void _04_validateDriverClass() {

		if (dbConf.getRDBMSDetail() == 1)
			Assert.assertEquals("oracle.jdbc.driver.OracleDriver",
					dbConf.getDBDriver());
		else if (dbConf.getRDBMSDetail() == 2)
			Assert.assertEquals("com.mysql.jdbc.Driver", dbConf.getDBDriver());
	}
	
	//Table name should not be null in the properties file
	@Test
	public void _05_validateTableName() {

		Assert.assertNotNull(dbConf.getTableName());
	}
	
	
	
	//Check database connectivity with the Host Name
	//Status returned as TRUE/FALSE
	@Test
	public void _06_validateHostConnection() {

		Assert.assertTrue(logic.validateHostConnection(dbConf));
	}

	//Try connect to database with the entire connection string
	@Test
	public void _07_testJdbcConnection() {

		Assert.assertTrue(logic.testJdbcConnection(dbConf));
	}

	@Test
	public void _08_validateCatalogPermission() throws SQLException{
		Assert.assertTrue(logic.validateCatalogPermission(dbConf));
	}
	
	//Table should exist in the source database
	//Checked by firing SQL query against the database
	@Test
	public void _09_testTableExistence() throws Exception {
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.testTableExistence(dbConf));
	}
	
	
	//Check the precision for number datatype in database
	@Test
	public void _10_validateTablePrecision() throws SQLException{
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.validateTablePrecision(dbConf));
	}

	

	//Table should have metadata entry in the database
	//Checked by firing SQL query against the database
	@Test
	public void _11_testTableMetadataExistence() throws SQLException {
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.testTableMetadataExistence(dbConf));
	}

	//Checked by firing SQL query against the database
	//required to set number of mappers in sqoop import
	@Ignore
	public void _12_validateTableSize() throws SQLException {
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.validateTableSize(dbConf));
	}

	//To check if table has or has not any partitions available
	//Checked by firing SQL query against the database
	@Test
	public void _13_validateTablePartition() throws SQLException {
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.validateTablePartition(dbConf));
	}

	//Checked by firing SQL query against the database
	//Required in creating mapping rules to create table in Hive
	@Test
	public void _14_validateTableSchema() throws SQLException {
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.validateTableSchema(dbConf));
	}

	

	//Required for detecting primary key in the database to be used as a split column in sqoop if needed
	@Test
	public void _15_validateTableConstraints() throws SQLException {
		if(logic.validateCatalogPermission(dbConf))
			this.dbConf.setDBCatalog(true);
		else
			this.dbConf.setDBCatalog(false);
		Assert.assertTrue(logic.validateTableConstraints(dbConf));
	}

	//Tobe validated at sqoop
	@Ignore
	public void _16_testTableMetadataStorage() {

		Assert.assertTrue(logic.testTableMetadataStorage());
	}
	
	
	@Test
	public void _17_validateWorkflowPath(){
		Assert.assertTrue(logic.validateWorkflowPath(dbConf));
	}
	
	@Ignore
	public void _18_validatePassFile(){
		Assert.assertTrue(logic.validatePassFile(dbConf));
	}
	
	@Test
	public void _19_validateJobPropFile() throws SQLException{
		Assert.assertTrue(logic.validateJobPropFile(dbConf));
	}
  
	/**
	 * Below test is to check whether properties file created which contains all
	 * sqoop properties is accessible or not.
	 */
  
	@Test
	public void _20_validateSqoopPropFileAccess() {
		Assert.assertEquals(true, implsqoop.validateSqoopPropFileAccess());

	}
	
	/**
	 * Validate connect string in properties file which will be used in sqoop
	 * connect options. This should not be null.
	 */
	@Test
	public void _21_validateConnectString() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getDBConnectString());
		
	}
	
	/**
	 * Validate UserName which will be used in sqoop connect. Value should not
	 * be null
	 */
	@Test
	public void _22_validateDBUsername() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getUserName());
	}
	
	/**
	 * Validate Table Name which will be used in sqoop connect. Value should not
	 * be null
	 */
	@Test
	public void _23_validateTable() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTableName());
	}
	
	/**
	 * Validate Query which can be used to fetch data from table using sqoop
	 * query options.
	 */

	@Test
	public void _24_validateQuery() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getQueryMilestone());
	}
	
	/**
	 * Validate column details in properties file and it should not be null.
	 */

	@Test
	public void _25_validateColumns() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getColumnDetails());
	}
	
	@Test
	public void _26_validateTargetDir() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTargetDirectory());
	}
	/**
	 * Validate Direct mode.if user have catlog permission and table does not
	 * contail blob and clob column then direct mode is set
	 */
	@Test
	public void _27_validateDirectMode() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getDirectMode());

	}
	
	@Test
	public void _28_validateQueueName() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getQueueName());
	}
	
	@Test
	public void _29_validateNumberOfMappers() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getNumOfMappers());
	}
	
	@Test
	public void _30_validateTargetDirYear() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTargetDirYear());
	}
	@Test
	public void _31_validateTargetDirmonth() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTargetDirMonth());
	}
	@Test
	public void _32_validateTargetDirDate() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTargetDirDate());
	}
	
	@Test
	public void _33_validateTargetDirHour() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTargetDirHour());
	}
	
	@Test
	public void _34_validateTargetDirMinute() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getTargetDirMinute());
	}
	
	//Cluster username should not be null in properties file
	@Ignore
	public void _35_validateClusterUsername() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(this.conf.getClusterUsername());
	}
	
	@Test
	public void _36_validateCreateTableQueryApp() {
		this.conf =conf.getHadoopConfigProperties();
		new HiveProcessImpl().copyHiveScriptsToHDFS(conf);
		if(conf.getSqoopFileFormat().contains("avro")){
			String hiveDirectory = "/user/" + conf.getQueueName() + "/landing/staging/"
					+ conf.getSourceName()+"/"+ conf.getTableOwner()+"/HDI_"
					+ conf.getTableName();
			String actual = "create external table if not exists " + conf.getQueueName() + "."
					+ conf.getHiveTableName()
					+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
					+ " ROW FORMAT SERDE " + "'" + SERDE_FORMAT + "'"
					+ " STORED AS INPUTFORMAT " + "'" + SERDE_IP_FORMAT + "'"
					+ " OUTPUTFORMAT " + "'" + SERDE_OP_FORMAT + "'" + " LOCATION '"
					+ /*hconf.getWorkflowNameNode() +*/ hiveDirectory + "'" + " TBLPROPERTIES " + "('"
					+ AVRO_SCHEMA_URL + "'='" /*+ hconf.getWorkflowNameNode()*/
					+ conf.getWorkspacePath() + "/HDI_" + conf.getTableName()
					+ ".avsc" + "')";
			String expected = impl.queryBuilder(conf);
			Assert.assertEquals(actual, expected);
		}
	}
	
	@Test
	public void _37_validateAddPartQueryApp() {
		this.conf =conf.getHadoopConfigProperties();
		String actual = "alter table " + conf.getQueueName() + "." + conf.getHiveTableName()
				+ " add if not exists partition (part_year='"
				+ conf.getTargetDirYear() + "',part_month='"
				+ conf.getTargetDirMonth() + "',part_day='"
				+ conf.getTargetDirDate() + "',part_hour='"
				+ conf.getTargetDirHour() + "',part_minute='"
				+ conf.getTargetDirMinute() + "')" + " location '"
				+ /*hconf.getWorkflowNameNode() +*/ conf.getTargetDirectory() + "'";
		String expected = impl.partitionBuilder(conf);
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void _38_validateAuditTableQuery() {
		this.conf =conf.getHadoopConfigProperties();
		String actual ="create external table if not exists ${audit_table_name} (${hdi_tab_cols}) row format delimited fields terminated by ',' location '${location}'";

		String expected = impl.createAuditTableQueryBuilder();
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void _39_validateTableCreation() {
		this.conf =conf.getHadoopConfigProperties();
		String cmd = "create table " + conf.getQueueName() + "." + "test_table(values String)";		
		try {
			pb = new ProcessBuilder("hive","-e",cmd);
			p = pb.start();
			int x = p.waitFor();
			Assert.assertEquals(0, x);
		} catch (Exception e) {}
	}
	
	@Test
	public void _40_validateDropTable() {
		this.conf =conf.getHadoopConfigProperties();
		String cmd = "drop table " + conf.getQueueName() + "." + "test_table";
		try {
			pb = new ProcessBuilder("hive","-e",cmd);
			p = pb.start();
			int x = p.waitFor();
			Assert.assertEquals(0, x);
		} catch (Exception e) {}
	}
	
	/*
	 * Below methods validate the values from job.properties
	 * These values should not be null
	 */
	
	//Check if sqoop target directory exists.
	@Test
	public void _41_validateTargetDirIfExists() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(conf.getTargetDirectory());
	}
	
	//Check if year is not null
	
	@Test
	public void _42_validateYear() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(conf.getTargetDirYear());
	}
	
	//Check if month is not null
	
	@Test
	public void _43_validateMonth() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(conf.getTargetDirMonth());
	}
	
	//Check if date is not null
	
	@Test
	public void _44_validateDate() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(conf.getTargetDirDate());
	}
	
	//Check if Hour is not null
	
	@Test
	public void _45_validateHour() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(conf.getTargetDirMonth());
	}
	
	//Check if minute is not null
	
	@Test
	public void _46_validateMinute() {
		this.conf =conf.getHadoopConfigProperties();
		Assert.assertNotNull(conf.getTargetDirMinute());
	}
}
