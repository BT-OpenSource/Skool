/*
 * Author: 609298143 (Manish Bajaj),
 * 			609349708 (Abhinav Meghmala)
 *          609504665 (prabhu Om)
 */

package com.bt.dataintegration.database;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import static com.bt.dataintegration.constants.Constants.*;

import com.bt.dataintegration.property.config.*;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class DBConnectImpl implements IDBConnect {
	
	final static Logger logger = Logger.getLogger(DBConnectImpl.class);
	private Properties properties = null;
	//private String nameNode=null;
	//private String jobTracker=null;

	public DBConnectImpl() {
		logger.info("***Loading Database.Properties***");
		properties = new Properties();
		InputStream ips = null;
		//ips = DBConnectImpl.class.getClassLoader().getResourceAsStream(DATABASE_PROPERTIES);
		try {
			ips = new FileInputStream(DATABASE_PROPERTIES_FILE);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			properties.load(ips);
			
		} catch (Exception e) {
			
			//System.out.println("Error loading properties at constructor DBConnectImpl()" + e.getMessage());
			logger.error("Error loading properties at constructor DBConnectImpl() ");
			logger.error("", e);
			throw new Error(e);
		}
	}

	public Connection connect(DIConfig diConfig) {
		

		Connection connection = null;
		try {
			
			logger.info("****************************");
			logger.info("***Logs for DBConnectImpl***");
			logger.info("****************************");

			Class.forName(ORACLE_DRIVER_NAME);

		} catch (ClassNotFoundException e) {

			logger.error("Oracle Driver class not found");
			logger.error("", e);
			throw new Error(e);
			
			//return connection;

		}
		try {

			/*String connectionString = ORACLE_DRIVER_TYPE + ":@"
					+ diConfig.getSourceHostName() + ":"
					+ diConfig.getSourcePort() + "/" + diConfig.getSourceSid()
					+ ", " + diConfig.getSourceUsername() + ", "
					+ diConfig.getSourcePassword();*/

			//System.out.println("Connecting as: " + connectionString);
			String logConnectString = ORACLE_DRIVER_TYPE + ":@"
					+ diConfig.getSourceHostName() + ":"
					+ diConfig.getSourcePort() + "/" + diConfig.getSourceSid()
					+ ", " + "*****" + ", "
					+ "*****";
			logger.debug("Connecting as: " + logConnectString);

			connection = DriverManager.getConnection(
					ORACLE_DRIVER_TYPE + ":@" + diConfig.getSourceHostName()
							+ ":" + diConfig.getSourcePort() + "/"
							+ diConfig.getSourceSid(),
					diConfig.getSourceUsername(), diConfig.getSourcePassword());

		} catch (SQLException e) {

			
			logger.error("Connection Failed!");
			logger.error("", e);
			throw new Error(e);
			//return connection;
		}

		if (connection != null) {
			//System.out.println("You made it, take control your database now!");
			logger.info("Successfully connected to the database");
			return connection;
		} else {
			
			logger.error("Failed to make connection!");
			//logger.error(e);
			StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
			for(StackTraceElement stackTrace: stackTraceElements){
			    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
			}
			throw new Error();
			//return connection;
		}
	}
	
	/*public String getTableOwner(Connection con, DIConfig conf) throws SQLException{
		logger.info("get table owner");
		String ownerName = null;
		String getTableOwnerQuery="";
		getTableOwnerQuery = properties.getProperty(ORACLE_TABLE_OWNER); 
		PreparedStatement statement = con.prepareStatement(getTableOwnerQuery);
				
		try {

			statement.setString(1, conf.getTableName().toUpperCase());

			//System.out.println(validTableQuery);
			logger.debug("getTableOwnerQuery --"+getTableOwnerQuery);

			// execute insert SQL stetement

			ResultSet rs = statement.executeQuery();
			rs.next();
			ownerName = rs.getString(1); // get the count to do object
											// Validation
			//System.out.println(rowCount);
			//logger.info(rowCount);
			// if there is
			if (ownerName == null) {
				logger.error("No Object found, Please check the Schema Name And Table Name");
				StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
				for(StackTraceElement stackTrace: stackTraceElements){
				    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
				}
				throw new Error();
			}

			//System.out.println("Table Exists");
			//logger.info("Table Exists");

			return ownerName;
		} catch (SQLException e) {

			logger.error("SQL Exception at getTableOwner()");
			logger.error("", e);
			throw new Error(e);
		} finally {

			if (statement != null) {
				statement.close();
			}


		}
	}
	*/
	
	public boolean validateTable(Connection con, DIConfig conf) throws Exception {
		String validTableQuery="";
		if(conf.isDBCatalog())
			validTableQuery = properties.getProperty(ORACLE_VALIDATE_TAB_CAT); 
		else
			validTableQuery = properties.getProperty(ORACLE_VALIDATE_TAB); 
		PreparedStatement statement = con.prepareStatement(validTableQuery);
				
		try {

			
			statement.setString(1, conf.getTableOwner().toUpperCase());
			statement.setString(2, conf.getTableName().toUpperCase());

			logger.debug("validTableQuery --"+validTableQuery);


			ResultSet rs = statement.executeQuery();
			rs.next();
			int rowCount = rs.getInt(1); 
			if (rowCount == 0) {
				logger.error("No Object found, Please check the Schema Name And Table Name");
				StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
				for(StackTraceElement stackTrace: stackTraceElements){
				    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
				}
				throw new Error();
			}

			logger.info("Table Exists");
			return true;
		} catch (SQLException e) {

			logger.error("SQL Exception at validateTable()");
			logger.error("", e);
			throw new Error(e);
		} finally {

			if (statement != null) {
				statement.close();
			}


		}
	}

	public LinkedHashMap<String, List<String>> getColumnDetails(Connection con, DIConfig conf) throws SQLException {

		PreparedStatement preparedStatement = null;
		LinkedHashMap<String, List<String>> colDetails = new LinkedHashMap<String, List<String>>();
		
		String selectSQL="";
		if(conf.isDBCatalog())
			selectSQL = properties.getProperty(ORACLE_COL_DETAILS_CAT); 
		else
			selectSQL = properties.getProperty(ORACLE_COL_DETAILS); 
		

		try {

			preparedStatement = con.prepareStatement(selectSQL);
			preparedStatement.setString(1, conf.getTableOwner().toUpperCase());
			preparedStatement.setString(2, conf.getTableName().toUpperCase());

			//System.out.println(selectSQL);
			logger.debug("query to get column details--"+selectSQL);

			// execute insert SQL stetement
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				List<String> colDatatypDetail = new LinkedList<String>();
				colDatatypDetail.add(rs.getString("DATA_TYPE"));
				colDatatypDetail.add(rs.getString("DATA_PREC_LEN"));
				colDetails.put(rs.getString("COLUMN_NAME").toUpperCase(), colDatatypDetail);

			}
			logger.debug("Table's column and data type details --"+colDetails.toString());
			return colDetails;

		} catch (SQLException e) {

			logger.error("SQL Exception at getColumnDetails()");
			logger.error("", e);
			throw new Error(e);
			//return null;
		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}


		}
	}

	public double getTableSize(Connection con, DIConfig conf)
			throws SQLException {

		PreparedStatement preparedStatement = null;
		double tabSize = 0.0;
		
		String selectSQL="";
		if(conf.isDBCatalog())
			selectSQL = properties.getProperty(ORACLE_TAB_SIZE_CAT); 
		else
			selectSQL = properties.getProperty(ORACLE_TAB_SIZE); 
		

		try {

			preparedStatement = con.prepareStatement(selectSQL);
			preparedStatement.setString(1, conf.getTableName().toUpperCase());

			//System.out.println(selectSQL);
			logger.debug("query to get table size -- "+selectSQL);

			// execute insert SQL stetement
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {

				String bytes = rs.getString(1);
				tabSize = Double.parseDouble(bytes);
			}
			
			logger.debug("Tables size is -- "+tabSize);
			return tabSize;
		}catch (NullPointerException e) {

			logger.error("Tablesize details not found defaulting it to 0 bytes");
			//logger.error("", e);
			//System.exit(0);
			tabSize = 0;
			return tabSize;
		} 
		catch (SQLException e) {

			logger.error("SQL Exception at getTableSize()");
			logger.error("", e);
			//System.exit(0);
			return tabSize;
		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}

		}
	}

	public Map<String, List<String>> getTablePartition(Connection con, DIConfig conf) throws SQLException {

		PreparedStatement preparedStatement = null;

		String selectSQL="";
		/*if(conf.isDBCatalog())
			selectSQL = properties.getProperty(ORACLE_TAB_PART_DETAILS_CAT); 
		else*/
			selectSQL = properties.getProperty(ORACLE_TAB_PART_DETAILS); 
		
		
		
		Map<String, List<String>> colParts = new HashMap<String, List<String>>();
		try {

			preparedStatement = con.prepareStatement(selectSQL);
			preparedStatement.setString(1, conf.getTableOwner().toUpperCase());
			preparedStatement.setString(2, conf.getTableName().toUpperCase());
			logger.debug("query to get table's partition details -- "+selectSQL);

			// execute insert SQL stetement
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				List<String> partDetails = new LinkedList<String>();
				String partType = rs.getString(1);
				String subPartType = rs.getString(2);
				String colname = rs.getString(3);

				partDetails.add(partType);
				partDetails.add(subPartType);
				colParts.put(colname, partDetails);
			}
			logger.debug("Table's partition details is -- "+colParts.toString());
			return colParts;
		} catch (SQLException e) {

			logger.error("SQL Exception at getTablePartition()");
			logger.error("", e);
			//System.exit(0);
			return colParts;
		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}


		}
	}

	public Map<String, String> getTableConstraints(Connection con, DIConfig conf) throws SQLException {

		PreparedStatement preparedStatement = null;


		String selectSQL="";
		if(conf.isDBCatalog())
			selectSQL = properties.getProperty(ORACLE_TAB_CONST_DETAILS_CAT); 
		else
			selectSQL = properties.getProperty(ORACLE_TAB_CONST_DETAILS); 
		
		
		Map<String, String> colCons = new HashMap<String, String>();
		try {

			preparedStatement = con.prepareStatement(selectSQL);
			preparedStatement.setString(1, conf.getTableName().toUpperCase());
			preparedStatement.setString(2, conf.getTableOwner().toUpperCase());

			//System.out.println(selectSQL);
			logger.debug("query to get table's constraint details -- "+selectSQL);

			// execute insert SQL stetement
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {

				String colName = rs.getString("COLUMN_NAME").toUpperCase();
				String consType = rs.getString("CONSTRAINT_TYPE");
				colCons.put(colName, consType);

			}
			logger.debug("Tables constraint details is -- "+colCons.toString());
			return colCons;
		} catch (SQLException e) {

			logger.error("SQL Exception at getTableConstraints()");
			logger.error("", e);
			//lSystem.exit(0);
			return colCons;
		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}



		}
	}
	public int validateTabMetadata(Connection con,DIConfig conf) throws SQLException{
		logger.info("Validating Table metadata");
		PreparedStatement preparedStatement = null;
		int count = 0;
		preparedStatement = con.prepareStatement(properties.getProperty(ORACLE_TAB_METATDATA));
		preparedStatement.setString(1, conf.getTableName().toUpperCase());
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {

			count = rs.getInt("METACOUNT");
		}
		return count;
	}
	
	
	public boolean validateHostConnection(DIConfig conf){
		logger.info("validating host connection");
		String cmd =properties.get(ORACLE_HOST_CONNECTION).toString() + " "+ conf.getSourceHostName();
		int output = Utility.executeSSH(cmd);
		if(output != 0)
			return false;
		else 
			return true;
	}
	
	public void storePassword(DIConfig conf){
		
		logger.info("Storing password file");
		int shellout1 = 1;
		String passFileName = conf.getSourceName()+"_"+conf.getTableOwner()+"_pfile.txt";
		String cmd1 = "hadoop fs -ls /user/"+conf.getInstanceName()+"/HDI_Password_Repository/"+passFileName;
		shellout1 = Utility.executeSSH(cmd1);
		if(shellout1 ==SHELL_SUCCESS){
			logger.warn("Password file with the name "+passFileName+" already exists. Same file will be used for sqoop import");
			DirectoryHandler.createNewFile(conf, passFileName, conf.getSourcePassword());
		}
		else{
		
			
			//DirectoryHandler.sendFileToHDFS(conf, passFileName);
			//DirectoryHandler.givePermissionToHDFSFile(conf, passFileName);
			DirectoryHandler.createNewFile(conf, passFileName, conf.getSourcePassword());
			String cmd="";
			String passwordPath= conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/HDI_Password_Repository/";
			cmd = "hadoop fs -put "+conf.getTableName()+"/"+passFileName+" "+passwordPath+passFileName;
			int shellout = 1;
			shellout = Utility.executeSSH(cmd);
			if(shellout !=SHELL_SUCCESS){
				logger.error("Failed to send password file to HDFS");
				throw new Error();
			}
			logger.info("Giving permission to password file");
			cmd = "hadoop fs -chmod 600 "+passwordPath+passFileName;
			shellout = 1;
			shellout = Utility.executeSSH(cmd);
			if(shellout !=SHELL_SUCCESS){
				logger.error("Failed to send password file to HDFS");
				throw new Error();
			}
			
					
		}
		
	}
	
	public boolean validateCatalogPermission(Connection con,DIConfig conf) throws SQLException{
		logger.info("validating catalog permission");
		PreparedStatement preparedStatement = null;
		int count = 0;
		preparedStatement = con.prepareStatement(properties.getProperty(ORACLE_CHECK_CAT));
		preparedStatement.setString(1, conf.getSourceUsername().toUpperCase());
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {

			count = rs.getInt("catcount");
		}
		if(count == 1){
			logger.info("User Has Catalog permission");
			conf.setDBCatalog(true);
			return true;
		}
		else{
			logger.info("User doesn't Has Catalog permission");
			conf.setDBCatalog(false);
			return false;
		}
	}
	
	
	public String[] sqoopQueryBuilder(TableProperties tabProp , DIConfig conf){
		logger.info("Building Sqoop Query");
		String[] result = {"","",""};
		LinkedHashMap<String, List<String>> colDetails = tabProp.getColDetails();
		List<String> userSelectedColumns = conf.getColumnDetails();
		StringBuffer query = new StringBuffer("select ");
		StringBuffer sqoopCols = new StringBuffer();
		StringBuffer mapColJava = new StringBuffer();
		if(userSelectedColumns.size() == 1 && userSelectedColumns.get(0).equals("")){
			userSelectedColumns = new LinkedList<String>();
			for (String col : colDetails.keySet()) {
				userSelectedColumns.add(col);
			}
		}
			
		
		for(String column : userSelectedColumns){
			if((colDetails.get(column).get(0).equalsIgnoreCase("DATE")) || (colDetails.get(column).get(0).equalsIgnoreCase("TIMESTAMP"))){
				String dateColumn = "TO_CHAR("+column+",'yyyy-mm-dd hh24:mi:ss') as "+column;
				query.append(dateColumn).append(",");
				sqoopCols.append(column).append(",");
				mapColJava.append(column).append("=String,");
			}
			else if((colDetails.get(column).get(0).equalsIgnoreCase("TIMESTAMP")) || (colDetails.get(column).get(0).equalsIgnoreCase("DATE"))|| (colDetails.get(column).get(0).equalsIgnoreCase("BLOB"))|| (colDetails.get(column).get(0).equalsIgnoreCase("CLOB"))){
				mapColJava.append(column).append("=String,");
			}
			else{
				query.append(column).append(",");
				sqoopCols.append(column).append(",");
			}
		}
		
		int lastComma = query.lastIndexOf(",");
		query.replace(lastComma,lastComma+1,"");
		
		if(sqoopCols.length() != 0){
			lastComma = sqoopCols.lastIndexOf(",");
			sqoopCols.replace(lastComma,lastComma+1,"");
		}
		
		if(mapColJava.length() != 0){
			lastComma = mapColJava.lastIndexOf(",");
			mapColJava.replace(lastComma,lastComma+1,"");
		}
		query.append(" from ").append(conf.getTableOwner()).append(".").append(conf.getTableName()).append(" where ");
		logger.debug("Sqoop query -- "+ query);
		logger.debug("Sqoop Columns --"+sqoopCols);
		logger.debug("Sqoop Map Column Java --"+mapColJava);
		//System.out.println("query---"+query);
		result[0] = query.toString();
		result[1] = sqoopCols.toString();
		result[2] = mapColJava.toString();
		return result;
	}
	
	public void storeTableMetadata(TableProperties tabProp, DIConfig conf){

		logger.info("Storing Table metadata in job.properties");
		Properties prop = new Properties();
		OutputStream fout = null;
		//String fileName = conf.getTableName()+"/job.properties";
		//String fileName = "job.properties";
		StringBuffer s= new StringBuffer();
		int clobBlobFlag = 0;
		String workspacePath="${nameNode}/user/${queueName}/workspace/HDI_${source_name}_${schema_name}_${tableName}";
		String landingDir="";
		String landingDirApp = "";
		String interimDir = conf.getInterimLandingDir();
		if("".equals(interimDir)){
			landingDir="${nameNode}/user/${queueName}/${source_name}/${schema_name}/HDI_${tableName}";
			landingDirApp=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+conf.getSourceName()+"/"+conf.getTableOwner()+"/HDI_"+conf.getTableName();
		}else{
			landingDir="${nameNode}/user/${queueName}/"+interimDir+"/${source_name}/${schema_name}/HDI_${tableName}";
			landingDirApp=conf.getAppNameNode()+"/user/"+conf.getInstanceName()+"/"+interimDir+"/"+conf.getSourceName()+"/"+conf.getTableOwner()+"/HDI_"+conf.getTableName();
		}
		String current_date = DirectoryHandler.targetDirDate+"-"+DirectoryHandler.targetDirMonthWords+"-"+DirectoryHandler.targetDirYear+" "+DirectoryHandler.targetDirHour+":"+DirectoryHandler.targetDirMinute;
		try {

			fout = new FileOutputStream(JOB_PROP_FILE);
			prop.setProperty("import_export_flag", conf.getImport_export_flag());
			prop.setProperty("nameNode",conf.getWorkflowNameNode());
			prop.setProperty("jobTracker",conf.getJobTracker());
			prop.setProperty("queueName",conf.getInstanceName());
			prop.setProperty("oozie.use.system.libpath","TRUE");
			//prop.setProperty("wf_application_path","${nameNode}/user/${queueName}/workspace");
			prop.setProperty("wf_application_path",workspacePath);
			if(conf.isCoordinatorFlag()){
				prop.setProperty("oozie.coord.application.path",workspacePath);
				prop.setProperty("coordinator_required", "true");
				prop.setProperty("start",conf.getWfStartTime());
				prop.setProperty("end",conf.getWfEndTime());
				prop.setProperty("timezone",conf.getTimeZone());
				prop.setProperty("concurrency",conf.getConcurrency());
				prop.setProperty("throttle",conf.getThrottle());
				prop.setProperty("timeout",conf.getTimeout());
				prop.setProperty("frequency",conf.getFrequency());
			}
			else{
				prop.setProperty("oozie.wf.application.path",workspacePath);
				prop.setProperty("coordinator_required", "false");
			}
			
			
			prop.setProperty("success_emails",conf.getSuccessEmailId());
			prop.setProperty("failure_emails",conf.getFailureEmailId());
			prop.setProperty("tableSize",tabProp.getTableSize().toString());
			prop.setProperty("landingDirApp",landingDirApp);
			
			logger.info("Setting properties for job.properties...");
			
			for (Map.Entry<String, List<String>> oraTable : tabProp.getColDetails().entrySet()) {
				s.append(oraTable.getKey()).append(":").append(oraTable.getValue().get(0)).append(":").append(oraTable.getValue().get(1)).append("|");
				if((oraTable.getValue().get(0).equalsIgnoreCase("CLOB")) ||(oraTable.getValue().get(0).equalsIgnoreCase("BLOB")))
					clobBlobFlag = 1;
			}
			if(s.length() != 0)
				prop.setProperty("columnDetails",s.substring(0, s.length()-1).toUpperCase());
			else
				prop.setProperty("columnDetails",s.toString().toUpperCase());
			s.setLength(0);
			
			
			for (Map.Entry<String, String> oraTable : tabProp.getColCons().entrySet()) {
				s.append(oraTable.getKey()).append(":").append(oraTable.getValue()).append("|");
			}
			if(s.length() != 0)
				prop.setProperty("consDetails",s.substring(0, s.length()-1).toUpperCase());
			else
				prop.setProperty("consDetails",s.toString().toUpperCase());
			s.setLength(0);
			
			
			for (Map.Entry<String, List<String>> oraTable : tabProp.getColPartition().entrySet()) {
				s.append(oraTable.getKey()).append(":").append(oraTable.getValue().get(0)).append(":").append(oraTable.getValue().get(1)).append("|");
			}
			if(s.length() != 0)
				prop.setProperty("partDetails",s.substring(0, s.length()-1).toUpperCase());
			else
				prop.setProperty("partDetails",s.toString().toUpperCase());
			s.setLength(0);
			
			//// hive text columns
			if(!(conf.getSqoopFileFormat().equals("avrodatafile"))){
				Map<String, String> hiveMappedTable = new LinkedHashMap<String, String>();
				List<String> userSelectedColumns = conf.getColumnDetails();
				LinkedHashMap<String, List<String>> userCols = new LinkedHashMap<String, List<String>>();
				if(userSelectedColumns.size() == 1 && userSelectedColumns.get(0).equals("")){
					hiveMappedTable = Utility.getMappedTable(tabProp.getColDetails());
				}
				else{
					for(String col : userSelectedColumns){
						userCols.put(col,tabProp.getColDetails().get(col));
					}
					hiveMappedTable = Utility.getMappedTable(userCols);
				}
				
				for (Map.Entry<String, String> oraTable : hiveMappedTable.entrySet()) {
					s.append(oraTable.getKey()).append(" ").append(oraTable.getValue()).append(",");
				}
				s.setLength(s.length() - 1);
				prop.setProperty("hiveTextColumns", s.toString().toUpperCase());
				s.setLength(0);
			}
			//// hive text columns
			
			prop.setProperty("tableName", conf.getTableName());
			
			if(conf.getColumnDetails() != null)
				for(String temp : conf.getColumnDetails()){
					s.append(temp).append(",");
				}
			prop.setProperty("userSelectedColumns", s.toString().toUpperCase());
			s.setLength(0);
			
			String[] query = sqoopQueryBuilder(tabProp , conf);
			String queryMilestone = "";
			if(conf.getLastModifiedDateColumn().equals("")){
				queryMilestone = query[0] +" rownum = 1 and $CONDITIONS";
			}
			else{
				queryMilestone = query[0] + conf.getLastModifiedDateColumn() + " > '01-JAN-1000' and " + conf.getLastModifiedDateColumn() +" <= TO_DATE('"+current_date+"','dd-mm-yyyy hh24:mi') and rownum = 1 and $CONDITIONS";
			}
			prop.setProperty("queryMilestone", queryMilestone);
			
			String queryIncremental = query[0] + "${lastModifiedDateColumn} > '${lastModifiedDateValueLowerBound}' and ${lastModifiedDateColumn} <= '${lastModifiedDateValueUpperBound}' and $CONDITIONS";
			//prop.setProperty("queryIncremental", queryIncremental);
			//logger.info(queryIncremental);
			
			//prop.setProperty("sqoopWhereClause",conf.getLastModifiedDateColumn() + " > '01-JAN-1000' and " + conf.getLastModifiedDateColumn() +" <= TO_DATE('"+current_date+"','dd-mm-yyyy hh24:mi')");
			
			//prop.setProperty("whereClause","${lastModifiedDateColumn} > '${lastModifiedDateValueLowerBound}' and ${lastModifiedDateColumn} <= '${lastModifiedDateValueUpperBound}'");
			
			prop.setProperty("sqoopColumns",query[1]);
			prop.setProperty("sqoopTableName", conf.getTableOwner()+"."+conf.getTableName());
			prop.setProperty("hiveTableName", conf.getTableName());
			prop.setProperty("source_name", conf.getSourceName());
			prop.setProperty("schema_name", conf.getTableOwner());
			prop.setProperty("mapColumnJava",query[2]);
			
			s.append(ORACLE_DRIVER_TYPE).append(":@").append(conf.getSourceHostName()).append(":").append(conf.getSourcePort()).append("/").append(conf.getSourceSid());
			prop.setProperty("DBConnectString", s.toString());
			
			prop.setProperty("userName", conf.getSourceUsername());
			prop.setProperty("database_host",conf.getSourceHostName());
			//prop.setProperty("password", "${nameNode}/user/${queueName}/HDI_Password_Repository/${source_name}_${schema_name}_pfile.txt");
			//String alias = "hdi." + conf.getSourceName() + "." + conf.getTableOwner() + ".pswd";
			String alias = "hdi_" +conf.getSourceHostName()+"_"+conf.getSourceName() + "_" + conf.getTableOwner() + ".pswd"; 
			prop.setProperty("password_alias", alias);
			
			String pwdProviderPath = JCEKS + "/" + conf.getInstanceName() + "/HDI_Password_Repository/${password_alias}";
			prop.setProperty("pwd_provider_path", pwdProviderPath);
			
			prop.setProperty("targetDirYear", DirectoryHandler.targetDirYear);
			prop.setProperty("targetDirMonth", DirectoryHandler.targetDirMonth);
			prop.setProperty("targetDirDate", DirectoryHandler.targetDirDate);
			prop.setProperty("targetDirHour", DirectoryHandler.targetDirHour);
			prop.setProperty("targetDirMinute", DirectoryHandler.targetDirMinute);
			
			prop.setProperty("targetDirectory", landingDir);
			
			//prop.setProperty("targetDirectorySqoop", "${nameNode}/user/${queueName}/${tableName}/landing/raw_data");
			
			prop.setProperty("numOfMappers", String.valueOf(conf.getNumOfMapper()));
			String col = conf.getSplitByColumn();
			if(col.equalsIgnoreCase("")){
				col = setSplitbyCloumn(conf, tabProp);
				conf.setSplitByColumn(col);
			}
			
			if(("".equals(col)) && (conf.getNumOfMapper() != 1)){
				prop.setProperty("numOfMappers", "1");
				logger.warn("Split-by column not specified. \nNo primary Key found. \nSetting number of mappers to 1");
			}
			prop.setProperty("splitByColumn", col);
			
			/*if((!conf.getSplitByColumn().equals("")) && (conf.isDBCatalog()) && (clobBlobFlag == 0)){
				conf.setDirect(true);
				prop.setProperty("directMode", "true");
			}*/
			if((conf.isDBCatalog()) && (clobBlobFlag == 0)){
				conf.setDirect(true);
				prop.setProperty("directMode", "true");
			}
			else{
				conf.setDirect(false);
				prop.setProperty("directMode", "false");
			}
			if("".equals(conf.getLastModifiedDateColumn())){
				prop.setProperty("milestone_everytime", "true");
				prop.setProperty("lastModifiedDateColumn", conf.getLastModifiedDateColumn());
			}
			else{
				prop.setProperty("milestone_everytime", "false");
				prop.setProperty("lastModifiedDateColumn", conf.getLastModifiedDateColumn());
			}
			prop.setProperty("lastModifiedDateValueLowerBound", "01-JAN-1000");
			
			//prop.setProperty("lastModifiedDateValueUpperBound", "TO_DATE('"+current_date+"','dd-mm-yyyy hh24:mi')");
			
			prop.setProperty("lastModifiedDateValueUpperBound", DirectoryHandler.targetDirDate+"-"+DirectoryHandler.targetDirMonthWords+"-"+DirectoryHandler.targetDirYear);
			
			
			
			prop.setProperty("shell_file",UPDATE_LAST_COL_VALUE_SCRIPT);
			prop.setProperty("shell_file_path", "${wf_application_path}/"+getAbsoluteName(UNIX_DATE_FILE));
			prop.setProperty("oraJarPath", "${wf_application_path}/"+getAbsoluteName(OJDBC_JAR)+"#"+getAbsoluteName(OJDBC_JAR));
			prop.setProperty("pigScript", "${wf_application_path}/HDI_${tableName}_COMPRESS_DATA.pig");
			prop.setProperty("hiveCreateScript", "${wf_application_path}/HDI_${tableName}_CREATE_TABLE.hql");
			prop.setProperty("hiveAddPartScript", "${wf_application_path}/HDI_${tableName}_ADD_PARTITION.hql");
			//prop.setProperty("hiveAddPart", "${nameNode}/user/${queueName}/${tableName}/landing/DELTA_DATA/${targetDirYear}/${targetDirMonth}/${targetDirDate}/${targetDirHour}/${targetDirMinute}");
			prop.setProperty("kerberos_flag", conf.getEnvDetails());
			prop.setProperty("hive2_jdbc_url", conf.getHive2_jdbc_url()+"${queueName}");
			prop.setProperty("hive2_server_principal", conf.getHive2_server_principal());
			prop.setProperty("application_nameNode", conf.getAppNameNode());
			prop.setProperty("oozie_action_sharelib_for_hive", "hive2");
			prop.setProperty("oozie_launcher_action_main_class", "org.apache.oozie.action.hadoop.Hive2Main");
			prop.setProperty("sqoopFileFormat", "--as-"+conf.getSqoopFileFormat());
			prop.setProperty("shell_file_init", "refresh_last_col_value.sh");
			prop.setProperty("audit_log_file_path","${nameNode}/user/${queueName}/HDI_AUDIT/audit_logs.txt");
			prop.setProperty("audit_log_path","${nameNode}/user/${queueName}/HDI_AUDIT");
			prop.setProperty("audit_shell_file",AUDIT_LOG_SCRIPT);
			prop.setProperty("error_shell_file",ERROR_LOG_SCRIPT);
			prop.setProperty("audit_table_name","HDI_AUDIT");
			prop.setProperty("housekeeping_shell_file",HOUSE_KEEPING_SCRIPT);
			prop.setProperty("hive_create_audit_table","HDI_CREATE_AUDIT_TABLE.hql");
			//String auditTableCols = "WORKFLOW_ID STRING,WORKFLOW_NAME STRING,RUN_NO STRING,JOB_START_TIME STRING,JOB_END_TIME STRING,ORACLE_TABLE_NAME STRING,SQOOP_IE_FLAG STRING,HADOOP_RAW_DATA_DIR STRING,HADOOP_FINAL_DATA_DIR STRING,RECORD_COUNT STRING";
			String auditTableCols = HDI_AUDIT_COLS;

			prop.setProperty("hdi_audit_table_cols",auditTableCols);
			
			
			if(conf.getRetentionRawData() == -1)
				prop.setProperty("retention_period_raw_data", "");
			else
				prop.setProperty("retention_period_raw_data", String.valueOf(conf.getRetentionRawData()));
			/*if(conf.getRetentionProcessedData() == -1)
				prop.setProperty("retention_period_processed_data", "");
			else
				prop.setProperty("retention_period_processed_data", String.valueOf(conf.getRetentionProcessedData()));*/
			
			prop.store(fout, null);
			
			logger.debug("Property set as: " + JOB_PROP_FILE);
			
		} catch (Exception e) {
			logger.error("Error Setting properties at JobProperties.storeProperties()");
			logger.error("", e);
			DirectoryHandler.cleanUpWorkspace(conf);
			DirectoryHandler.cleanUpLanding(conf);
			throw new Error(e);
			
		}
		finally{
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException e) {
					logger.error("Error Setting properties at JobProperties.storeProperties()");
					logger.error(e);
					/*StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
					for(StackTraceElement stackTrace: stackTraceElements){
					    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
					}*/
					DirectoryHandler.cleanUpWorkspace(conf);
					DirectoryHandler.cleanUpLanding(conf);
					throw new Error(e);
				}
			}
		}

		DirectoryHandler.sendFileToHDFS(conf, JOB_PROP_FILE);
		DirectoryHandler.givePermissionToHDFSFile(conf,JOB_PROP_FILE);
	}
	
	
	public void storeTableMetadataForExport(DIConfig conf){

		logger.info("Storing Table metadata in job.properties");
		Properties prop = new Properties();
		OutputStream fout = null;
		//String fileName = conf.getTableName()+"/job.properties";
		//String fileName = "job.properties";
		StringBuffer s= new StringBuffer();
		String workspacePath="${nameNode}/user/${queueName}/workspace/HDI_${source_name}_${schema_name}_${tableName}_EXPORT";
		try {

			fout = new FileOutputStream(JOB_PROP_FILE);
			prop.setProperty("import_export_flag", conf.getImport_export_flag());
			prop.setProperty("nameNode",conf.getWorkflowNameNode());
			prop.setProperty("jobTracker",conf.getJobTracker());
			prop.setProperty("queueName",conf.getInstanceName());
			prop.setProperty("oozie.use.system.libpath","TRUE");
			//prop.setProperty("wf_application_path","${nameNode}/user/${queueName}/workspace");
			prop.setProperty("wf_application_path",workspacePath);
			if(conf.isCoordinatorFlag()){
				prop.setProperty("oozie.coord.application.path",workspacePath);
				prop.setProperty("coordinator_required", "true");
			}
			else{
				prop.setProperty("oozie.wf.application.path",workspacePath);
				prop.setProperty("coordinator_required", "false");
			}
			prop.setProperty("start",conf.getWfStartTime());
			prop.setProperty("end",conf.getWfEndTime());
			prop.setProperty("timezone",conf.getTimeZone());
			prop.setProperty("success_emails",conf.getSuccessEmailId());
			prop.setProperty("failure_emails",conf.getFailureEmailId());
			//prop.setProperty("tableSize",tabProp.getTableSize().toString());
			prop.setProperty("concurrency",conf.getConcurrency());
			prop.setProperty("throttle",conf.getThrottle());
			prop.setProperty("timeout",conf.getTimeout());
			prop.setProperty("frequency",conf.getFrequency());
			
			logger.info("Setting properties for job.properties...");
			
						
			prop.setProperty("tableName", conf.getTableName());
			prop.setProperty("source_name", conf.getSourceName());
			prop.setProperty("schema_name", conf.getTableOwner());
			prop.setProperty("DBConnectString", s.toString());
			
			prop.setProperty("userName", conf.getSourceUsername());
			
			prop.setProperty("password", "${nameNode}/user/${queueName}/HDI_Password_Repository/${source_name}_${schema_name}_pfile.txt");

			prop.setProperty("targetDirYear", DirectoryHandler.targetDirYear);
			prop.setProperty("targetDirMonth", DirectoryHandler.targetDirMonth);
			prop.setProperty("targetDirDate", DirectoryHandler.targetDirDate);
			prop.setProperty("targetDirHour", DirectoryHandler.targetDirHour);
			prop.setProperty("targetDirMinute", DirectoryHandler.targetDirMinute);
			
			prop.setProperty("numOfMappers", String.valueOf(conf.getNumOfMapper()));
			
		
			prop.setProperty("oraJarPath", "${wf_application_path}/ojdbc6-11.2.0.3.jar#ojdbc6-11.2.0.3.jar");
			prop.setProperty("kerberos_flag", conf.getEnvDetails());
			prop.setProperty("hive2_jdbc_url", conf.getHive2_jdbc_url()+"${queueName}");
			prop.setProperty("hive2_server_principal", conf.getHive2_server_principal());
			prop.setProperty("application_nameNode", conf.getAppNameNode());
			prop.setProperty("oozie_action_sharelib_for_hive", "hive2");
			prop.setProperty("oozie_launcher_action_main_class", "org.apache.oozie.action.hadoop.Hive2Main");
			prop.setProperty("sqoopFileFormat", "--as-"+conf.getSqoopFileFormat());
			prop.setProperty("audit_log_file_path","${nameNode}/user/${queueName}/HDI_AUDIT/audit_logs.txt");
			prop.setProperty("audit_log_path","${nameNode}/user/${queueName}/HDI_AUDIT");
			prop.setProperty("audit_shell_file","audit_logs.sh");
			prop.setProperty("error_shell_file","error_logs.sh");
			prop.setProperty("audit_table_name","HDI_AUDIT");
			prop.setProperty("hive_create_audit_table","HDI_CREATE_AUDIT_TABLE.hql");
			String auditTableCols = "WORKFLOW_ID STRING,WORKFLOW_NAME STRING,RUN_NO STRING,JOB_START_TIME STRING,JOB_END_TIME STRING,ORACLE_TABLE_NAME STRING,SQOOP_IE_FLAG STRING,HADOOP_RAW_DATA_DIR STRING,RECORD_COUNT STRING,JOB_STATUS STRING,ERROR_MESSAGE STRING,ERROR_CODE STRING";

			prop.setProperty("hdi_audit_table_cols",auditTableCols);
			
			prop.setProperty("exportDirectory",conf.getExportDir());
			prop.setProperty("update_key_column",conf.getUpdateKeyColumn());
			prop.setProperty("update_mode", conf.getUpdateMode());
			prop.setProperty("staging_table",conf.getStagingTable());
			if(conf.isStagingRequired())
				prop.setProperty("staging_required","true");
			else
				prop.setProperty("staging_required","false");
			if(conf.isUpdateDatabase())
				prop.setProperty("update_database_required","true");
			else
				prop.setProperty("update_database_required","false");
			
			prop.setProperty("fieldSeparator",conf.getFieldSeparator());
			prop.setProperty("lineSeparator",conf.getLineSeparator());
			
			prop.store(fout, null);
			
			logger.debug("Property set as: " + JOB_PROP_FILE);
			
		} catch (Exception e) {
			logger.error("Error Setting properties at JobProperties.storeProperties()");
			logger.error("", e);
			DirectoryHandler.cleanUpWorkspaceExport(conf);
			throw new Error(e);
			
		}
		finally{
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException e) {
					logger.error("Error Setting properties at JobProperties.storeProperties()");
					logger.error(e);
					/*StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
					for(StackTraceElement stackTrace: stackTraceElements){
					    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
					}*/
					DirectoryHandler.cleanUpWorkspaceExport(conf);
					throw new Error(e);
				}
			}
		}

		DirectoryHandler.sendFileToHDFS(conf, JOB_PROP_FILE);
		DirectoryHandler.givePermissionToHDFSFile(conf,JOB_PROP_FILE);
	}
	
	public String setSplitbyCloumn(DIConfig conf, TableProperties tabProp){
		String col = "";
		if(tabProp.getColCons().size() == 1){
			Set<String> cols= tabProp.getColCons().keySet();
			for(String s : cols){
				col = s;
			}
		}
		return col;
	}

	public boolean validateStagingTable(Connection con, DIConfig conf) throws Exception {
		String validTableQuery="";
		if(conf.isDBCatalog())
			validTableQuery = properties.getProperty(ORACLE_VALIDATE_TAB_CAT); 
		else
			validTableQuery = properties.getProperty(ORACLE_VALIDATE_TAB); 
		PreparedStatement statement = con.prepareStatement(validTableQuery);
				
		try {

			
			statement.setString(1, conf.getTableOwner().toUpperCase());
			statement.setString(2, conf.getStagingTable().toUpperCase());

			logger.debug("validTableQuery --"+validTableQuery);


			ResultSet rs = statement.executeQuery();
			rs.next();
			int rowCount = rs.getInt(1); 
			if (rowCount == 0) {
				logger.error("No Object found, Please check the Schema Name And Table Name");
				StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
				for(StackTraceElement stackTrace: stackTraceElements){
				    logger.debug(stackTrace.getClassName()+ "  "+ stackTrace.getMethodName()+" "+stackTrace.getLineNumber());
				}
				throw new Error();
			}

			logger.info("Table Exists");
			return true;
		} catch (SQLException e) {

			logger.error("SQL Exception at validateTable()");
			logger.error("", e);
			throw new Error(e);
		} finally {

			if (statement != null) {
				statement.close();
			}


		}
	}

 public String getAbsoluteName(String name){ 
	return name.split("/")[1];
	 
 }

}