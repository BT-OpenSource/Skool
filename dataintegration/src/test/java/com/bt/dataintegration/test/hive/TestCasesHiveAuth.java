/*
 * @Author: 609349708 (Abhinav Meghmala)
 */

package com.bt.dataintegration.test.hive;

import org.junit.Assert;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;

import com.bt.dataintegration.hive.HiveProcessImpl;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.sqoop.ImplSqoopImport;
import com.bt.dataintegration.utilities.Utility;

import static com.bt.dataintegration.constants.Constants.*;

/*
 * All the test cases to be validated post Sqoop Import
 */
public class TestCasesHiveAuth {
	
	HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties();
	HiveProcessImpl impl = new HiveProcessImpl();
	ProcessBuilder pb = null;
	Process p = null;
	String scriptCmds = "hadoop fs -ls /user/" + hconf.getQueueName() + "/" + hconf.getTableName() + "/workspace";
	
	static {
		HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties(); 
		Utility.executeSSH("hadoop fs -rm -R /user/" + hconf.getQueueName() + "/" + hconf.getTableName());		
		Utility.executeSSH("hadoop fs -mkdir -p /user/" + hconf.getQueueName() + "/" + hconf.getTableName() + "/workspace");
		new HiveProcessImpl().copyHiveScriptsToHDFS(hconf);
	}
	
	/*
	 * Below tests are needed for design of oozie workflow as well.
	 * job.properties file should not be empty or null with any values.
	 */
	
	//Cluster username should not be null in properties file
	@Test
	public void validateClusterUsername() {
		
		Assert.assertNotNull(hconf.getClusterUsername());
	}
	
	/*Cluster password is to be verified in VM environment
	 * For HaaS, this will be upgraded for kerberos authentication
	 */
	@Test
	public void validateClusterPassword() {
		
		Assert.assertNotNull(hconf.getClusterPassword());
	}
	
	//Table name should not be null in job.properties
	@Test
	public void validateTable() throws MetaException, UnknownDBException, TException {
		
		Assert.assertNotNull(hconf.getTableName());
	}
	
	@Test
	public void validateCreateTableQueryApp() {
		
		String actual = "create external table if not exists " + hconf.getQueueName() + "."
				+ hconf.getTableName()
				+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
				+ " ROW FORMAT SERDE " + "'" + SERDE_FORMAT + "'"
				+ " STORED AS INPUTFORMAT " + "'" + SERDE_IP_FORMAT + "'"
				+ " OUTPUTFORMAT " + "'" + SERDE_OP_FORMAT + "'" + " LOCATION '"
				+ /*hconf.getWorkflowNameNode() +*/ "/user/"
				+ hconf.getQueueName() + "/" + hconf.getTableName() + "/"
				+ "processed_data" + "'" + " TBLPROPERTIES " + "('"
				+ AVRO_SCHEMA_URL + "'='" /*+ hconf.getWorkflowNameNode()*/
				+ "/user/" + hconf.getQueueName() + "/"
				+ hconf.getTableName() + "/workspace/" + hconf.getTableName()
				+ ".avsc" + "')";
		String expected = impl.queryBuilder(hconf);
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void validateAddPartQueryApp() {
		
		String actual = "alter table " + hconf.getQueueName() + "." + hconf.getTableName()
				+ " add if not exists partition (part_year='"
				+ hconf.getTargetDirYear() + "',part_month='"
				+ hconf.getTargetDirMonth() + "',part_day='"
				+ hconf.getTargetDirDate() + "',part_hour='"
				+ hconf.getTargetDirHour() + "',part_minute='"
				+ hconf.getTargetDirMinute() + "')" + " location '"
				+ /*hconf.getWorkflowNameNode() +*/ "/user/"
				+ hconf.getQueueName() + "/" + hconf.getTableName()
				+ "/processed_data'";
		String expected = impl.partitionBuilder(hconf);
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void validateWFCreateQuery() {
	
		String actual = "create external table if not exists "
				+ "${tableName}"
				+ " COMMENT '" + " This table is created from source table " + hconf.getTableName() + " under schema " + hconf.getUserName()
				+ "'"
				+ " partitioned by (part_year String,part_month String,part_day String,part_hour String,part_minute String)"
				+ " ROW FORMAT SERDE " + "'" + SERDE_FORMAT + "'"
				+ " STORED AS INPUTFORMAT " + "'" + SERDE_IP_FORMAT + "'"
				+ " OUTPUTFORMAT " + "'" + SERDE_OP_FORMAT + "'" + " LOCATION "
				+ "'" + "${targetDirectory}" 
				+ "'" + " TBLPROPERTIES " + "('"
				+ AVRO_SCHEMA_URL + "'='" + "${nameNode}/user/${queueName}/${tableName}/workspace/${tableName}"
				+ ".avsc" + "')";
		String expected = impl.queryBuilderWorkflow(hconf);
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void validateWFPartitionQuery() {
		
		String actual = "alter table ${tableName} add if not exists partition (part_year='${targetDirYear}',"
				+ "part_month='${targetDirMonth}',part_day='${targetDirDate}',part_hour='${targetDirHour}',"
				+ "part_minute='${targetDirMinute}')" + " location '${hiveAddPart}'";
		String expected = impl.partitionBuilderWorkflow(hconf);
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void validateAuditTableQuery() {
		
		String actual ="create external table if not exists ${audit_table_name} (${hdi_tab_cols}) row format delimited fields terminated by ',' location '${location}'";
		String expected = impl.createAuditTableQueryBuilder();
		Assert.assertEquals(actual, expected);
	}
	
	@Test
	public void validateTableCreation() {
		
		String cmd = "create table " + hconf.getQueueName() + "." + "test_table(values String)";		
		try {
			pb = new ProcessBuilder("hive","-e",cmd);
			p = pb.start();
			int x = p.waitFor();
			Assert.assertEquals(0, x);
		} catch (Exception e) {}
	}
	
	@Test
	public void validateDropTable() {
		
		String cmd = "drop table " + hconf.getQueueName() + "." + "test_table";
		try {
			pb = new ProcessBuilder("hive","-e",cmd);
			p = pb.start();
			int x = p.waitFor();
			Assert.assertEquals(0, x);
		} catch (Exception e) {}
	}
	
	@Test
	public void validateHiveCreateScript() {
		
		int status = Utility.executeSSH(scriptCmds + "/" + hconf.getTableName() + "_CREATE_AVRO_TABLE.hql");
		Assert.assertEquals(0, status);
	}
	
	@Test
	public void validateAddPartScript() {
		
		int status = Utility.executeSSH(scriptCmds + "/" + hconf.getTableName() + "_ADD_PARTITION.hql");
		Assert.assertEquals(0, status);
	}
	
	@Test
	public void validateAudTabScript() {
		
		int status = Utility.executeSSH(scriptCmds + "/" + "HDI_CREATE_AUDIT_TABLE.hql");
		Assert.assertEquals(0, status);
	}
	
	/*
	 * Below methods validate the values from job.properties
	 * These values should not be null
	 */
	
	//Check if sqoop target directory exists.
	@Test
	public void validateTargetDirIfExists() {
		
		Assert.assertNotNull(hconf.getTargetDirectory());
	}
	
	//Check if year is not null
	
	@Test
	public void validateYear() {
		
		Assert.assertNotNull(hconf.getTargetDirYear());
	}
	
	//Check if month is not null
	
	@Test
	public void validateMonth() {
		
		Assert.assertNotNull(hconf.getTargetDirMonth());
	}
	
	//Check if date is not null
	
	@Test
	public void validateDate() {
		
		Assert.assertNotNull(hconf.getTargetDirDate());
	}
	
	//Check if Hour is not null
	
	@Test
	public void validateHour() {
		
		Assert.assertNotNull(hconf.getTargetDirMonth());
	}
	
	//Check if minute is not null
	
	@Test
	public void validateMinute() {
		
		Assert.assertNotNull(hconf.getTargetDirMinute());
	}
}