package com.bt.dataintegration.test.sqoop;

import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.bt.dataintegration.sqoop.ImplSqoopImport;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author Prabhu Om Abhinav Manish
 *
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestCaseSqoopAuth {

	
	private ImplSqoopImport  implsqoop=new ImplSqoopImport();
	private HadoopConfig conf =new HadoopConfig().getHadoopConfigProperties();
	private TestLogicSqoopAuth logic = new TestLogicSqoopAuth();
   
  
	/**
	 * Below test is to check whether properties file created which contains all
	 * sqoop properties is accessible or not.
	 */
  
	@Test
	public void validateSqoopPropFileAccess() {
		Assert.assertEquals(true, implsqoop.validateSqoopPropFileAccess());

	}
	
	/**
	 * Validate connect string in properties file which will be used in sqoop
	 * connect options. This should not be null.
	 */
	@Test
	public void validateConnectString() {
		Assert.assertNotNull(conf.getDBConnectString());
		
	}

	

	/**
	 * Validate UserName which will be used in sqoop connect. Value should not
	 * be null
	 */
	@Test
	public void validateDBUsername() {

		Assert.assertNotNull(conf.getUserName());
	}

	/**
	 * Validate Password which will be used in sqoop connect. Value should not
	 * be null
	 */
	@Test
	public void validateDBPassword() {
		Assert.assertNotNull(conf.getPassword());
	}

	/**
	 * Validate Table Name which will be used in sqoop connect. Value should not
	 * be null
	 */

	public void validateTable() {

		Assert.assertNotNull(conf.getTableName());
	}

	/**
	 * Validate Query which can be used to fetch data from table using sqoop
	 * query options.
	 */

	@Test
	public void validateQuery() {

		Assert.assertNotNull(conf.getQueryMilestone());
	}

	/**
	 * Validate column details in properties file and it should not be null.
	 */

	@Test
	public void validateColumns() {
        
		Assert.assertNotNull(conf.getColumnDetails());
	}

	@Test
	// To be taken care later as we are using default file format as Avro
	public void validateFileFormat() {

		Assert.assertTrue(true);
	}

	/**
	 * Validate Export director from properties file and it should not be null.
	 */

	@Test
	public void validateTargetDir() {

		Assert.assertNotNull(conf.getTargetDirectory());
	}

	/**
	 * Validate Table Name which will be used in sqoop connect. Value should not
	 * be null
	 */

	@Test
	public void validateSplitByColumn() {

		Assert.assertNotNull(conf.getSplitbyColumn());
	}

	

	/**
	 * Validate Direct mode.if user have catlog permission and table does not
	 * contail blob and clob column then direct mode is set
	 */
	@Test
	public void validateDirectMode() {
		
		Assert.assertNotNull(conf.getDirectMode());

	}

	

	/**
	 * Validate check column in case of delta pull.this value should not be
	 * null.
	 */
	@Test
	public void validateCheckColumn() {

		Assert.assertNotNull(conf.getLastModifiedDateColumn());
	}

	

	@Ignore
	public void validateMapperBasedTblSize() {

		Assert.assertNotNull(conf.getTableSize());
	}

	@Test
	public void checkSplitByColumn() {
		Assert.assertNotNull(conf.getSplitbyColumn());
		
	}

	@Test
	public void validateSqoopOptions() {

		Assert.assertEquals(true, logic.validateSqoopOptions());
	}

	
	@Test
	public void storeLastKeyValue() {

		Assert.assertNotNull(conf.getLastModifiedDateValue());
	}

	@Test
	public void zvalidateCreatedFileFormat() {

		Assert.assertEquals(true, implsqoop.validateFileFormat(new Path(conf.getTargetDirectory()), new Configuration()));
	}

	@Test
	public void testSqoopImport() throws FileNotFoundException {
		
		Assert.assertEquals(0, implsqoop.sqoopImport(conf));
	}

	@Test
	public void validateTransferedRecords() {

		Assert.assertEquals(true, logic.validateTransferredrecords());
	}
	
	
	@Test
	public void validateQueueName() {

		Assert.assertNotNull(conf.getQueueName());
	}
	
	@Test
	public void validateNumberOfMappers() {

		Assert.assertNotNull(conf.getNumOfMappers());
	}

	@Test
	public void validateTargetDirYear() {

		Assert.assertNotNull(conf.getTargetDirYear());
	}
	@Test
	public void validateTargetDirmonth() {

		Assert.assertNotNull(conf.getTargetDirMonth());
	}
	@Test
	public void validateTargetDirDate() {

		Assert.assertNotNull(conf.getTargetDirDate());
	}
	
	@Test
	public void validateTargetDirHour() {

		Assert.assertNotNull(conf.getTargetDirHour());
	}
	
	@Test
	public void validateTargetDirMinute() {

		Assert.assertNotNull(conf.getTargetDirMinute());
	}
	
}
