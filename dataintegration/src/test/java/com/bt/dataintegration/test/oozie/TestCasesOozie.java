package com.bt.dataintegration.test.oozie;



import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.database.DBConnectImpl;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.DIConfigService;
import com.bt.dataintegration.property.config.TableProperties;
import com.bt.dataintegration.utilities.DirectoryHandler;

public class TestCasesOozie{
	
	private static HashMap<String, String> prop = TestLogicOozie.validateJobPropertiesFile();
		
	@Test
	public void validateNameNode() {
		Assert.assertNotNull(prop.get("nameNode"));
	}
	
	@Test
	public void validateShellFileInit() {
		Assert.assertNotNull(prop.get("shell_file_init"));
	}
	
	@Test
	public void validateSplitByColumn() {
		Assert.assertNotNull(prop.get("splitByColumn"));
	}
	
	@Test
	public void validateFrequency() {
		Assert.assertNotNull(prop.get("frequency"));

	}
	
	@Test
	public void validateDirectMode() {
		Assert.assertNotNull(prop.get("directMode"));
	}
	
	@Test
	public void validateTargetDir() {
		Assert.assertNotNull(prop.get("targetDirectory"));
	}
	
	@Test
	public void validateNumOfMappers() {
		Assert.assertNotNull(prop.get("numOfMappers"));
	}
	
	@Test
	public void validateHive2JDBCUrl() {
		Assert.assertNotNull(prop.get("hive2_jdbc_url"));
	}
	
	@Test
	public void validateOozieCoordAppPath() {
		Assert.assertNotNull(prop.get("oozie.coord.application.path"));
	}
	
	@Test
	public void validateHiveCreateScript() {
		Assert.assertNotNull(prop.get("hiveCreateScript"));
	}
	
	@Test
	public void validateLastModifiedDateCol() {
		Assert.assertNotNull(prop.get("lastModifiedDateColumn"));
	} 
	
	@Test
	public void validateSuccessEmails() {
		Assert.assertNotNull(prop.get("success_emails"));
	}
	
	@Test
	public void validateDBConnectString() {
		Assert.assertNotNull(prop.get("DBConnectString"));
	}
	
	@Test
	public void validateSqoopWhereClause() {
		Assert.assertNotNull(prop.get("sqoopWhereClause"));
	}
	
	@Test
	public void validateTimezone() {
		Assert.assertNotNull(prop.get("timezone"));
	} 
	
	@Test
	public void validateImportExportFlag() {
		Assert.assertNotNull(prop.get("import_export_flag"));
	}
	
	@Test
	public void validateSqoopFileFormat() {
		Assert.assertNotNull(prop.get("sqoopFileFormat"));
	}
	
	@Test
	public void validateMapColJava() {
		Assert.assertNotNull(prop.get("mapColumnJava"));
	}
	
	@Test
	public void validateOozieLauncherMainClass() {
		Assert.assertNotNull(prop.get("oozie_launcher_action_main_class"));
	}
	
	@Test
	public void validateHourField() {
		Assert.assertNotNull(prop.get("targetDirHour"));
	}
	
	@Test
	public void validateCoordinatorEndTime() {
		Assert.assertNotNull(prop.get("end"));
	}
	
	@Test
	public void validateTableSize() {
		Assert.assertNotNull(prop.get("tableSize"));
	}
	
	@Test
	public void validateHive2ServerPrincipal() {
		Assert.assertNotNull(prop.get("hive2_server_principal"));
	}
	
	@Test
	public void validateQueueName() {
		Assert.assertNotNull(prop.get("queueName"));
	}
	
	@Test
	public void validateRetentionPeriodProcessedData() {
		Assert.assertNotNull(prop.get("retention_period_processed_data"));
	}
	
	@Test
	public void validateMinutesField() {
		Assert.assertNotNull(prop.get("targetDirMinute"));
	}
	
	@Test
	public void validateColDetails() {
		Assert.assertNotNull(prop.get("columnDetails"));
	}
	
	@Test
	public void validatePartDetails() {
		Assert.assertNotNull(prop.get("partDetails"));
	}
	
	@Test
	public void validateWhereClause() {
		Assert.assertNotNull(prop.get("whereClause"));
	}
	
	@Test
	public void validateOozieLibPath() {
		Assert.assertNotNull(prop.get("oozie.use.system.libpath"));
	}
	
	@Test
	public void validateStartField() {
		Assert.assertNotNull(prop.get("start"));
	}
	
	@Test
	public void validateQueryMilestone() {
		Assert.assertNotNull(prop.get("queryMilestone"));
	}
	
	@Test
	public void validateUsername() {
		Assert.assertNotNull(prop.get("userName"));
	}
	
	@Test
	public void validateShellFile() {
		Assert.assertNotNull(prop.get("shell_file"));
	}
	
	@Test
	public void validateTableName() {
		Assert.assertNotNull(prop.get("tableName"));
	}
	
	@Test
	public void validateQueryIncremental() {
		Assert.assertNotNull(prop.get("queryIncremental"));
	}
	
	@Test
	public void validateJobTracker() {
		Assert.assertNotNull(prop.get("jobTracker"));
	}
	
	@Test
	public void validateHiveAddPartScript() {
		Assert.assertNotNull(prop.get("hiveAddPartScript"));
	}
	
	@Test
	public void validateSqoopColumns() {
		Assert.assertNotNull(prop.get("sqoopColumns"));
	}
	
	@Test
	public void validateoozieHiveSharelib() {
		Assert.assertNotNull(prop.get("oozie_action_sharelib_for_hive"));
	}
	
	@Test
	public void validateMonthField() {
		Assert.assertNotNull(prop.get("targetDirMonth"));
	}
	
	@Test
	public void validateFailureEmails() {
		Assert.assertNotNull(prop.get("failure_emails"));
	}
	
	@Test
	public void validatePassword() {
		Assert.assertNotNull(prop.get("password"));
	}
	
	@Test
	public void validateRetentionPeriodRawData() {
		Assert.assertNotNull(prop.get("retention_period_raw_data"));
	}
	
	@Test
	public void validateThrottle() {
		Assert.assertNotNull(prop.get("throttle"));
	}
	
	@Test
	public void validateUpperBound() {
		Assert.assertNotNull(prop.get("lastModifiedDateValueUpperBound"));
	}
	
	@Test
	public void validateAuditLogPath() {
		Assert.assertNotNull(prop.get("audit_log_path"));
	}
	
	@Test
	public void validateSqoopTargetDir() {
		Assert.assertNotNull(prop.get("targetDirectorySqoop"));
	}
	
	@Test
	public void validateTargetDirdate() {
		Assert.assertNotNull(prop.get("targetDirDate"));
	}
	
	@Test
	public void validateWFAppPath() {
		Assert.assertNotNull(prop.get("wf_application_path"));
	}
	
	@Test
	public void validateKerberosFlag() {
		Assert.assertNotNull(prop.get("kerberos_flag"));
	}
	
	@Test
	public void validatePugScriptPath() {
		Assert.assertNotNull(prop.get("pigScript"));
	}
	
	@Test
	public void validateAppNameNode() {
		Assert.assertNotNull(prop.get("application_nameNode"));
	}
	
	@Test
	public void validateTimeout() {
		Assert.assertNotNull(prop.get("timeout"));
	}
	
	@Test
	public void validateOracleJarPath() {
		Assert.assertNotNull(prop.get("oraJarPath"));
	}
	
	@Test
	public void validateConstraintDetails() {
		Assert.assertNotNull(prop.get("consDetails"));
	}
	
	@Test
	public void validateShellFilePath() {
		Assert.assertNotNull(prop.get("shell_file_path"));
	}
	
	@Test
	public void validateUserSelectedCols() {
		Assert.assertNotNull(prop.get("userSelectedColumns"));
	}
	
	@Test
	public void validateConcurrency() {
		Assert.assertNotNull(prop.get("concurrency"));
	}
	
	@Test
	public void validateYearField() {
		Assert.assertNotNull(prop.get("targetDirYear"));
	}
	
	@Test
	public void validateHiveAddPart() {
		Assert.assertNotNull(prop.get("hiveAddPart"));
	}
	
	@Test
	public void validateLowerBound() {
		Assert.assertNotNull(prop.get("lastModifiedDateValueLowerBound"));
	}
}
 