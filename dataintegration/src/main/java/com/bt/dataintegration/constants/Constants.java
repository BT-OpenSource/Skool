package com.bt.dataintegration.constants;

public interface Constants {

                public static final String ORACLE_DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";
                public static final String ORACLE_DRIVER_TYPE = "jdbc:oracle:thin";
                public static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
                public static final String HIVE2_CON_STRING_VM = "jdbc:hive2://quickstart.cloudera:10000/default";
                public static final String HIVE_METASTORE_URI = "hive.metastore.uris";
                public static final String CREDS_CREATE = "hadoop credential create";
                public static final String JCEKS = "jceks://hdfs/user";
                public static final String SQ_IMPORT_PARAM_CREDS = "-Dhadoop.security.credential.provider.path";
                public static final String SQ_PARAM_QUEUENAME = "-D mapreduce.job.queuename";
                public static final String PROVIDER = "-provider";
                // public static final String
                // HIVE_METASTORE_URI_VAL="thrift://quickstart.cloudera:9083";
                public static final String ORACLE_TABLE_OWNER = "oracle.table.owner";
                public static final String DATABASE_PROPERTIES = "database.properties";
                public static final String ORACLE_VALIDATE_TAB = "oracle.validate.tab";
                public static final String ORACLE_COL_DETAILS = "oracle.col.details";
                public static final String ORACLE_TAB_SIZE = "oracle.tab.size";
                public static final String ORACLE_TAB_PART_DETAILS = "oracle.tab.partition.details";
                public static final String ORACLE_TAB_CONST_DETAILS = "oracle.tab.const.details";
                public static final String ORACLE_VALIDATE_TAB_CAT = "oracle.validate.tab.cat";
                public static final String ORACLE_COL_DETAILS_CAT = "oracle.col.details.cat";
                public static final String ORACLE_TAB_SIZE_CAT = "oracle.tab.size.cat";
                // public static final String ORACLE_TAB_PART_DETAILS_CAT =
                // "oracle.tab.partition.details.cat";
                public static final String ORACLE_TAB_CONST_DETAILS_CAT = "oracle.tab.const.details.cat";
                public static final String ORACLE_TAB_METATDATA = "oracle.tab.metadata";
                public static final String ORACLE_HOST_CONNECTION = "oracle.host.connection";
                public static final String ORACLE_CHECK_CAT = "oracle.check.cat";
                // public static final String NAMENODE_VM =
                // "hdfs://quickstart.cloudera:8020";
                public static final String JOBTRACKER_VM = "quickstart.cloudera:8032";
                // public static final String NAMENODE = "hdfs://nameservice1";
                // public static final String NAMENODE =
                // "hdfs://tplhc01c001.iuser.iroot.adidom.com";
                public static final String JOBTRACKER = "yarnRM";
                // public static final String AVRO_SCHEMA_CMD = "avro-tools getschema";
                // public static final String AVRO_FILE = "part-m-00000.avro";
                // public static final String PUSH_AVSC_TO_HDFS = "hadoop fs -put";
                // public static final String HADOOP_FS = "hadoop fs -ls";
                public static final String SERDE_IP_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
                public static final String SERDE_OP_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
                public static final String AVRO_SCHEMA_URL = "avro.schema.url";
                public static final String SERDE_FORMAT = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
                public static final String MAPRED_CHILD_JAVA_OPTS = "-Xmx4096m";
                public static final String OUTPUT_COMPRESSION_CODEC = "org.apache.hadoop.io.compress.SnappyCodec";
                // Oozie workflow XML constants
                public static final String GCP_QUEUENAME = "mapred.job.queue.name";
                public static final String HS2_CP_URI = "hive2.jdbc.url";
                public static final String HS2_CP_PRINCIPAL = "hive2.server.principal";
                // public static final String HS2_CP_PRINCIPAL_VALUE =
                // "hive/_HOST@IUSER.IROOT.ADIDOM.COM";
                public static final String JAVA_CP_QUEUENAME = "mapred.queue.name";
                public static final String WORKFLOW_XMLNS = "uri:oozie:workflow:0.5";
                public static final String SQOOP_XMLNS = "uri:oozie:sqoop-action:0.2";
                public static final String HIVE_XMLNS = "uri:oozie:hive2-action:0.1";
                public static final String EMAIL_XMLNS = "uri:oozie:email-action:0.1";
                public static final String RETRY_MAX = "3";
                public static final String RETRY_INTERVAL = "1800";
                public static final String HS2_CP_URI_VALUE_VM = "jdbc:hive2://quickstart.cloudera:10000/default";
                public static final String HS2_SHARELIB = "oozie.action.sharelib.for.hive";
                public static final String HS2_MAIN_CLASS = "oozie.launcher.action.main.class";
                public static final String HS2_MAIN_CLASS_VALUE = "org.apache.oozie.action.hadoop.Hive2Main";
                public static final String HIVE_SITE_PATH = "/etc/hive/conf/hive-site.xml";
                public static final String SHELL_XMLNS = "uri:oozie:shell-action:0.1";
                public static final String HIVE_CREDENTIALS = "hive_credentials";
                public static final String HIVE_CREDENTIALS_TYPE = "hive2";
                public static final String HDI_AUDIT_COLS = "WORKFLOW_ID STRING,WORKFLOW_NAME STRING,RUN_NO STRING,JOB_START_TIME STRING,JOB_END_TIME STRING,ORACLE_TABLE_NAME STRING,SQOOP_IE_FLAG STRING,HADOOP_RAW_DATA_DIR STRING,RECORD_COUNT STRING,JOB_STATUS STRING,ERROR_CODE STRING,ERROR_MESSAGE STRING,FILE_SOURCE_DIRECTORY STRING,MILESTONE_INCREMENTAL STRING";
                public static final String HDI_FILE_VALIDATE_JAR = "file-validations-v1.jar";
                public static final String SQOOP_CREDS_PARAM = "hadoop.security.credential.provider.path";
                // public static final String JAVA_MAIN_CLASS =
                // "com.bt.dataintegration.fileValidations.ValidateFile";
                public static final String JAVA_MAIN_CLASS = "com.bt.dataintegration.fileValidations.ValidateFileSystem";
                public static final String PIG_CONF_PROP_NAME = "oozie.action.external.stats.write";
                public static final String PIG_CONF_PROP_VALUE = "true";
                public static final String MAPPING_SHEET_DELIMITER = "\\^\\^";
                public static final String DATE_FORMAT_YEAR = "yyyy";
                public static final String DATE_FORMAT_MONTH = "MM";
                public static final String DATE_FORMAT_MONTH_WORDS = "MMM";
                public static final String DATE_FORMAT_DAY = "dd";
                public static final String DATE_FORMAT_HOUR = "HH";
                public static final String DATE_FORMAT_MINUTE = "mm";
                public static final String SQOOP_IMPORT = "1";
                public static final String SQOOP_EXPORT = "2";
                public static final String FILE_IMPORT = "3";
                public static final String AUDIT_FILE = "audit_logs.txt";
                public static final String JOB_PROP_FILE = "job.properties";
                public static final String PASSWORD_FILE = "configuration/password.properties";
                public static final String LOG4J_PROPERTY_FILE = "configuration/log4j.properties";
                public static final String DATABASE_PROPERTIES_FILE = "configuration/database.properties";
                public static final String CONFIGURATION_PROPERTIES_FILE = "configuration/configuration.properties";
                public static final String UNIX_DATE_FILE = "configuration/unix_date.txt";
                public static final String REFRESH_LAST_COL_VALUE_SCRIPT = "refresh_last_col_value.sh";
                public static final String UPDATE_LAST_COL_VALUE_SCRIPT = "update_last_col_value.sh";
                public static final String AUDIT_LOG_SCRIPT = "audit_logs.sh";
                public static final String HOUSE_KEEPING_SCRIPT = "housekeep.sh";
                public static final String ERROR_LOG_SCRIPT = "error_logs.sh";
                public static final String CAPTURE_CREATED_DATE_SCRIPT = "capture_date_createdir.sh";
                public static final String OJDBC_JAR = "configuration/ojdbc6-11.2.0.3.jar";
                public static final String FILE_VALIDATIONS_JAR = "configuration/file-validations-v1.jar";
                //oozie action names
                public static final String ACTION_EMAIL_FAILURE = "EMAIL_FAILURE";
                public static final String ACTION_SQOOP_IMPORT = "SQOOP_IMPORT";
                public static final String ACTION_EMAIL_SUCCESS = "EMAIL_SUCCESS";
                public static final String ACTION_HIVE_CREATE_TABLE = "HIVE_CREATE_TABLE";
                public static final String ACTION_HIVE_CREATE_AUDIT_TABLE = "HIVE_CREATE_AUDIT_TABLE";
                public static final String ACTION_HIVE_ADD_PARTITION = "HIVE_ADD_PARTITION";
                public static final String ACTION_CAPTURE_ERROR_LOGS = "CAPTURE_ERROR_LOGS";
                public static final String ACTION_CAPTURE_AUDIT_LOGS = "CAPTURE_AUDIT_LOGS";
                public static final String ACTION_HOUSEKEEPING = "HOUSEKEEPING";
                public static final String ACTION_RECORD_VALIDATIONS = "RECORD_VALIDATIONS";
                public static final String ACTION_FILESYSTEM_VALIDATIONS = "FILESYSTEM_VALIDATIONS";
                public static final String ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE_FILE = "REFRESH_LAST_MODIFIED_DATE_VALUE_FILE";
                public static final String ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE = "REFRESH_LAST_MODIFIED_DATE_VALUE";
                public static final String ACTION_PIG_COMPRESS = "PIG_COMPRESS_DATA";
                public static final String ACTION_CAPTURE_DATE_AND_CREATEDIR = "CAPTURE_DATE_AND_CREATEDIR";
                
                public static final int SHELL_SUCCESS = 0;
                public static final int SHELL_FAILURE = 1;
				public static final String PIG_RECORD_VALIDATOR_SCRIPT="pig_record_validator.pig";
	            public static final String COORDINATOR_XML_FILE="coordinator.xml";

}
