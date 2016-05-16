package com.bt.dataintegration.shell;

import org.apache.log4j.Logger;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import static com.bt.dataintegration.constants.Constants.*;

public class ShellImpl implements IShell {
	final static Logger logger = Logger.getLogger(ShellImpl.class);

	public void shellToHDFS(HadoopConfig conf) {
		String sourceDir = "NA";
		String sourceDirError = "NA";

		DirectoryHandler.createNewFile(conf, REFRESH_LAST_COL_VALUE_SCRIPT,
				refreshLastColValueShell());
		DirectoryHandler.createNewFile(conf, UPDATE_LAST_COL_VALUE_SCRIPT,
				updateLastColValueShell(conf));
		DirectoryHandler.createNewFile(conf, AUDIT_LOG_SCRIPT,
				auditLogShell(sourceDir));
		DirectoryHandler.createNewFile(conf, HOUSE_KEEPING_SCRIPT,
				housekeepShell());
		DirectoryHandler.createNewFile(conf, ERROR_LOG_SCRIPT,
				errorShellFile(sourceDirError));

		DirectoryHandler.sendFileToHDFS(conf, REFRESH_LAST_COL_VALUE_SCRIPT);
		DirectoryHandler.sendFileToHDFS(conf, UPDATE_LAST_COL_VALUE_SCRIPT);
		DirectoryHandler.sendFileToHDFS(conf, AUDIT_LOG_SCRIPT);
		DirectoryHandler.sendFileToHDFS(conf, HOUSE_KEEPING_SCRIPT);
		DirectoryHandler.sendFileToHDFS(conf, UNIX_DATE_FILE);
		DirectoryHandler.sendFileToHDFS(conf, ERROR_LOG_SCRIPT);

		DirectoryHandler.givePermissionToHDFSFile(conf,
				REFRESH_LAST_COL_VALUE_SCRIPT);
		DirectoryHandler.givePermissionToHDFSFile(conf,
				UPDATE_LAST_COL_VALUE_SCRIPT);
		DirectoryHandler.givePermissionToHDFSFile(conf, AUDIT_LOG_SCRIPT);
		DirectoryHandler.givePermissionToHDFSFile(conf, HOUSE_KEEPING_SCRIPT);
		DirectoryHandler.givePermissionToHDFSFile(conf, UNIX_DATE_FILE);
		DirectoryHandler.givePermissionToHDFSFile(conf, ERROR_LOG_SCRIPT);
	}

	public void shellToHDFSFile(HadoopConfig conf) {
		String sourceDir = "${10}";
		String sourceDirError = "${12}";

		DirectoryHandler.createNewFile(conf, AUDIT_LOG_SCRIPT,
				auditLogShell(sourceDir));
		DirectoryHandler.createNewFile(conf, ERROR_LOG_SCRIPT,
				errorShellFile(sourceDirError));
		DirectoryHandler.createNewFile(conf, CAPTURE_CREATED_DATE_SCRIPT,
				captureDateShellFile());

		DirectoryHandler.sendFileToHDFS(conf, AUDIT_LOG_SCRIPT);
		DirectoryHandler.sendFileToHDFS(conf, ERROR_LOG_SCRIPT);
		DirectoryHandler.sendFileToHDFS(conf, CAPTURE_CREATED_DATE_SCRIPT);

		DirectoryHandler.givePermissionToHDFSFile(conf, AUDIT_LOG_SCRIPT);
		DirectoryHandler.givePermissionToHDFSFile(conf, ERROR_LOG_SCRIPT);
		DirectoryHandler.givePermissionToHDFSFile(conf,
				CAPTURE_CREATED_DATE_SCRIPT);
	}

	public String captureDateShellFile() {
		String cmd = "echo \"Capturing preliminary information...\"\n"
				+ "target_dir=$1\n" + "start_date=`date +%Y-%m-%d_%H-%M-%S`\n"
				+ "dir_year=`date +'%Y'`\n" + "dir_month=`date +'%m'`\n"
				+ "dir_day=`date +'%d'`\n" + "dir_hour=`date +'%H'`\n"
				+ "dir_minute=`date +'%M'`\n" + "raw_dir=${target_dir}/raw\n"
				+ "archive_dir=${target_dir}/archive\n"
				+ "temp_dir=${target_dir}/temp\n"
				+ "rejected_dir=${target_dir}/rejected_files/${start_date}\n"
				+ "hadoop fs -rm -r ${temp_dir}\n"
				+ "hadoop fs -mkdir -p ${temp_dir}\n"
				+ "hadoop fs -mkdir -p ${rejected_dir}\n"
				+ "echo start_date=${start_date}\n"
				+ "echo dir_year=${dir_year}\n"
				+ "echo dir_month=${dir_month}\n" + "echo dir_day=${dir_day}\n"
				+ "echo dir_hour=${dir_hour}\n"
				+ "echo dir_minute=${dir_minute}\n"
				+ "echo temp_dir=${temp_dir}\n"
				+ "echo rejected_dir=${rejected_dir}\n"
				+ "echo raw_dir=${raw_dir}\n"
				+ "echo archive_dir=${archive_dir}\n";

		return cmd;
	}

	public String refreshLastColValueShell() {
		String shellCmd = "lastVal=`hadoop fs -cat $1 | tail -1`\n"
				+ "a=0\n"
				+ "while [\"$lastVal\" !=\"\"  ]\n"
				+ "do\n"
				+ "a=`expr $a + 1`\n" + "lastVal=`hadoop fs -cat $1 | tail -$a | head -1`\n"
				+ "done\n"
				+ "IFS=\",\"; declare -a Array=($lastVal)\n"
				+ "lowerBound=${Array[5]}\n"
				+ "echo lowerBound=${lowerBound}\n"
				+ "start_date=`date +%Y-%m-%d:%H:%M:%S`\n"
				+ "d=`date +%d-%h-%Y`\n" + "h=`date +%H:%M:%S`\n"
				+ "dateNow=\"$d $h\"\n" + "targetDirYear=`date +'%Y'`\n"
				+ "targetDirMonth=`date +'%m'`\n"
				+ "targetDirDate=`date +'%d'`\n"
				+ "targetDirHour=`date +'%H'`\n"
				+ "targetDirMinute=`date +'%M'`\n"
				+ "echo upperBound=${dateNow}\n"
				+ "echo targetDirYear=${targetDirYear}\n"
				+ "echo targetDirMonth=${targetDirMonth}\n"
				+ "echo targetDirDate=${targetDirDate}\n"
				+ "echo targetDirHour=${targetDirHour}\n"
				+ "echo targetDirMinute=${targetDirMinute}\n"
				+ "echo dateNow=${dateNow}\n"
				+ "echo start_date=${start_date}";
		return shellCmd;
	}

	public String updateLastColValueShell(HadoopConfig conf) {
		String shellCmd = "";
		if (conf.getSqoopFileFormat().contains("parquet")) {
			if ("".equals(conf.getLastModifiedDateColumn())) {

			} else {
				shellCmd = "echo  $'\\n'$2,$3,$4,$5,$6,$7 | hadoop fs -appendToFile - $1";
			}
		} else {
			if ("".equals(conf.getLastModifiedDateColumn())) {
				shellCmd = "hadoop fs -rm -r $8/$2 \n"+
						"hadoop fs -mkdir -p $8/$2/$3/$4/$5/$6 \n"+
						"hadoop fs -mv  $8/temp/* $8/$2/$3/$4/$5/$6 \n"+
						"hadoop fs -rm -r $8/temp";
			} else {
				shellCmd = "echo  $'\\n'$2,$3,$4,$5,$6,$7 | hadoop fs -appendToFile - $1\n"
						+ "hadoop fs -rm -r $8/$2/$3/$4/$5/$6/* \n"
						+ "hadoop fs -mv  $8/temp/* $8/$2/$3/$4/$5/$6 \n"
						+ "hadoop fs -rm -r $8/temp";
			}
		}
		return shellCmd;
	}

	public String auditLogShell(String sourceDir) {

		String shellCmd = "echo \"Starting Audit Logs...\"\n" + "#runno=$1\n"
				+ "auditlog_path=$2\n" + "jobstart=$3\n"
				+ "import_export_flag=$4\n" + "oracle_table_name=$5\n"
				+ "hadoop_raw_dir_name=$6\n" + "#hadoop_final_dir_name=$7\n"
				+ "workflow_id=$7\n" + "workflow_name=$9\n"
				+ "recordcount=$8\n" + "sourceDirectory="
				+ sourceDir
				+ "\n"
				+ "milestone=NA\n"
				+ "runno=`hadoop fs -cat  ${auditlog_path} | grep ${workflow_name} | tail -1 | cut -d',' -f3`\n"
				+ "runno=$((runno+1))\n"
				+ "jobend=`date +%Y-%m-%d_%H-%M-%S`\n"
				+ "#lastVal=`hadoop fs -cat $1 | tail -1`\n"
				+ "#IFS=\",\"; declare -a Array=($lastVal)\n"
				+ "#runno=${Array[0]}\n"

				+ "if [ ${import_export_flag} -eq 1 ]\n"
				+ "then\n"
				+ "ieflag=\"SQOOP_IMPORT\"\n"
				+ "if [ ${10} == \"true\" ]\n"
				+ "then\n" 
				+ "milestone=MILESTONE\n"
				+ "else \n"
				+ "milestone=INCREMENTAL\n"
				+ "fi\n"
				+ "elif [ ${import_export_flag} -eq 2 ]\n"
				+ "then\n"
				+ "ieflag=\"SQOOP_EXPORT\"\n"
				+ "elif [ ${import_export_flag} -eq 3 ]\n"
				+ "then\n"
				+ "hadoop fs -mv ${11}/* ${12}\n"
				+ "hadoop fs -rm -r ${11}/*\n"
				+ "ieflag=\"FILE_IMPORT\"\n"
				+ "else\n"
				+ "echo \"Incorrect import_export_flag argument encountered.\n Please check job.properties file.\"\n"
				+ "fi\n"

				+ "echo \"NOR = ${recordcount}, WFN = ${workflow_name}\"\n"
				+ "echo \"Saving details to file :-\"\n"
				+ "echo ${workflow_id},${workflow_name},${runno},${jobstart},${jobend},${oracle_table_name},${ieflag},${hadoop_raw_dir_name},${recordcount},\"SUCCEEDED\",,,${sourceDirectory},${milestone} | hadoop fs -appendToFile - $2";
		return shellCmd;
	}

	public String housekeepShell() {
		String shellCmd = "RET_DAYS_RAW_DATA=$1\n"
				+ "PATH_RAW_DATA=$2\n"
				+ "#RET_DAYS_PROCESSED_DATA=$3\n"
				+ "#PATH_PROCESSED_DATA=$4\n"
				+ "if [ -z \"$RET_DAYS_RAW_DATA\"] && [\"$RET_DAYS_RAW_DATA\" == \"\"] && [ -z \"$PATH_RAW_DATA\"] && [\"$PATH_RAW_DATA\" == \"\"]; then\n"
				+ "echo \"empty\"\n"
				+ "else\n"
				+ "date_threshold=`date -d ' $RET_DAYS_RAW_DATA days' +'%Y/%m/%d'`\n"
				+ "for i in `hadoop fs -ls -R $PATH_RAW_DATA | awk '{print $1\"|\" $8}'`\n"
				+ "do \n"
				+ "DIRECTORY_NAME=${i:$((${#2}+11))}\n"
				+ "#echo $DIRECTORY_NAME\n"
				+ "if [[ ${i:0:1} == 'd' && ${#DIRECTORY_NAME} -eq 10 ]]; then \n"
				+ "#echo $DIRECTORY_NAME; \n"
				+ "if [[ $DIRECTORY_NAME < $date_threshold ]]; then \n"
				+ "hadoop fs -rm -r $PATH_RAW_DATA$DIRECTORY_NAME\n" + "fi\n"
				+ "fi\n" + "done\n" + "fi\n";

		/*
		 * +
		 * "if [ -z \"$RET_DAYS_PROCESSED_DATA\"] && [\"$RET_DAYS_PROCESSED_DATA\" == \"\"] && [ -z \"$PATH_PROCESSED_DATA\"] && [\"$PATH_PROCESSED_DATA\" == \"\"]; then\n"
		 * +"echo \"empty\"\n" +"else\n" +
		 * "date_threshold=`date -d ' $RET_DAYS_PROCESSED_DATA days' +'%Y/%m/%d'`\n"
		 * +
		 * "for i in `hadoop fs -ls -R $PATH_PROCESSED_DATA | awk '{print $1\"|\" $8}'`\n"
		 * +"do \n" +"DIRECTORY_NAME=${i:$((${#2}+11))}\n"
		 * +"#echo $DIRECTORY_NAME\n"
		 * +"if [[ ${i:0:1} == 'd' && ${#DIRECTORY_NAME} -eq 10 ]]; then \n"
		 * +"#echo $DIRECTORY_NAME; \n"
		 * +"if [[ $DIRECTORY_NAME < $date_threshold ]]; then \n"
		 * +"hadoop fs -rm -r $PATH_PROCESSED_DATA$DIRECTORY_NAME\n" +"fi\n"
		 * +"fi\n" +"done\n" +"fi\n";
		 */
		return shellCmd;
	}

	public String errorShellFile(String sourceDirError) {
		String shellCmd = "echo \"Starting Audit Logs...\"\n" + "#runno=$1\n"
				+ "auditlog_path=$2\n" + "jobstart=$3\n"
				+ "import_export_flag=$4\n" + "oracle_table_name=$5\n"
				+ "hadoop_raw_dir_name=$6\n" + "workflow_id=$7\n"
				+ "recordcount=$8\n" + "workflow_name=$9\n"
				+ "error_code=${10}\n" + "error_msg=${11}\n"
				+ "sourceDirectory="
				+ sourceDirError
				+ "\n"
				+ "milestone=NA\n"
				+ "runno=`hadoop fs -cat  ${auditlog_path} | grep ${workflow_name} | tail -1 | cut -d',' -f3`\n"
				+ "runno=$((runno+1))\n"
				+ "jobend=`date +%Y-%m-%d_%H-%M-%S`\n"
				+ "#lastVal=`hadoop fs -cat $1 | tail -1`\n"
				+ "#IFS=\",\"; declare -a Array=($lastVal)\n"
				+ "#runno=${Array[0]}\n"

				+ "if [ ${import_export_flag} -eq 1 ]\n"
				+ "then\n"
				+ "ieflag=\"SQOOP_IMPORT\"\n"
				+ "if [ ${12} == \"true\" ]\n"
				+ "then\n" 
				+ "milestone=MILESTONE\n"
				+ "else \n"
				+ "milestone=INCREMENTAL\n"
				+ "fi\n"
				+ "elif [ ${import_export_flag} -eq 2 ]\n"
				+ "then\n"
				+ "ieflag=\"SQOOP_EXPORT\"\n"
				+ "elif [ ${import_export_flag} -eq 3 ]\n"
				+ "then\n"
				+ "ieflag=\"FILE_IMPORT\"\n"
				+ "else\n"
				+ "echo \"Incorrect import_export_flag argument encountered.\n Please check job.properties file.\"\n"
				+ "fi\n"

				+ "echo \"NOR = ${recordcount}, WFN = ${workflow_name}\"\n"
				+ "echo \"Saving details to file :-\"\n"
				+ "echo ${workflow_id},${workflow_name},${runno},${jobstart},${jobend},${oracle_table_name},${ieflag},${hadoop_raw_dir_name},${recordcount},\"FAILED\",${error_code},${error_msg},${sourceDirectory},${milestone} | hadoop fs -appendToFile - $2";
		return shellCmd;
	}

}
