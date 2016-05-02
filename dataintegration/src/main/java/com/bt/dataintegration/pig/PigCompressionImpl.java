package com.bt.dataintegration.pig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.tool.DataFileGetSchemaTool;
import org.apache.log4j.Logger;

import static com.bt.dataintegration.constants.Constants.*;

import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class PigCompressionImpl implements IPigCompression{
	final static Logger logger = Logger.getLogger(PigCompressionImpl.class);
	public void compressData(HadoopConfig conf){
		int shellout = 1;
		String fileFormat= null;
		if(conf.getSqoopFileFormat().contains("avro")){
			fileFormat = "AvroStorage()";
		}
		else if(conf.getSqoopFileFormat().contains("text")){
			fileFormat = "PigStorage('\\u0001')";
		}
		String pigCmd = "set mapred.child.java.opts "+MAPRED_CHILD_JAVA_OPTS+"; \nset output.compression.enabled true; \nset output.compression.codec "+OUTPUT_COMPRESSION_CODEC+"; \n"
				+ "raw_equipment = load '" + conf.getTargetDirectory()+"' USING "+fileFormat+";\n"
				+"store raw_equipment into '" + conf.getAppNameNode()
				+ "/user/" + conf.getQueueName() + "/landing/staging/"
				+ conf.getSourceName()+"/"+ conf.getTableOwner()+"/HDI_"
				+ conf.getTableName()+"/temp' USING "+fileFormat+"; \n";
		
		logger.debug("pig command to compress data ---" +pigCmd);
		
  		DirectoryHandler.createNewFile(conf, "pigCompress.pig", pigCmd);
  		
  		pigCmd = "pig "+"-Dmapred.job.queue.name="+conf.getQueueName()+ " "+conf.getTableName()+"/pigCompress.pig";
  		shellout = Utility.executeSSH(pigCmd);
  		if(shellout !=0){
			throw new Error();
		}

  		pigCmd = "set mapred.child.java.opts "+MAPRED_CHILD_JAVA_OPTS+"; \nset output.compression.enabled true; \nset output.compression.codec "+OUTPUT_COMPRESSION_CODEC+"; \n"
				+ "raw_equipment = load '${targetDirectory}/${year}/${month}/${date}/${hour}/${minute}' USING "+fileFormat+";\n"
				+"store raw_equipment into '${targetDirectory}/temp' USING "+fileFormat+"; \n";
  		
  		DirectoryHandler.createNewFile(conf, "pigCompress.pig", pigCmd);
  		String cmd = "hadoop fs -put "+conf.getTableName()+"/pigCompress.pig "+conf.getWorkspacePath()+"/HDI_"+conf.getTableName()+"_COMPRESS_DATA.pig";
		logger.debug("command to store pigCompress.pig file -- " + cmd);
		shellout = Utility.executeSSH(cmd);
		if(shellout !=0){
			throw new Error();
		}
		
		if(conf.getSqoopFileFormat().contains("avro"))
			avscToHdfs(conf);
		//deleteUncompressedData(conf);
	}
	
	private void avscToHdfs(HadoopConfig conf){
		generateAvscFile(conf);
		int shellout = 1;
		logger.info("copying avro schema file to HDFS");
		String cmd = "hadoop fs -put "+conf.getTableName()+"/"+conf.getTableName()+".avsc "+conf.getWorkspacePath()+"/HDI_"+conf.getTableName()+".avsc";
		logger.debug("command to avro schema file to HDFS --" + cmd);
		shellout = Utility.executeSSH(cmd);
		if(shellout !=0){
			throw new Error();
		}
	}
	
	/*private void deleteUncompressedData(HadoopConfig conf){
		logger.info("deleteing uncompressed data");
		String cmd ="hadoop fs -rm -r "+conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/"+conf.getTableName()+"/landing/uncompressed_data";
		logger.debug("command to delete uncompressed data ---"+ cmd);
		Utility.executeSSH(cmd);
	}*/
	public void generateAvscFile(HadoopConfig conf){
		int shellout = 1;
		logger.info("generating avsc file");
		String hadoopGetAvroFile = "hadoop fs -get " + conf.getTargetDirectory()+"/part-m-00000.avro"/* + " /home/cloudera"*/;
		logger.info("Getting AVRO file...\n" + hadoopGetAvroFile);
		shellout = Utility.executeSSH(hadoopGetAvroFile);
		if(shellout !=0){
			throw new Error();
		}
		//String cmd ="avro-tools getschema "+conf.getAppNameNode()+"/user/"+conf.getQueueName()+"/"+conf.getTableName()+"/landing/uncompressed_data/part-m-00000.avro";
		//logger.debug("command to generate avsc schema ---"+ cmd);
		//String schema = Utility.executeSSH(cmd);
		PrintWriter out = null;
  		try {
  			/*out = new PrintWriter("QueryResult.avsc");
  			out.println(schema);*/
  			
  			File file = new File(conf.getTableName()+"/"+conf.getTableName()+".avsc");
  			// OutputStream ops = new FileOutputStream(file);
  			PrintStream ps = new PrintStream(file);
  			DataFileGetSchemaTool tool = new DataFileGetSchemaTool();
  			List<String> lst = new ArrayList<String>();
  			lst.add("part-m-00000.avro");
  			try {
				tool.run(System.in, ps, System.err, lst);
			} catch (Exception e) {
				logger.error("Error Running Avro Tool", e);
				DirectoryHandler.cleanUpWorkspace(conf);
				DirectoryHandler.cleanUpLanding(conf);
				throw new Error(e);
			}
  			
  		} catch (FileNotFoundException e) {
  			
  			logger.error("Error loading "+conf.getTableName()+".avsc");
  			logger.error("", e);
  			DirectoryHandler.cleanUpWorkspace(conf);
			DirectoryHandler.cleanUpLanding(conf);
			throw new Error(e);
  		}
  		finally{
  		//out.close();
  		}
	}
	
	public void generatePigFile(HadoopConfig conf){
		/*String pigCmd = "set mapred.child.java.opts -Xmx4096m;\n"+
						"set output.compression.enabled true;\n"+
						"set output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;\n"+
						"register ${jar_file};\n"+
						"raw = load '${temp_dir}' USING PigStorage(',');\n"+
						//"ranked_data = rank raw;\n"+
						//"noHeader_data = Filter ranked_data by ($0 >= ${lineNumberData});\n"+
						"final_data = FOREACH (GROUP raw ALL) {\n"+
							 //"noHeader_ordered = ORDER noHeader_data BY $0;\n"+
							   //"GENERATE FLATTEN(com.bt.dataintegration.fileValidations.Pig_Validate_UDF(raw,'${pig_cols}','${input_date_format}','\\\\|\\\\^','${fileTrailerKeyword}'));\n"+
							   "GENERATE FLATTEN(com.bt.dataintegration.fileValidations.Pig_Validate_UDF(raw,'${pig_cols}','${input_date_format}','\\\\\t','${fileTrailerKeyword}','${fileHeaderKeyword}'));\n"+
							"};\n"+
						"valid_data = filter final_data by $0 == '0';\n"+
						"val = foreach valid_data generate $1..;\n"+
						"invalid_data = filter final_data by $0 == '1';\n"+
						"inval = foreach invalid_data generate $1..;\n"+
						"Store val into '${valid_final_dir}' using PigStorage('\\u0001');\n"+
						"STORE inval INTO '${rejected_dir}' USING PigStorage('\\u0001');\n";*/
		String pigCmd = "set mapred.child.java.opts -Xmx4096m;\n"+
				"set output.compression.enabled true;\n"+
				"set output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;\n"+
				"register ${jar_file};\n"+
				"raw = load '${temp_dir}' USING com.bt.dataintegration.fileValidations.Pig_Load('${pig_cols}','${input_date_format}','"+Utility.delimiter+"','${fileTrailerKeyword}','${fileHeaderKeyword}');\n"+
				"valid_data = filter raw by $0 == '0';\n"+
				"val = foreach valid_data generate $1..;\n"+
				"invalid_data = filter raw by $0 == '1';\n"+
				"inval = foreach invalid_data generate $1..;\n"+
				"Store val into '${valid_final_dir}' using PigStorage('\\u0001');\n"+
				"STORE inval INTO '${rejected_dir}' USING PigStorage('\\u0001');\n";
		
		String pigFileName = "pig_record_validator.pig";
		DirectoryHandler.createNewFile(conf, pigFileName, pigCmd);
		DirectoryHandler.sendFileToHDFS(conf, pigFileName);
		DirectoryHandler.givePermissionToHDFSFile(conf, pigFileName);
		
	}
}
