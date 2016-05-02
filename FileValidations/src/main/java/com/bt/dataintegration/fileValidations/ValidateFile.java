package com.bt.dataintegration.fileValidations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;

public class ValidateFile {
    static final Logger logger=Logger.getLogger(ValidateFile.class);
	
	/*public static LinkedList<String> readMaping(String mapingcols){
		LinkedList<String>mappingcolumn=new LinkedList<String>();
		String[] str=mapingcols.split(",");
		for(String splitcolumn:str){
			mappingcolumn.add(splitcolumn.split(" ")[0]);
			
		}
		
		return mappingcolumn;
		
	}*/
	
	public static void validateSimple(Path pt, String seperator, Path rejecteddir, String lineStartsFrom, String fileTrailerPresent) {
		
		logger.info("validating simple file.....");
		
		FileSystem fileSystem;
		long ipCount = Long.parseLong(lineStartsFrom);
		long expectedCount = 0;
		long recordCount = 0;
		String line = "";
		
		if("true".equalsIgnoreCase(fileTrailerPresent)) {
			expectedCount = ipCount + 1;
		} else if("false".equalsIgnoreCase(fileTrailerPresent)) {
			expectedCount = ipCount;
		}
		
		System.out.println(expectedCount + " : " + ipCount);
		
		try {
			Configuration conf = new Configuration();
			fileSystem = FileSystem.get(pt.toUri(), conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(pt)));
			
			while(((line = br.readLine()) != null) && (recordCount <= expectedCount)) {
				recordCount ++;
			}
			if(recordCount < expectedCount) {
				fileSystem.rename(pt, rejecteddir);
				return;
			}
		} catch (Exception e) {
			logger.error("Error: Unable to read file. Please check logs", e);
			return;
		}
		
		System.out.println(recordCount + " : " + expectedCount);
	}
	
	public static void readHeaderTrailerBoth(Path pt, String seperator,
			/*LinkedList<String> mapingcols,*/ Path rejecteddir) {
		System.out.println("inside readHeader");
		FileSystem fileSystem;
		boolean trailerFlag = false;
		String line = "";
		String[] str = null;
		long recordcount = 2;
		LinkedList<String> headercols = new LinkedList<String>();
		try {
			Configuration conf = new Configuration();
			fileSystem = FileSystem.get(pt.toUri(), conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fileSystem.open(pt)));
			line = br.readLine();
			if (line.split(seperator)[0].contains("FILEHEADER")) {
				line = br.readLine();
				if (line.startsWith("@")) {
					/*str = line.split(seperator);
					for (int i = 1; i < str.length; i++) {
						headercols.add(str[i]);
					}*/

					/*if (mapingcols.size() == headercols.size()) {
						for (int i = 0; i < mapingcols.size(); i++) {
							if (!mapingcols.get(i).trim()
									.equalsIgnoreCase(headercols.get(i))) {
								fileSystem.rename(pt, rejecteddir);
								return;
							}
						}
					}else {
						fileSystem.rename(pt, rejecteddir);
						return;
						}*/
					line = br.readLine();
					while (line != null) {
						if (line.contains("FILETRAILER")) {
							trailerFlag = true;
							recordcount=recordcount+1;
							if (!(recordcount == Long.parseLong(line
									.split(seperator)[1]))) {
								fileSystem.rename(pt, rejecteddir);
								return;
							}
						}
						recordcount = recordcount + 1;

						line = br.readLine();
					}
					if (!trailerFlag) {
						System.out.println("Trailer record is not there");
						fileSystem.rename(pt, rejecteddir);
						return;
					}
					if (recordcount == 3) {
						
						System.out.println("Trailer record is not there");
						fileSystem.rename(pt, rejecteddir);
						return;
					}
				} else{
					fileSystem.rename(pt, rejecteddir);
					return;
				}

			} else {
				
				fileSystem.rename(pt, rejecteddir);
				System.out.println(pt.getName()+" File rejected as Header is not matching with Mapping columns  Look into rejected file directory "+rejecteddir.toString());
                return;
			}
		} catch (Exception e) {
			//send file to rejected dir
		}

	}
	
	public static void rejectZeroSizeFile(String basePath, String pattern,Path rejecteddir) throws IllegalArgumentException, IOException{
		System.out.println(" Inside reject Method");
		Configuration conf=new Configuration();
		FileSystem fs = FileSystem.get(new Path(basePath).toUri(), conf);
		FileStatus[] listStatus = fs.globStatus(new Path(basePath + pattern));
		//Path tempDir = new Path(basePath + fileType+"dir");
		//Path rejecteddir=new Path(tempDir.toString()+"/rejected_files");
		//fs.mkdirs(tempDir);
		//fs.mkdirs(rejecteddir);
		for (FileStatus fstat : listStatus) {
			if(fstat.isFile() && fstat.getLen()==0){
			System.out.println("rejecting zero length files "+fstat.getPath().getName());	
			fs.rename(fstat.getPath(), rejecteddir);
			}
           
		}
		
	}

	public static List<Path> seprateFiles(String basePath, String pattern,Path tempdir)
			throws IOException {
		List<Path> inputhPaths = new ArrayList<Path>();
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(new Path(basePath).toUri(), conf);
		
		FileStatus[] listStatus = fs.globStatus(new Path(basePath + pattern));

		for (FileStatus fstat : listStatus) {
			fs.rename(fstat.getPath(), tempdir);
			

		}
		
		FileStatus[] listfile = fs.listStatus(tempdir);
		
		for (FileStatus listStatus2 : listfile) {
			if(listStatus2.isFile() && (listStatus2.getPath().getName().toString().endsWith(".gz")|| listStatus2.getPath().getName().toString().endsWith(".lzo")
					||listStatus2.getPath().getName().toString().endsWith(".bz2")||listStatus2.getPath().getName().toString().endsWith(".snappy"))){
				decompress(listStatus2.getPath().toString());
				fs.delete(listStatus2.getPath());
			}
				
		}
		FileStatus[] finalListofFile = fs.listStatus(tempdir);
		for(FileStatus listoffile:finalListofFile){
			if(listoffile.isFile()){
				inputhPaths.add(listoffile.getPath());
			}
		}
		System.out.println("List of files ****" + inputhPaths);
		return inputhPaths;

	}

	public static void decompress(String uri) throws IOException{
	
		   
		    Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(URI.create(uri), conf);
		    
		    Path inputPath = new Path(uri);
		    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		    CompressionCodec codec = factory.getCodec(inputPath);
		    if (codec == null) {
		      System.err.println("No codec found for " + uri);
		      System.exit(1);
		    }

		    String outputUri =
		      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
            
		    InputStream in = null;
		    OutputStream out = null;
		    try {
		      in = codec.createInputStream(fs.open(inputPath));
		      out = fs.create(new Path(outputUri));
		      IOUtils.copyBytes(in, out, conf);
		    } finally {
		      IOUtils.closeStream(in);
		      IOUtils.closeStream(out);
		    }
		  }
	

	
	
	public static void main(String[] args) throws Exception {
		//decompress("hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/BPP_RC_PUR-PUMAIN-RVCE_v6_DNH_1_1_20160314040229.dat.gz");
		/*String fileTypeMask = "*.csv";
		String separator = "[|^]+";
		String mapcolumn="InputBillingUsage.Msisdn int,InputBillingUsage.EventType varchar,InputBillingUsage.EventDirection varchar";
		String basePath="hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/";
		String tempdir="hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/EMP/TEMP_DIR";
		String rejectedDir="hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/EMP/REJECTED_FILES_DIR";
		String successfileName="cust_success.txt";*/
		
		String fileTypeMask = args[0];
		String separator = args[1];
		String basePath=args[2] + "/";
		String tempdir=args[3];
		String rejectedDir=args[4];
		String successfileName=args[5];
		String lineStartsFrom = args[6];
		String isTrailerPresent = args[7];
		
		boolean successFileFlag=false;
		Path basedirpath= new Path(basePath);
		Path tempdirpath=new Path(tempdir);
		Path rejectedDirPath=new Path(rejectedDir);
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(basedirpath.toUri(), conf);
		FileStatus[] listStatus = fs.listStatus(basedirpath);
		for(FileStatus list:listStatus){
			if(list.getPath().getName().equalsIgnoreCase(successfileName)){
			successFileFlag=true;
			fs.deleteOnExit(list.getPath());
			}
		}
		
		if(successFileFlag){
		rejectZeroSizeFile(basePath, fileTypeMask,rejectedDirPath);
		List<Path> filelist = seprateFiles(basePath,fileTypeMask,tempdirpath);
		//LinkedList<String> mapingcols=readMaping(mapcolumn);
		
		for(Path path:filelist){
			
			//readHeaderTrailerBoth(path, separator, /*mapingcols,*/ rejectedDirPath);
			/**
			 * Separator will not be used in this module.
			 * This is specified here for future validations if needed.
			 */
			validateSimple(path, separator, rejectedDirPath, lineStartsFrom, isTrailerPresent);
		}
		
		}else
		{
			throw new Error("Success file not found! Please check and take action accordingly ");
		}
	
		
	
		
	
		// seprateFiles("hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/","*.csv"));
		/*
		 * for(Path lif:filelist){ if(lif.getName().contains("maping")){
		 * typeOfFile="MapFile"; mapingDetails=readHeader(lif,
		 * Separator,typeOfFile); }
		 * 
		 * } for(Path lif:filelist){ if(!lif.getName().contains("maping")){
		 * typeOfFile=""; fileHeader=readHeader(lif, Separator,"");
		 * if(fileHeader.size()==mapingDetails.size()){ for(int i =0
		 * ;i<fileHeader.size();i++){
		 * if((fileHeader.get(i).trim().equalsIgnoreCase
		 * (mapingDetails.get(i).trim()))){
		 * System.out.println(" Matched !!!!!"+lif.getName());
		 * 
		 * 
		 * }else System.out.println(" Not Matched !!!!!"+lif.getName()); }
		 * //validateFiles(lif.getParent().toString(), fileType, processingDir,
		 * rejectedFilesDir); }else
		 * System.out.println("Not matched"+lif.getName());
		 * 
		 * } }
		 */
		// String
		// filePath="hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/header.csv";
		// String
		// mapFilePath="hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/maping.csv";

		//String baseDir = "hdfs://quickstart.cloudera:8020/user/cloudera/fs_data_dir";

		// validateFiles(baseDir, fileType);

		//System.out.println("start");

		// System.out.println(" fileheader  "+fileHeader);
		// System.out.println(" mapping  "+mapingDetails);

	}
}
