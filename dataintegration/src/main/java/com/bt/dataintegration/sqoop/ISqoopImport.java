package com.bt.dataintegration.sqoop;
import java.io.FileNotFoundException;

import com.bt.dataintegration.property.config.HadoopConfig;


public interface ISqoopImport {

	//Generate sqoop command based on the properties/config
	public String generateImportCommand(); 
	
	//Check for existence of auto-generated/user-defined hdfs import path
	public boolean validateTargetDirectory();	
	
	//run sqoop -import
	public int sqoopImport(HadoopConfig conf) throws FileNotFoundException ;
}