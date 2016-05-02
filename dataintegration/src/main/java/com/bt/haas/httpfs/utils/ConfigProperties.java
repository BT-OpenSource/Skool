
package com.bt.haas.httpfs.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bt.haas.httpfs.client.HaaSHTTPFSClient;

/**
 * 
 * Config Properties - This class for loading sms-config.properties file.
 * @author 606723501
 *
 */
public class ConfigProperties {
	
	static Logger log = Logger.getLogger(ConfigProperties.class);
	
	private static Properties props = null;
	
	static {
		loadPropertiesFile();
	}
	
	private static void loadPropertiesFile(){
		String filename = "haas-httpfs-config.properties";
		File jarPath=new File(HaaSHTTPFSClient.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String propertiesPath=jarPath.getParentFile().getAbsolutePath();
        System.out.println(" propertiesPath-"+propertiesPath);
        InputStream isr = null;
		try {
			isr = new FileInputStream(propertiesPath+File.separator+filename);
		} catch (FileNotFoundException e1) {
			log.error("Couldn't load 'haas-httpfs-config.properties' properties files , make sure it is in proper location");
		}
        //InputStream isr = ConfigProperties.class.getResourceAsStream(filename);
		if (isr != null){
		  try {
			props = new Properties();
			props.load(isr);
			isr.close();
		} catch (IOException e) {
			log.error("Couldn't load 'haas-httpfs-config.properties' properties files , make sure it is in proper location");
		} 
		}
	}
	
	public static final String getProperty(String key, String defaultValue) throws NullPointerException , FileNotFoundException {

		if (props == null) {
			loadPropertiesFile();
		}

		String value = props.getProperty(key);
		if (value == null)
			value = defaultValue.trim();

		return value.trim();

	} 
	
	public static final String getPassword(String key) throws NullPointerException , FileNotFoundException{

		if (props == null) {
			loadPropertiesFile();
		}

		String value = props.getProperty(key);
		if (value != null){
			try {
				value = CipherText.decrypt(value).trim();
			} catch (Exception e) {
				e.printStackTrace();
				value = null;
			}
		}

		return value.trim();

	}
	
	
	public static void unloadPropertiesFile(){
		if(props !=null){
			props.clear();
			props = null;
		}
	}
	

}
