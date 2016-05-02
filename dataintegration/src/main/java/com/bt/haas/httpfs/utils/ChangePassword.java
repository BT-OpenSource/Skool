package com.bt.haas.httpfs.utils;

import java.io.Console;
import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bt.haas.httpfs.client.HaaSHTTPFSClient;

public class ChangePassword {

	private static final Logger logger = LoggerFactory.getLogger(ChangePassword.class);
	
	public ChangePassword() {
		
	}

	/**
	 * Change the password in the property file
	 * @param password
	 * @throws ConfigurationException
	 */
	
	public void setPassword(String password) throws ConfigurationException {
		PropertiesConfiguration config = new PropertiesConfiguration();
		String filename = "haas-httpfs-config.properties";
		File jarPath=new File(HaaSHTTPFSClient.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String propertiesPath=jarPath.getParentFile().getAbsolutePath();
        File file = null;
		file = new File(propertiesPath+File.separator+filename);
        if (file.exists()){
		  config.load(file); 
		}
		try {
			config.setProperty("haas.password", CipherText.encrypt(password));
		} catch (Exception e) {
			logger.error(e.getMessage() , e);
		}
		config.save(file);
	}
	
	
	public void promptPassword() throws ConfigurationException {

		Console console = System.console();
	    if (console == null) {
	        System.out.println("Couldn't get Console instance");
	        System.exit(1);
	    }

	    char passwordArray[] = console.readPassword("Enter the password: ");
	    
	    String passwordStr = String.valueOf(passwordArray);
	    
	    while(passwordStr.isEmpty()){
	    	System.out.println("Password cannot be blank ");
	    	passwordArray = console.readPassword("Enter the password: ");
	    	passwordStr = String.valueOf(passwordArray);
	    }

		
		ChangePassword chgPassword = new ChangePassword();
		chgPassword.setPassword(passwordStr);

	}
	
	

}
