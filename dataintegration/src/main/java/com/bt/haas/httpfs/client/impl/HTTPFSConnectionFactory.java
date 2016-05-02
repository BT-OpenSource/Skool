package com.bt.haas.httpfs.client.impl;

import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bt.haas.httpfs.utils.ConfigProperties;

/**
 * 
 * @author 606723501
 * 
 */
public class HTTPFSConnectionFactory {

	private static final Logger logger = LoggerFactory
			.getLogger(HTTPFSConnectionFactory.class);

	/** The default host to connect to */
	public static final String DEFAULT_HOST = "localhost";

	/** The default port */
	public static final int DEFAULT_PORT = 14000;

	/** The default username */
	public static final String DEFAULT_USERNAME = System
			.getProperty("user.name");

	/** The default password */
	public static final String DEFAULT_PASSWORD = "";

	/** The default service Principal */
	public static final String DEFAULT_SERVICE_PRINCIPAL = "";

	public static final String DEFAULT_PROTOCOL = "http://";

	public static enum AuthenticationType {
		KERBEROS, PSEUDO
	}

	private static IHTTPFSConnection httpfsConnection;

	public static IHTTPFSConnection getConnection() {
		String authType = HTTPFSConnectionFactory.AuthenticationType.PSEUDO
				.name();
		try {
			authType = ConfigProperties.getProperty("haas.authType",
					HTTPFSConnectionFactory.AuthenticationType.PSEUDO.name());
		} catch (NullPointerException e1) {
			logger.error(e1.getMessage(), e1);
		} catch (FileNotFoundException e1) {
			logger.error(e1.getMessage(), e1);
		}

		if (httpfsConnection == null) {
			if (authType.equalsIgnoreCase(AuthenticationType.KERBEROS.name())) {
				httpfsConnection = new KerberosHTTPFSConnection();
			} else if (authType.equalsIgnoreCase(AuthenticationType.PSEUDO
					.name())) {
				httpfsConnection = new PseudoHTTPFSConnection();
			} else {
				logger.info("Defaulting to default connection Auth Type : Pseudo,  as proper Auth Type not mentioned. "
						+ "connection.AuthType should be Kerberos Or Pseudo in properties file");
				httpfsConnection = new PseudoHTTPFSConnection();
			}
		}
		return httpfsConnection;
	}

}
