package com.bt.haas.httpfs.client.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

/**
 * 
 * @author 606723501
 *
 */

public interface IHTTPFSConnection {
	
	public static final String PATH_SEPERATOR = "/";
	
	public static int ERROR_RESPONSE_CODE_BAD_REQUEST = 400 ; 
	public static int ERROR_RESPONSE_CODE_UNAUTHORISED = 401 ;
	public static int ERROR_RESPONSE_CODE_FORBIDDEN = 403 ;
	public static int ERROR_RESPONSE_CODE_FILE_NOT_FOUND = 404 ;
	public static int ERROR_RESPONSE_CODE_INTERNAL_SERVER_ERROR = 500 ;

	public static int OK_RESPONSE_CODE = 200 ;
	public static int CREATED_RESPONSE_CODE = 201 ;
	
	/*
	 * ========================================================================
	 * GET
	 * ========================================================================
	 */
	/**
	 * <b>GETHOMEDIRECTORY</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"
	 * 
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String getHomeDirectory() throws MalformedURLException, IOException,
			AuthenticationException;

	/**
	 * <b>OPEN</b>
	 * 
	 * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
	 * [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
	 * 
	 * @param path
	 * @param os
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public String open(String path, OutputStream os)
			throws MalformedURLException, IOException, AuthenticationException;

	/**
	 * <b>GETCONTENTSUMMARY</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
	 * 
	 * @param path
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String getContentSummary(String path) throws MalformedURLException,
			IOException, AuthenticationException;

	/**
	 * <b>LISTSTATUS</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
	 * 
	 * @param path
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String listStatus(String path) throws MalformedURLException,
			IOException, AuthenticationException;

	/**
	 * <b>GETFILESTATUS</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
	 * 
	 * @param path
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String getFileStatus(String path) throws MalformedURLException,
			IOException, AuthenticationException;

	/**
	 * <b>GETFILECHECKSUM</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
	 * 
	 * @param path
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String getFileCheckSum(String path) throws MalformedURLException,
			IOException, AuthenticationException;

	/*
	 * ========================================================================
	 * PUT
	 * ========================================================================
	 */
	/**
	 * <b>CREATE</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
	 * [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
	 * [&permission=<OCTAL>][&buffersize=<INT>]"
	 * 
	 * @param path
	 * @param is
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String create(String path, InputStream is)
			throws MalformedURLException, IOException, AuthenticationException;

	/**
	 * <b>MKDIRS</b>
	 * 
	 * curl -i -X PUT
	 * "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
	 * 
	 * @param path
	 * @return
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public String mkdirs(String path) throws MalformedURLException,
			IOException, AuthenticationException;

	/**
	 * <b>RENAME</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=RENAME
	 * &destination=<PATH>[&createParent=<true|false>]"
	 * 
	 * @param path
	 * @return
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public String rename(String srcPath, String destPath)
			throws MalformedURLException, IOException, AuthenticationException;

	/**
	 * <b>SETPERMISSION</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
	 * [&permission=<OCTAL>]"
	 * 
	 * @param path
	 * @return
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public String setPermission(String path, int permission) throws MalformedURLException,
			IOException, AuthenticationException;

	/**
	 * <b>SETOWNER</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
	 * [&owner=<USER>][&group=<GROUP>]"
	 * 
	 * @param path
	 * @return
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public String setOwner(String path, String owner, String group) throws MalformedURLException,
			IOException, AuthenticationException;

	
	
	/*
	 * ========================================================================
	 * POST
	 * ========================================================================
	 */
	/**
	 * curl -i -X POST
	 * "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
	 * 
	 * @param path
	 * @param is
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public String append(String path, InputStream is)
			throws MalformedURLException, IOException, AuthenticationException;

	/*
	 * ========================================================================
	 * DELETE
	 * ========================================================================
	 */
	/**
	 * <b>DELETE</b>
	 * 
	 * curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
	 * [&recursive=<true|false>]"
	 * 
	 * @param path
	 * @return
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public String delete(String path) throws MalformedURLException,
			IOException, AuthenticationException;
	
	public void setOutputResponseCode(String responseCode);
	
	public String getOutputResponseCode(); 

}
