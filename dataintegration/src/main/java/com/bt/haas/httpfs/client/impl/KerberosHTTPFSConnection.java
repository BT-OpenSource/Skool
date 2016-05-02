package com.bt.haas.httpfs.client.impl;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bt.haas.httpfs.utils.ConfigProperties;
import com.bt.haas.httpfs.utils.URLUtil;
import com.google.gson.Gson;

/**
 * 
 * @author 606723501
 *
 */
public class KerberosHTTPFSConnection implements IHTTPFSConnection {

	private static final Logger logger = LoggerFactory.getLogger(KerberosHTTPFSConnection.class);

	
	
	private String httpfsUrl = HTTPFSConnectionFactory.DEFAULT_PROTOCOL
			+ HTTPFSConnectionFactory.DEFAULT_HOST + ":"
			+ HTTPFSConnectionFactory.DEFAULT_PORT;
	private String principal = HTTPFSConnectionFactory.DEFAULT_USERNAME;
	private String password = HTTPFSConnectionFactory.DEFAULT_PASSWORD;
	private String servicePrincipal = null;

	private Token token = new AuthenticatedURL.Token();
	
	
	private String outputResponseCode = null;
	
	private AuthenticatedURL authenticatedURL =null;

	public KerberosHTTPFSConnection() {
		try {
			this.httpfsUrl = HTTPFSConnectionFactory.DEFAULT_PROTOCOL
					+ ConfigProperties.getProperty("haas.httpfs.host",
							HTTPFSConnectionFactory.DEFAULT_HOST)
					+ ":"
					+ ConfigProperties.getProperty("haas.httpfs.port", String
							.valueOf(HTTPFSConnectionFactory.DEFAULT_PORT));

			this.principal = ConfigProperties.getProperty("haas.username",
					HTTPFSConnectionFactory.DEFAULT_USERNAME);
			this.password = ConfigProperties.getPassword("haas.password");
			this.servicePrincipal = ConfigProperties.getProperty(
					"haas.servicePrincipal", "");
			System.setProperty("java.security.krb5.realm",
					ConfigProperties.getProperty("haas.realm", ""));
			System.setProperty("java.security.krb5.kdc",
					ConfigProperties.getProperty("haas.kdc", ""));
			this.authenticatedURL = new AuthenticatedURL(new KerberosAuthenticator2(this.principal, this.password,this.servicePrincipal));
		} catch (NullPointerException e) {
			logger.error(e.getMessage(), e);
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	
	public KerberosHTTPFSConnection(String httpfsUrl, String principal,
			String password, String servicePrincipal) {
		this.httpfsUrl = httpfsUrl;
		this.principal = principal;
		this.password = password;
		this.servicePrincipal = servicePrincipal;
		this.authenticatedURL = new AuthenticatedURL(
				new KerberosAuthenticator2(this.principal, this.password,
						this.servicePrincipal));
	}


	public static synchronized Token generateToken(String srvUrl, String princ,
			String passwd , String servicePrincpal) {
		
		
		
		AuthenticatedURL.Token newToken = new AuthenticatedURL.Token();
		try {

			HttpURLConnection conn = new AuthenticatedURL(new KerberosAuthenticator2(princ, passwd,servicePrincpal))
			.openConnection(new URL(new URL(srvUrl),"/webhdfs/v1/?op=homedir"), newToken);
			conn.connect();

			conn.disconnect();

		} catch (Exception ex) {
			logger.error(ex.getMessage());
			logger.error("[" + princ + ":  password ]@" + srvUrl, ex);
		}

		return newToken;

	}

	private long copy(InputStream input, OutputStream result)
			throws IOException {
		byte[] buffer = new byte[12288]; // 8K=8192 12K=12288 64K=
		long count = 0L;
		int n = 0;
		while (-1 != (n = input.read(buffer))) {
			result.write(buffer, 0, n);
			count += n;
			result.flush();
		}
		result.flush();
		return count;
	}

	

	

	public void ensureValidToken() {
		if (!token.isSet()) { // if token is null
			token = generateToken(httpfsUrl, principal, password,servicePrincipal);
			
		} else {
			long currentTime = new Date().getTime();
			long tokenExpired = Long.parseLong(token.toString().split("&")[3]
					.split("=")[1]);
			logger.info("[currentTime vs. tokenExpired] " + currentTime + " "
					+ tokenExpired);

			if (currentTime > tokenExpired) { // if the token is expired
				token = generateToken(httpfsUrl, principal, password,servicePrincipal);
			}
		}

	}

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
	@Override
	public String getHomeDirectory() throws MalformedURLException, IOException,
			AuthenticationException {
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(new URL(
				new URL(httpfsUrl), "/webhdfs/v1/?op=GETHOMEDIRECTORY"), token);
		conn.connect();

		String resp = result(conn, true);
		conn.disconnect();
		return resp;
	}

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
	@Override
	public String open(String path, OutputStream os)
			throws MalformedURLException, IOException, AuthenticationException {
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=OPEN", URLUtil.encodePath(path))),
				token);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Content-Type", "application/octet-stream");
		conn.connect();
		InputStream is = conn.getInputStream();
		copy(is, os);
		is.close();
		os.close();
		String resp = result(conn, false);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String getContentSummary(String path) throws MalformedURLException,
			IOException, AuthenticationException {
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=GETCONTENTSUMMARY",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("GET");
		// conn.setRequestProperty("Content-Type", "application/octet-stream");
		conn.connect();
		String resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String listStatus(String path) throws MalformedURLException,
			IOException, AuthenticationException {
		ensureValidToken();
		//System.out.println("Token = "+token.isSet());

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=LISTSTATUS",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("GET");
		conn.connect();
		String resp = result(conn,true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String getFileStatus(String path) throws MalformedURLException,
			IOException, AuthenticationException {
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=GETFILESTATUS",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("GET");
		conn.connect();
		String resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String getFileCheckSum(String path) throws MalformedURLException,
			IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=GETFILECHECKSUM",
						URLUtil.encodePath(path))), token);

		conn.setRequestMethod("GET");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String create(String path, InputStream is)
			throws MalformedURLException, IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		String redirectUrl = null;
		logger.info(MessageFormat.format("/webhdfs/v1/{0}?op=CREATE",URLUtil.encodePath(path)));
		HttpURLConnection conn = authenticatedURL
				.openConnection(
						new URL(new URL(httpfsUrl), MessageFormat.format(
								"/webhdfs/v1/{0}?op=CREATE",
								URLUtil.encodePath(path))), token);
		conn.setRequestMethod("PUT");
		conn.setInstanceFollowRedirects(false);
		conn.connect();
		logger.info("Location:" + conn.getHeaderField("Location"));
		//System.out.println("Location:" + conn.getHeaderField("Location"));
		resp = result(conn, true);
		if (conn.getResponseCode() == 307)
			redirectUrl = conn.getHeaderField("Location");
		conn.disconnect();

		if (redirectUrl != null) {
			logger.info("Redirected URL : "+redirectUrl);
			conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
			conn.setRequestMethod("PUT");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setRequestProperty("Content-Type", "application/octet-stream");
			// conn.setRequestProperty("Transfer-Encoding", "chunked");
			//final int _SIZE = is.available();
			//conn.setRequestProperty("Content-Length", "" + _SIZE);
			//conn.setFixedLengthStreamingMode(_SIZE);
			conn.setChunkedStreamingMode(1024);
			conn.connect();
			OutputStream os = conn.getOutputStream();
			copy(is, os);
			// Util.copyStream(is, os);
			is.close();
			os.close();
			resp = result(conn, false);
			conn.disconnect();
		}

		return resp;
	}

	

	/*
	 * ========================================================================
	 * PUT
	 * ========================================================================
	 */

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
	@Override
	public String mkdirs(String path) throws MalformedURLException,
			IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL
				.openConnection(
						new URL(new URL(httpfsUrl), MessageFormat.format(
								"/webhdfs/v1/{0}?op=MKDIRS",
								URLUtil.encodePath(path))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

	

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
	@Override
	public String rename(String srcPath, String destPath)
			throws MalformedURLException, IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=RENAME&destination={1}",
						URLUtil.encodePath(srcPath),
						URLUtil.encodePath(destPath))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String setPermission(String path, int permission) throws MalformedURLException,
			IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=SETPERMISSION&permission={1}",
						URLUtil.encodePath(path),
						URLUtil.encodePath(String.valueOf(permission)))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String setOwner(String path, String owner, String group) throws MalformedURLException,
			IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl),
						MessageFormat.format("/webhdfs/v1/{0}?op=SETOWNER&owner={1}&group={2}",
								URLUtil.encodePath(path),
								URLUtil.encodePath(owner),
								URLUtil.encodePath(group))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

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
	@Override
	public String append(String path, InputStream is)
			throws MalformedURLException, IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		String redirectUrl = null;
		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format(
						"/webhdfs/v1/{0}?op=APPEND", path)), token);
		conn.setRequestMethod("POST");
		conn.setInstanceFollowRedirects(false);
		conn.connect();
		logger.info("Location:" + conn.getHeaderField("Location"));
		resp = result(conn, true);
		if (conn.getResponseCode() == 307)
			redirectUrl = conn.getHeaderField("Location");
		conn.disconnect();

		if (redirectUrl != null) {
			conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setRequestProperty("Content-Type", "application/octet-stream");
			// conn.setRequestProperty("Transfer-Encoding", "chunked");
			final int _SIZE = is.available();
			conn.setRequestProperty("Content-Length", "" + _SIZE);
			conn.setFixedLengthStreamingMode(_SIZE);
			conn.connect();
			OutputStream os = conn.getOutputStream();
			copy(is, os);
			// Util.copyStream(is, os);
			is.close();
			os.close();
			resp = result(conn, true);
			conn.disconnect();
		}

		return resp;
	}

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
	@Override
	public String delete(String path) throws MalformedURLException,
			IOException, AuthenticationException {
		String resp = null;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL
				.openConnection(
						new URL(new URL(httpfsUrl), MessageFormat.format(
								"/webhdfs/v1/{0}?op=DELETE",
								URLUtil.encodePath(path))), token);
		conn.setRequestMethod("DELETE");
		conn.setInstanceFollowRedirects(false);
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

	// Begin Getter & Setter
	public String getHttpfsUrl() {
		return httpfsUrl;
	}

	public void setHttpfsUrl(String httpfsUrl) {
		this.httpfsUrl = httpfsUrl;
	}

	public String getPrincipal() {
		return principal;
	}

	public void setPrincipal(String principal) {
		this.principal = principal;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	
	
	
	/**
	 * Report the result in JSON way
	 * 
	 * @param conn
	 * @param input
	 * @return
	 * @throws IOException
	 */
	private String result(HttpURLConnection conn, boolean input)
			throws IOException {
		StringBuffer sb = new StringBuffer();
		if (input) {
			InputStream is = conn.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					is, "utf-8"));
			String line = null;

			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
			reader.close();
			is.close();
		}
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("code", conn.getResponseCode());
		result.put("mesg", conn.getResponseMessage());
		result.put("type", conn.getContentType());
		result.put("data", sb);
		
		String output = String.valueOf(conn.getResponseCode());
		setOutputResponseCode(output);
		Gson gson = new Gson();
		String json = gson.toJson(result);
		logger.info("json = " + json);

		return json;
	}

	/**
	 * @return the output
	 */
	public String getOutputResponseCode() {
		return outputResponseCode;
	}

	/**
	 * @param output the output to set
	 */
	public void setOutputResponseCode(String output) {
		this.outputResponseCode = output;
	}
	
	
	

	

	// End Getter & Setter
}