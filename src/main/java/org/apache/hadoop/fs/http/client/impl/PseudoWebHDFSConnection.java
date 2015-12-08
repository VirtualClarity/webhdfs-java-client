package org.apache.hadoop.fs.http.client.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;

import org.apache.hadoop.fs.http.client.WebHDFSConnection;
import org.apache.hadoop.fs.http.client.WebHDFSConnectionFactory;
import org.apache.hadoop.fs.http.client.WebHDFSResponse;
import org.apache.hadoop.fs.http.client.util.Streams;
import org.apache.hadoop.fs.http.client.util.URLUtil;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
===== HTTP GET <br/>
<li>OPEN (see FileSystem.open)
<li>GETFILESTATUS (see FileSystem.getFileStatus)
<li>LISTSTATUS (see FileSystem.listStatus)
<li>GETCONTENTSUMMARY (see FileSystem.getContentSummary)
<li>GETFILECHECKSUM (see FileSystem.getFileChecksum)
<li>GETHOMEDIRECTORY (see FileSystem.getHomeDirectory)
<li>GETDELEGATIONTOKEN (see FileSystem.getDelegationToken)
<li>GETDELEGATIONTOKENS (see FileSystem.getDelegationTokens)
<br/>
===== HTTP PUT <br/>
<li>CREATE (see FileSystem.create)
<li>MKDIRS (see FileSystem.mkdirs)
<li>CREATESYMLINK (see FileContext.createSymlink)
<li>RENAME (see FileSystem.rename)
<li>SETREPLICATION (see FileSystem.setReplication)
<li>SETOWNER (see FileSystem.setOwner)
<li>SETPERMISSION (see FileSystem.setPermission)
<li>SETTIMES (see FileSystem.setTimes)
<li>RENEWDELEGATIONTOKEN (see FileSystem.renewDelegationToken)
<li>CANCELDELEGATIONTOKEN (see FileSystem.cancelDelegationToken)
<br/>
===== HTTP POST <br/>
APPEND (see FileSystem.append)
<br/>
===== HTTP DELETE <br/>
DELETE (see FileSystem.delete)

 */
class PseudoWebHDFSConnection implements WebHDFSConnection {

	protected static final Logger logger = LoggerFactory.getLogger(PseudoWebHDFSConnection.class);

	private String httpfsUrl = WebHDFSConnectionFactory.DEFAULT_URL;
	private String principal = WebHDFSConnectionFactory.DEFAULT_USERNAME;
	private String password = WebHDFSConnectionFactory.DEFAULT_PASSWORD;

	private Token token = new AuthenticatedURL.Token();
	private AuthenticatedURL authenticatedURL = new AuthenticatedURL(new PseudoAuthenticator2(principal));

	PseudoWebHDFSConnection(String httpfsUrl, String principal, String password) {
		this.httpfsUrl = httpfsUrl;
		this.principal = principal;
		this.password = password;
		this.authenticatedURL = new AuthenticatedURL(new PseudoAuthenticator2(principal));
	}

	public static synchronized Token generateToken(String srvUrl, String princ, String passwd) {
		AuthenticatedURL.Token newToken = new AuthenticatedURL.Token();
		Authenticator authenticator = new PseudoAuthenticator2(princ);
		
		try {
			String spec = MessageFormat.format("/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name={0}", princ);
			HttpURLConnection conn = new AuthenticatedURL(authenticator).openConnection(createQualifiedUrl(srvUrl, spec), newToken);

			conn.connect();
			conn.disconnect();
			
			logger.info("Successfully authenticated client.");
		}
		catch(Exception ex) {
			logger.error(ex.getMessage());
			logger.error("[" + princ + ":" + passwd + "]@" + srvUrl, ex);
			// WARN
			// throws IOException,
			// AuthenticationException, InterruptedException
		}

		return newToken;
	}

	public void ensureValidToken() {
		if (!token.isSet()) { // if token is null
			token = generateToken(httpfsUrl, principal, password);
		} else {
			long currentTime = System.currentTimeMillis();
			long tokenExpired = Long.parseLong(token.toString().split("&")[3].split("=")[1]);
			logger.debug("[currentTime vs. tokenExpired] " + currentTime + " " + tokenExpired);

			if (currentTime > tokenExpired) { // if the token is expired
				token = generateToken(httpfsUrl, principal, password);
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
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse getHomeDirectory() throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name={0}", this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		return execute(conn);
	}

	/**
	 * <b>OPEN</b>
	 * 
	 * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
	 * [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
	 *
	 * @param path The HDFS path to the file to be opened
	 * @param os An output stream object to write to
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse open(String path, OutputStream os) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=OPEN&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Content-Type", "application/octet-stream");
		
		try {
			conn.connect();
	
			Streams.copy(conn.getInputStream(), os);
			
			return result(conn, false);
		}
		finally {
			conn.disconnect();
		}
	}

	/**
	 * <b>GETCONTENTSUMMARY</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
	 *
	 * @param path The HDFS path to the object to get a summary for
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse getContentSummary(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=GETCONTENTSUMMARY&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("GET");
		return execute(conn);
	}

	/**
	 * <b>LISTSTATUS</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
	 *
	 * @param path The HDFS path to the directory to list the contents of
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse listStatus(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=LISTSTATUS&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("GET");
		return execute(conn);
	}



	/**
	 * <b>GETFILESTATUS</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
	 *
	 * @param path The HDFS path to the file to the list the status of
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse getFileStatus(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=GETFILESTATUS&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("GET");
		return execute(conn);
	}

	/**
	 * <b>GETFILECHECKSUM</b>
	 * 
	 * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
	 *
	 * @param path The HDFS path to the file to get a checksum for
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse getFileCheckSum(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=GETFILECHECKSUM&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("GET");
		return execute(conn);
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
	 * @param path The HDFS path at which the file should be created
	 * @param is The InputStream to read the data from
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse create(String path, InputStream is) throws IOException, AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=CREATE&user.name={1}", URLUtil.encodePath(path), this.principal);
		String redirectUrl = null;
		
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		conn.setInstanceFollowRedirects(false);
		conn.connect();

		resp = result(conn, true);

		if (conn.getResponseCode() == 307) {
			logger.info("Redirecting to => " + conn.getHeaderField("Location"));
			redirectUrl = conn.getHeaderField("Location");
		}

		conn.disconnect();

		if (redirectUrl != null) {
			conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
			conn.setRequestMethod("PUT");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setRequestProperty("Content-Type", "application/octet-stream");
			// conn.setRequestProperty("Transfer-Encoding", "chunked");
			final int _SIZE = is.available();
			conn.setRequestProperty("Content-Length", "" + _SIZE);
			conn.setFixedLengthStreamingMode(_SIZE);
			conn.connect();

			Streams.copy(is, conn.getOutputStream());

			resp = result(conn, false);
			conn.disconnect();
		}

		return resp;
	}

	/**
	 * <b>MKDIRS</b>
	 * 
	 * curl -i -X PUT
	 * "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
	 *
	 * @param path The path to the directory to make, including any missing parents
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse mkdirs(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=MKDIRS&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
	}

	/**
	 * <b>CREATESYMLINK</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=CREATESYMLINK
	 * &destination=<PATH>[&createParent=<true|false>]"
	 *
	 * @param srcPath  The HDFS path that the link will point to
	 * @param destPath The HDFS path that the link will be created at
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse createSymLink(String srcPath, String destPath) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=CREATESYMLINK&destination={1}&user.name={2}",
				URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
	}

	/**
	 * <b>RENAME</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=RENAME
	 * &destination=<PATH>[&createParent=<true|false>]"
	 *
	 * @param srcPath The HDFS path to the object to be renamed
	 * @param destPath The new HDFS path that the object should be renamed to
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse rename(String srcPath, String destPath) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=RENAME&destination={1}&user.name={2}",
				URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
	}

	/**
	 * <b>SETPERMISSION</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
	 * [&permission=<OCTAL>]"
	 *
	 * @param path The HDFS path to the object upon which permissions should be set
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse setPermission(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETPERMISSION&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
	}

	/**
	 * <b>SETOWNER</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
	 * [&owner=<USER>][&group=<GROUP>]"
	 *
	 * @param path The HDFS path to the object of which the owner should be set
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse setOwner(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETOWNER&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
	}

	/**
	 * <b>SETREPLICATION</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
	 * [&replication=<SHORT>]"
	 *
	 * TODO: Does this make any sense without a replication factor?
	 *
	 * @param path The HDFS path to the object for which replication should be set.
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse setReplication(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETREPLICATION&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
	}

	/**
	 * <b>SETTIMES</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
	 * [&modificationtime=<TIME>][&accesstime=<TIME>]"
	 *
	 * TODO: Does this make any sense without the times?
	 *
	 * @param path The HDFS path to the object for which to set the times
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse setTimes(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETTIMES&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("PUT");
		return execute(conn);
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
	 * @param path The HDFS path to the file which should be appended to
	 * @param is The InputStream to read data from
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse append(String path, InputStream is) throws IOException, AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=APPEND&user.name={1}", URLUtil.encodePath(path), this.principal);
		String redirectUrl = null;
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("POST");
		conn.setInstanceFollowRedirects(false);
		
		try {
			conn.connect();
			resp = result(conn, true);
	
			if (conn.getResponseCode() == 307) {
				logger.info("Redirecting to => " + conn.getHeaderField("Location"));
				redirectUrl = conn.getHeaderField("Location");
			}
		}
		finally {
			conn.disconnect();
		}

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
			
			Streams.copy(is, conn.getOutputStream());

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
	 * @param path The HDFS path to the object to be deleted
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse delete(String path) throws IOException, AuthenticationException {
		ensureValidToken();
		String spec = MessageFormat.format("/webhdfs/v1/{0}?op=DELETE&user.name={1}", URLUtil.encodePath(path), this.principal);
		HttpURLConnection conn = authenticatedURL.openConnection(createQualifiedUrl(spec), token);
		conn.setRequestMethod("DELETE");
		conn.setInstanceFollowRedirects(false);
		return execute(conn);
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

	// End Getter & Setter

	protected WebHDFSResponse execute(HttpURLConnection conn) throws IOException {
		try {
			conn.connect();
			return result(conn, true);
		}
		finally {
			conn.disconnect();
		}
	}
	
	private URL createQualifiedUrl(String spec) throws MalformedURLException {
		return createQualifiedUrl(httpfsUrl, spec);
	}
	
	private static URL createQualifiedUrl(String baseUrl, String spec) throws MalformedURLException {
		return new URL(new URL(baseUrl), spec);
	}
	
	/*
	 * Report the result in JSON way
	 * 
	 * @param conn
	 * @param input
	 * @return
	 * @throws IOException
	 */
	private static WebHDFSResponse result(HttpURLConnection conn, boolean input) throws IOException {
		String data = "";
		
		if (input) {
			try {
				data = Streams.toString(conn.getInputStream());
			}
			catch(IOException e) {
				data = Streams.toString(conn.getErrorStream());
			}
		}

		return new WebHDFSResponse(conn.getResponseCode(), conn.getResponseMessage(), conn.getContentType(), data.toString());
	}
}
