package org.apache.hadoop.fs.http.client.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;

import org.apache.hadoop.fs.http.client.WebHDFSConnection;
import org.apache.hadoop.fs.http.client.WebHDFSConnectionFactory;
import org.apache.hadoop.fs.http.client.WebHDFSResponse;
import org.apache.hadoop.fs.http.client.util.URLUtil;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
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
public class KerberosWebHDFSConnection implements WebHDFSConnection {

	protected static final Logger logger = LoggerFactory.getLogger(KerberosWebHDFSConnection.class);

	private String httpfsUrl = WebHDFSConnectionFactory.DEFAULT_PROTOCOL 
			+ WebHDFSConnectionFactory.DEFAULT_HOST + ":" + WebHDFSConnectionFactory.DEFAULT_PORT;
	private String principal = WebHDFSConnectionFactory.DEFAULT_USERNAME;
	private String password = WebHDFSConnectionFactory.DEFAULT_PASSWORD;

	private Token token = new AuthenticatedURL.Token();
	private AuthenticatedURL authenticatedURL = new AuthenticatedURL(new KerberosAuthenticator2(principal, password));

	public KerberosWebHDFSConnection(String httpfsUrl, String principal, String password) {
		this.httpfsUrl = httpfsUrl;
		this.principal = principal;
		this.password = password;
		this.authenticatedURL = new AuthenticatedURL(new KerberosAuthenticator2(principal, password));
	}

	public static synchronized Token generateToken(String srvUrl, String princ, String passwd) {
		AuthenticatedURL.Token newToken = new AuthenticatedURL.Token();
		try {

			HttpURLConnection conn = new AuthenticatedURL(new KerberosAuthenticator2(princ, passwd)).openConnection(
					new URL(new URL(srvUrl), "/webhdfs/v1/?op=GETHOMEDIRECTORY"), newToken);

			conn.connect();

			conn.disconnect();

		} catch (Exception ex) {
			logger.error(ex.getMessage());
			logger.error("[" + princ + ":" + passwd + "]@" + srvUrl, ex);
			// WARN
			// throws MalformedURLException, IOException,
			// AuthenticationException, InterruptedException
		}

		return newToken;

	}

	protected static long copy(InputStream input, OutputStream result) throws IOException {
		byte[] buffer = new byte[12288]; // 8K=8192 12K=12288 64K=
		long count = 0L;
		int n;
		while (-1 != (n = input.read(buffer))) {
			result.write(buffer, 0, n);
			count += n;
			result.flush();
		}
		result.flush();
		return count;
	}

	/**
	 * Report the result in JSON way
	 * 
	 * @param conn The HTTP connection object to use for the connection
	 * @param output Whether or not output is expected
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws IOException
	 */
	private static WebHDFSResponse result(HttpURLConnection conn, boolean output) throws IOException {
		StringBuilder sb = new StringBuilder();
		if (output) {
			InputStream is = conn.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
			String line;

			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
			reader.close();
			is.close();
		}
		
		return new WebHDFSResponse(conn.getResponseCode(), conn.getResponseMessage(), conn.getContentType(), sb.toString());		
	}

	public void ensureValidToken() {
		if (!token.isSet()) { // if token is null
			token = generateToken(httpfsUrl, principal, password);
		} else {
			long currentTime = new Date().getTime();
			long tokenExpired = Long.parseLong(token.toString().split("&")[3].split("=")[1]);
			logger.trace("[currentTime vs. tokenExpired] " + currentTime + " " + tokenExpired);

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

		HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl),
				"/webhdfs/v1/?op=GETHOMEDIRECTORY"), token);
		conn.connect();

		WebHDFSResponse resp = result(conn, true);
		conn.disconnect();
		return resp;
	}

	/**
	 * <b>OPEN</b>
	 * 
	 * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
	 * [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
	 * 
	 * @param path The HDFS path to the file to be opened 
	 * @param os An output stream object to write to 
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse open(String path, OutputStream os) throws IOException, AuthenticationException {
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=OPEN", URLUtil.encodePath(path))),
				token);
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Content-Type", "application/octet-stream");
		conn.connect();
		InputStream is = conn.getInputStream();
		copy(is, os);
		is.close();
		os.close();
		WebHDFSResponse resp = result(conn, false);
		conn.disconnect();

		return resp;
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

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=GETCONTENTSUMMARY",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("GET");
		// conn.setRequestProperty("Content-Type", "application/octet-stream");
		conn.connect();
		WebHDFSResponse resp = result(conn, true);
		conn.disconnect();

		return resp;
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

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=LISTSTATUS",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("GET");
		conn.connect();
		WebHDFSResponse resp = result(conn, true);
		conn.disconnect();

		return resp;
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

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=GETFILESTATUS",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("GET");
		conn.connect();
		WebHDFSResponse resp = result(conn, true);
		conn.disconnect();

		return resp;
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
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=GETFILECHECKSUM",
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
	 * @param path The HDFS path at which the file should be created
	 * @param is The InputStream to read the data from
	 * @param overwrite Whether or not to overwrite an existing file with the same name
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse create(String path, InputStream is, boolean overwrite) throws IOException,
			AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		String redirectUrl = null;
		String arguments = overwrite ? "&overwrite=true" : "&overwrite=false";
		URL end_url = new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=CREATE{1}",
				URLUtil.encodePath(path), arguments));
		logger.debug(end_url.toString());
		HttpURLConnection conn = authenticatedURL.openConnection(end_url, token);
		conn.setRequestMethod("PUT");
		conn.setInstanceFollowRedirects(false);
		conn.connect();
		logger.trace("Redirected to:" + conn.getHeaderField("Location"));
		resp = result(conn, true);
		if (conn.getResponseCode() == 307)
			redirectUrl = conn.getHeaderField("Location");
		conn.disconnect();
		if (redirectUrl != null)
		{
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
			OutputStream os = conn.getOutputStream();
			try
			{
				copy(is, os);
				// Util.copyStream(is, os);
				is.close();
				os.close();
				resp = result(conn, false);
			}
			catch (IOException e)
			{
				resp = new WebHDFSResponse(conn.getResponseCode(), conn.getResponseMessage(), null, null);
			}
			conn.disconnect();
		}

		return resp;
	}

	/**
	 * <b>MKDIRS</b>
	 * 
	 * curl -i -X PUT
	 * "http://<HOST>:<PORT>/webhdfs/v1/<PATH>/webhdfs/v1/?op=MKDIRS[&permission=<OCTAL>]"
	 * 
	 * @param path The path to the directory to make, including any missing parents
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse mkdirs(String path) throws IOException, AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		URL end_url = new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=MKDIRS",	URLUtil.encodePath(path)));
		HttpURLConnection conn = authenticatedURL.openConnection(end_url, token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

	/**
	 * <b>CREATESYMLINK</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
	 * &destination=<PATH>[&createParent=<true|false>]"
	 * 
	 * @param srcPath  The HDFS path that the link will point to
	 * @param destPath The HDFS path that the link will be created at
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse createSymLink(String srcPath, String destPath) throws IOException,
			AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=CREATESYMLINK&destination={1}",
						URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
	}

	/**
	 * <b>RENAME</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME
	 * &destination=<PATH>[&createParent=<true|false>]"
	 * 
	 * @param srcPath The HDFS path to the object to be renamed
	 * @param destPath The new HDFS path that the object should be renamed to
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse rename(String srcPath, String destPath) throws IOException,
			AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=RENAME&destination={1}",
						URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath))), token);
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
	 * TODO: Does this make any sense without new permissions?
	 *
	 * @param path The HDFS path to the object upon which permissions should be set
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse setPermission(String path) throws IOException, AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=SETPERMISSION",
						URLUtil.encodePath(path))), token);
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
	 * TODO: Does this make any sense without a new owner?
	 *
	 * @param path The HDFS path to the object of which the owner should be set
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse setOwner(String path) throws IOException, AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=SETOWNER",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
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
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=SETREPLICATION",
						URLUtil.encodePath(path))), token);
		conn.setRequestMethod("PUT");
		conn.connect();
		resp = result(conn, true);
		conn.disconnect();

		return resp;
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
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=SETTIMES",
						URLUtil.encodePath(path))), token);
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
	 * @param path The HDFS path to the file which should be appended to
	 * @param is The InputStream to read data from
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	public WebHDFSResponse append(String path, InputStream is) throws IOException,
			AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		String redirectUrl = null;
		HttpURLConnection conn = authenticatedURL.openConnection(
				new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=APPEND", path)), token);
		conn.setRequestMethod("POST");
		conn.setInstanceFollowRedirects(false);
		conn.connect();
		logger.trace("Redirected to:" + conn.getHeaderField("Location"));
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
	 * @param path The HDFS path to the object to be deleted
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	public WebHDFSResponse delete(String path) throws IOException, AuthenticationException {
		WebHDFSResponse resp;
		ensureValidToken();

		HttpURLConnection conn = authenticatedURL
				.openConnection(
						new URL(new URL(httpfsUrl), MessageFormat.format("/webhdfs/v1/{0}?op=DELETE",
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

	// End Getter & Setter
}
