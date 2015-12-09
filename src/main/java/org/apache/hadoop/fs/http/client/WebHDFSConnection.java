package org.apache.hadoop.fs.http.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

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
public interface WebHDFSConnection {
	
	
	
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
	 WebHDFSResponse getHomeDirectory() throws IOException, AuthenticationException ;
	
	/**
	 * <b>OPEN</b>
	 * 
	 * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
	                    [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
	 * @param path The HDFS path to the file to be opened
	 * @param os An output stream object to write to
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse open(String path, OutputStream os) throws IOException, AuthenticationException ;
	
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
	 WebHDFSResponse getContentSummary(String path) throws IOException, AuthenticationException ;
	
	/**
	 * <b>LISTSTATUS</b>
	 * 
	 * curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
	 *
	 * @param path The HDFS path to the directory to list the contents of
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	 WebHDFSResponse listStatus(String path) throws IOException, AuthenticationException;
	
	/**
	 * <b>GETFILESTATUS</b>
	 * 
	 * curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
	 *
	 * @param path The HDFS path to the file to the list the status of
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	 WebHDFSResponse getFileStatus(String path) throws IOException, AuthenticationException;
	
	/**
	 * <b>GETFILECHECKSUM</b>
	 * 
	 * curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
	 *
	 * @param path The HDFS path to the file to get a checksum for
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	 WebHDFSResponse getFileCheckSum(String path) throws IOException, AuthenticationException ;
	
/*
 * ========================================================================
 * PUT
 * ========================================================================	
 */
	/**
	 * <b>CREATE</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
                    [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
                    [&permission=<OCTAL>][&buffersize=<INT>]"
	 * @param path The HDFS path at which the file should be created
	 * @param is The InputStream to read the data from
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	 WebHDFSResponse create(String path, InputStream is) throws IOException, AuthenticationException;
	
	/**
	 * <b>MKDIRS</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
	 *
	 * @param path The path to the directory to make, including any missing parents
	 * @throws AuthenticationException
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse mkdirs(String path) throws IOException, AuthenticationException ;
	
	/**
	 * <b>CREATESYMLINK</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
                              &destination=<PATH>[&createParent=<true|false>]"
	 *
	 * @param srcPath  The HDFS path that the link will point to
	 * @param destPath The HDFS path that the link will be created at
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse createSymLink(String srcPath, String destPath) throws IOException, AuthenticationException ;
	
	/**
	 * <b>RENAME</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME
                              &destination=<PATH>[&createParent=<true|false>]"
	 *
	 * @param srcPath The HDFS path to the object to be renamed
	 * @param destPath The new HDFS path that the object should be renamed to
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse rename(String srcPath, String destPath) throws IOException, AuthenticationException ;
	
	/**
	 * <b>SETPERMISSION</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
                              [&permission=<OCTAL>]"
	 *
	 * @param path The HDFS path to the object upon which permissions should be set
	 * @throws AuthenticationException
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse setPermission(String path) throws IOException, AuthenticationException ;
	
	/**
	 * <b>SETOWNER</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
                              [&owner=<USER>][&group=<GROUP>]"
	 *
	 * @param path The HDFS path to the object of which the owner should be set
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse setOwner(String path) throws IOException, AuthenticationException;
	
	/**
	 * <b>SETREPLICATION</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
                              [&replication=<SHORT>]"
	 *
	 * @param path The HDFS path to the object for which replication should be set.
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse setReplication(String path) throws IOException, AuthenticationException ;
	
	/**
	 * <b>SETTIMES</b>
	 * 
	 * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
                              [&modificationtime=<TIME>][&accesstime=<TIME>]"
	 *
	 * @param path The HDFS path to the object for which to set the times
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse setTimes(String path) throws IOException, AuthenticationException;
	
/*
 * ========================================================================
 * POST	
 * ========================================================================
 */
	/**
	 * curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
	 * @param path The HDFS path to the file which should be appended to
	 * @param is The InputStream to read data from
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	 WebHDFSResponse append(String path, InputStream is) throws IOException, AuthenticationException ;
/*
 * ========================================================================
 * DELETE	
 * ========================================================================
 */
	/**
	 * <b>DELETE</b>
	 * 
	 * curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
                              [&recursive=<true|false>]"
	 *
	 * @param path The HDFS path to the object to be deleted
	 * @return The response from the endpoint, wrapped in an {@link WebHDFSResponse}
	 * @throws AuthenticationException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	 WebHDFSResponse delete(String path) throws IOException, AuthenticationException ;
	
	
	
	
}
