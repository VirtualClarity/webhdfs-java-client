package org.apache.hadoop.fs.http.client.impl;

import java.io.*;
import java.util.Random;

import static org.junit.Assert.*;
import org.apache.hadoop.fs.http.client.WebHDFSResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.tree.ExpandVetoException;


public class KerberosWebHDFSConnectionTest
{
	protected static final Logger log = LoggerFactory.getLogger(KerberosWebHDFSConnectionTest.class);

	static KerberosWebHDFSConnection conn = null;
	static String host = "0efb1d907e7f";
	static String port = "50070";				// 14000 for HttpFS, 50070 for webhdfs. If using 50070 you will also need 50075
	static String principal = "fUcacfba0f0c29445d8284096097f925e1";
	static String password = "pea6e2d22-6e00-4101-b0d0-584765f3ed68";
	String user = "dr.who";
	static String in_text = "This is some\nmulti-line\ntext. And it has some funny characters. Olé\n";
	int in_text_bytes = 69;						// How many bytes the text above takes up
	static File temp_file;
	public final ExpectedException no_exception = ExpectedException.none();


	@Rule
	public final ExpectedException exception = no_exception;

	@Before
	public void setUp() throws Exception
	{
//		log.info("Creating temp file");
		conn = new KerberosWebHDFSConnection("http://" + host + ":" + port, principal, password);
		temp_file = File.createTempFile("test", ".txt");
		// Put some text in a file
		PrintWriter out = new PrintWriter(temp_file);
		out.print(in_text);
		out.close();
		// Put it in the cluster
		FileInputStream is = new FileInputStream(temp_file);
		WebHDFSResponse response = conn.create(temp_file.getName(), is, false);
		assertEquals(201, response.getResponseCode());
		assertEquals("Created", response.getResponseMessage());
	}

	@After
	public void tearDown() throws Exception {
//		log.info("Deleting temp file");
		WebHDFSResponse response = conn.delete(temp_file.getName());			// delete remote
		temp_file.delete();														// delete local
	}

	@Test
	public void getHomeDirectory() throws IOException, AuthenticationException
	{
		log.info("Starting test getHomeDirectory()");
		WebHDFSResponse response = conn.getHomeDirectory();
		assertEquals(200, response.getResponseCode());
		assertEquals("/user/" + user, deQuote(response.getJSONResponse().get("Path").toString()));
	}

	@Test
	public void createListOpen() throws  IOException, AuthenticationException
	{
		log.info("Starting test createListOpen()");
		// File was already created in test set up

		// List it to check it's there and has the correct size
		WebHDFSResponse response = conn.getFileStatus(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text_bytes, response.getJSONResponse().get("FileStatus") .get("length").asInt());

		// Bring it back and check it is the same as what we put up
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		response = conn.open(temp_file.getName(), os);
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text, os.toString());

	}

	@Test
	public void createWithOverwrite() throws  IOException, AuthenticationException
	{
		log.info("Starting test createListOpen()");
		// File was already created in test set up

		// Send it up again without an overwrite should give us an IOException
		FileInputStream is = new FileInputStream(temp_file);
		WebHDFSResponse response = conn.create(temp_file.getName(), is, false);
		assertEquals(403, response.getResponseCode());
		assertEquals("Forbidden", response.getResponseMessage());
		log.debug(response.getResponseCode() + " " + response.getResponseMessage());

		// Send it up again with an overwrite should be OK
		is = new FileInputStream(temp_file);
		response = conn.create(temp_file.getName(), is, true);
		assertEquals(201, response.getResponseCode());
		assertEquals("Created", response.getResponseMessage());
		is.close();
	}

	@Test
	public void getContentSummary() throws IOException, AuthenticationException
	{
		log.info("Starting test getContentSummary()");
		WebHDFSResponse response = conn.getContentSummary(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text_bytes, response.getJSONResponse().get("ContentSummary").get("length").asInt());
	}

	@Test
	public void getFileCheckSum() throws IOException, AuthenticationException
	{
		log.info("Starting test getFileCheckSum()");
		WebHDFSResponse response = conn. getFileCheckSum(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		// TODO In theory we should probably do the same checksum on the local file, but the checksum
		// TODO is a an MD5 of an MD5 of a CRC32 and I don't have time for that nonsense right now
	}

	@Test
	public void mkdirs() throws IOException, AuthenticationException
	{
		log.info("Starting test mkdirs()");
		log.info("Making directories");
		String dir1 = "test-" + (new Random()).nextInt(10000);
		String dir2 = "test-" + (new Random()).nextInt(10000);
		String dirs = dir1 + "/" + dir2;
		WebHDFSResponse response = conn.mkdirs(dirs);
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());

		log.info("Checking directories exist");
		response = conn.getFileStatus(dirs);
		assertEquals(200, response.getResponseCode());
		assertEquals("DIRECTORY", deQuote(response.getJSONResponse().get("FileStatus").get("type").toString()));

		//Tidy up
		log.info("Deleting directories");
		response = conn.delete(dirs);
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());
		response = conn.delete(dir1);
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());
	}

	// If this tests fails with java.io.IOException: Server returned HTTP response code: 400
	// then probably the error behind it is
	// {"RemoteException":{"exception":"UnsupportedOperationException","javaClassName":"java.lang.UnsupportedOperationException","message":"Symlinks not supported"}}
	// which means your version of Hadoop doesn't support symlinks. In this case you can comment
	// out this test by commenting out the next line. You can verify this like this:
	//
	// curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK&destination=<PATH>[&createParent=<true|false>]"
	//@Test
	public void createSymlink() throws IOException, AuthenticationException
	{
		log.info("Starting test createSymlink()");
		String link_name = "/link";
		WebHDFSResponse response = conn.createSymLink(temp_file.getName(), link_name);
		assertEquals(200, response.getResponseCode());

		response = conn.getFileStatus(link_name);
		assertEquals(200, response.getResponseCode());
		assertEquals("DIRECTORY", deQuote(response.getJSONResponse().get("FileStatus").get("type").toString()));
	}

	@Test
	public void rename() throws IOException, AuthenticationException
	{
		log.info("Starting test rename()");
		String new_name = "renamed";
		// Rename it
		WebHDFSResponse response = conn.rename(temp_file.getName(), "/" + new_name);
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());

		// Rename it back so it gets deleted in tearDown();
		response = conn.rename(new_name, "/" + temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());
	}

	@Test
	public void delete() throws IOException, AuthenticationException
	{
		log.info("Starting test delete()");
		// Delete it
		WebHDFSResponse response = conn.delete(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());

		// List it to make sure it has gone
		exception.expect(FileNotFoundException.class);
		conn.getFileStatus(temp_file.getName());
	}

	@Test
	public void append() throws IOException, AuthenticationException
	{
		log.info("Starting test append()");
		// The temp file has already been uploaded. If we upload the same content again, we should double its length
		FileInputStream is = new FileInputStream(temp_file);
		WebHDFSResponse response = conn.append(temp_file.getName(), is);
		is.close();
		assertEquals(200, response.getResponseCode());

		response = conn.getFileStatus(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text_bytes*2, response.getJSONResponse().get("FileStatus").get("length").asInt());
	}

	//@Test
	public void setPermission() throws IOException, AuthenticationException
	{
		log.info("Starting test setPermission()");
		// TODO - I believe the implementation currently lacks the ability to pass in a permission
		// This will need to be rectified before this test can be run
	}

	//@Test
	public void setOwner() throws IOException, AuthenticationException
	{
		log.info("Starting test setPermission()");
		// TODO - I believe the implementation currently lacks the ability to pass in an owner
		// This will need to be rectified before this test can be run
	}

	//@Test
	public void setReplication() throws IOException, AuthenticationException
	{
		log.info("Starting test setPermission()");
		// TODO - I believe the implementation currently lacks the ability to pass in a replication factor
		// This will need to be rectified before this test can be run
	}

	//@Test
	public void setTimes() throws IOException, AuthenticationException
	{
		log.info("Starting test setPermission()");
		// TODO - I believe the implementation currently lacks the ability to pass in a time
		// This will need to be rectified before this test can be run
	}

	private String deQuote(String s)
	{
		return s.substring(1, s.length() -1);
	}
}
