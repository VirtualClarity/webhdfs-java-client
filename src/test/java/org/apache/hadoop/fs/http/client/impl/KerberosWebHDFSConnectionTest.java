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


public class KerberosWebHDFSConnectionTest
{
	protected static final Logger log = LoggerFactory.getLogger(KerberosWebHDFSConnectionTest.class);

	static KerberosWebHDFSConnection conn = null;
	static String host = "0efb1d907e7f";
	static String port = "50070";				// 14000 for HttpFS, 50070 for webhdfs. If using 50070 you will also need 50075
	static String principal = "fUcacfba0f0c29445d8284096097f925e1";
	static String password = "pea6e2d22-6e00-4101-b0d0-584765f3ed68";
	String user = "dr.who";
	String in_text = "This is some\nmulti-line\ntext. And it has some funny characters. Olé\n";
	int in_text_bytes = 69;						// How many bytes the text above takes up
	static File temp_file;

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@BeforeClass
	public static void setUp() throws Exception {
		conn = new KerberosWebHDFSConnection("http://" + host + ":" + port, principal, password);
		temp_file= File.createTempFile("test", ".txt");
	}

	@AfterClass
	public static void tearDown() throws Exception {
		temp_file.delete();
	}

	@Test
	public void getHomeDirectory() throws IOException, AuthenticationException
	{
		WebHDFSResponse response = conn.getHomeDirectory();
		assertEquals(200, response.getResponseCode());
		assertEquals("/user/" + user, deQuote(response.getJSONResponse().get("Path").toString()));
	}

	@Test
	public void createListOpen() throws  IOException, AuthenticationException
	{
		// Put some text in a file
		PrintWriter out = new PrintWriter(temp_file);
		out.print(in_text);
		out.close();

		// Push that file up
		FileInputStream is = new FileInputStream(temp_file);
		String path = temp_file.getName();
		WebHDFSResponse response = conn.create(path, is);
		assertEquals(201, response.getResponseCode());
		assertEquals("Created", response.getResponseMessage());

		// List it to check it's there and has the correct size
		response = conn.getFileStatus(path);
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text_bytes, response.getJSONResponse().get("FileStatus").get("length").asInt());

		// Bring it back and check it is the same as what we put up
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		response = conn.open(path, os);
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text, os.toString());

	}

	@Test
	public void getContentSummary() throws IOException, AuthenticationException
	{
		WebHDFSResponse response = conn.getContentSummary(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals(in_text_bytes, response.getJSONResponse().get("ContentSummary").get("length").asInt());
	}

	@Test
	public void getFileCheckSum() throws IOException, AuthenticationException
	{
		WebHDFSResponse response = conn. getFileCheckSum(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		// TODO In theory we should probably do the same checksum on the local file, but the checksum
		// TODO is a an MD5 of an MD5 of a CRC32 and I don't have time for that nonsense right now
	}

	@Test
	public void mkdirs() throws IOException, AuthenticationException
	{
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

	@Test
	public void createSymlink() throws IOException, AuthenticationException
	{

	}

	// Keep this one last
	@Test
	public void delete() throws IOException, AuthenticationException
	{
		// Delete it
		WebHDFSResponse response = conn.delete(temp_file.getName());
		assertEquals(200, response.getResponseCode());
		assertEquals("true", response.getJSONResponse().get("boolean").toString());

		// List it to make sure it has gone
		exception.expect(FileNotFoundException.class);
		conn.getFileStatus(temp_file.getName());
	}

	private String deQuote(String s)
	{
		return s.substring(1, s.length() -1);
	}

}
