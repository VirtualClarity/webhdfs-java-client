package org.apache.hadoop.fs.http.client.impl;

import java.io.*;
import java.net.MalformedURLException;
import static org.junit.Assert.*;
import com.fasterxml.jackson.databind.*;
import org.apache.hadoop.fs.http.client.WebHDFSResponse;
import org.apache.hadoop.fs.http.client.impl.KerberosWebHDFSConnection;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.*;


public class KerberosWebHDFSConnectionTest {
	KerberosWebHDFSConnection conn = null;
	@Before
	public void setUp() throws Exception {
		String host = "0efb1d907e7f";
		String port = "50070";				// 14000 for HttpFS, 50070 for webhdfs. If using 50070 you will also need 50075
		String principal = "fUcacfba0f0c29445d8284096097f925e1";
		String password = "pea6e2d22-6e00-4101-b0d0-584765f3ed68";
		conn = new KerberosWebHDFSConnection("http://" + host + ":" + port, principal, password);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void getHomeDirectory() throws MalformedURLException, IOException, AuthenticationException {
		WebHDFSResponse response = conn.getHomeDirectory();
		assertEquals(200, response.getResponseCode());
		assertEquals("\"/user/dr.who\"", response.getJSONResponse().get("Path").toString());
	}

/*	@Test
	public void createListDelete() throws MalformedURLException, IOException, AuthenticationException {
		// Put some text in a file
		String in_text = "This is some\nmulti-line\ntext. And it has some funny characters. Olé\n";
		File temp_file = File.createTempFile("test", ".txt");
		PrintWriter out = new PrintWriter(temp_file);
		out.print(in_text);
		out.close();

		// Push that file up
		FileInputStream is = new FileInputStream(temp_file);
		String path = temp_file.getName();
		String expected_response = "{\"code\":201,\"data\":\"\",\"mesg\":\"Created\"}";
		String response = conn.create(path, is);
		assertEquals(expected_response, response);

		// List it to check it's there
//		response = "{"code":200,"data":"{\"FileStatus\":{\"accessTime\":1449584563902,\"blockSize\":134217728,\"childrenNum\":0,\"fileId\":16600,\"group\":\"supergroup\",\"length\":69,\"modificationTime\":1449584564026,\"owner\":\"dr.who\",\"pathSuffix\":\"\",\"permission\":\"755\",\"replication\":1,\"storagePolicy\":0,\"type\":\"FILE\"}}","type":"application/json","mesg":"OK"}";"
		response = conn.getFileStatus(path);
		JsonNode top_level = jsonStringToObject(response);
		assertEquals(200, top_level.get("code").asInt());

		System.out.println(deQuote(top_level.get("data").toString()));
		JsonNode data = jsonStringToObject(deQuote(top_level.get("data").toString()));
		System.out.println("Object: " + data.isObject());
		System.out.println("Container: " + data.isContainerNode());
		System.out.println("Missing: " + data.isMissingNode());
		System.out.println("POJO: " + data.isPojo());
		System.out.println("Value: " + data.isValueNode());
		System.out.println("Find: " + data.findValue("fileId"));
		
		
/*		for(String k : data.keySet())
		{
			System.out.println(k);
		}*/
//		System.out.println(data.get("FileStatus"));

		/*Map<String, String> top_level = jsonStringToMap(response);
		Map<String, String> data = jsonStringToMap(top_level.get("data"));
		System.out.println(data.get("FileStatus"));
		//assertEquals(new Double("200"), FileStatus.get("length"));
	}

	//@Test
	public void listStatus() throws MalformedURLException, IOException, AuthenticationException {
		String path= "user/fUcacfba0f0c29445d8284096097f925e1";
		String json = conn.listStatus(path);
		System.out.println(json);
	}
	
	//@Test
	public void open() throws MalformedURLException, IOException, AuthenticationException {
		String path="user/zen/在TMSBG南京軟件部總結的資料.7z.001";
		FileOutputStream os = new  FileOutputStream(new File("/tmp/downloadfromhdfs.file"));
		String json = conn.open(path, os);
		System.out.println(json);
	}
	
	

	//@Test
	public void delete() throws MalformedURLException, IOException, AuthenticationException {
		String path="user/zen/bigfile.tar.gz-new"; 
		String json = conn.delete(path);
		System.out.println(json);
	}*/


}
