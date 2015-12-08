package org.apache.hadoop.fs.http.client;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/*
 * Copyright Virtual Clarity Limited 2015.
 * Public Domain
 */
public class WebHDFSResponse
{
	private int response_code;
	private String response_message;
	private String content_type;
	private String response;


	public WebHDFSResponse(int responseCode, String responseMessage, String contentType, String response)
	{
		this.response_code = responseCode;
		this.response_message = responseMessage;
		this.content_type = contentType;
		this.response = response;
	}

	public int getResponseCode()
	{
		return response_code;
	}

	public String getResponseMessage()
	{
		return response_message;
	}

	public String getContentType()
	{
		return content_type;
	}

	public String getRawResponse()
	{
		return response;
	}

	public JsonNode getJSONResponse() throws IOException
	{
		if(!content_type.equals("application/json"))
		{
			throw new JsonParseException("Content type not application/json", null);
		}
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(response);
	}

	/*public static Map<String, Object> toMap(String json) {
		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, Object>>() {}.getType();
		return gson.fromJson(json, type);
	}*/

}
