package org.apache.hadoop.fs.http.client;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.*;
import java.io.IOException;

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

	// It would be nice if this package only had one JSON library, GSON or Jackson.
	// Unfortunately GSON it for deserializing into types, which measn you need to
	// tell it a lot about the structure you are expecting. This isn't useful for
	// a class like this where the response could contain lots of different JSON
	// structures. Jackson on the other hand deserialises into a generic tree of
	// JsonNodes and does a decent job of guessing primitive types. If we use GSON
	// this library has to recognise the structure and sort it out (nicer for the#
	// user), if we use Jackson the user using this library has to deal with it (faster
	// for us).
	//
	// Some work on providing proper classes has been done in ContentSummary and FileStatus
	// If in future we want it be nicer, we can go back to Gson. For now I'm doing
	// it the quick way.
	/*public static Map<String, Object> toMap(String json) {
		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, Object>>() {}.getType();
		return gson.fromJson(json, type);
	}*/

}
