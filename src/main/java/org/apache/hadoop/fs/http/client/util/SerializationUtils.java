/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 */
package org.apache.hadoop.fs.http.client.util;

import org.apache.hadoop.fs.http.client.ContentSummary;
import org.apache.hadoop.fs.http.client.FileStatus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SerializationUtils {
	
	private static final Gson gson = new GsonBuilder()
		.registerTypeAdapter(FileStatus.class, FileStatus.adapter())
		.registerTypeAdapter(ContentSummary.class, ContentSummary.adapter())
		.create();
	
	/**
	 * Convenience method to convert a JSON data String into a {@link ContentSummary}
	 * 
	 * @param data
	 * 			the JSON String
	 * @return a new {@link ContentSummary} instance
	 */
	public static ContentSummary getContentSummary(final String data) {
		return deserialize(data, ContentSummary.class);
	}

	/**
	 * Convenience method to convert a JSON data String into a {@link FileStatus}
	 * 
	 * @param data
	 * 			the JSON String
	 * @return a new {@link FileStatus} instance
	 */
	public static FileStatus getFileStatus(final String data) {
		return deserialize(data, FileStatus.class);
	}

	/**
	 * Common method to convert a JSON data String into a {@code T}
	 * 
	 * @param data
	 * 			the JSON String
	 * @param clazz
	 * 			the target {@link Class}
	 * @return a new instance of {@code clazz}
	 */
	public static <T> T deserialize(final String data, Class<T> clazz) {
		return gson.fromJson(data, clazz);
	}
}
