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
package org.apache.hadoop.fs.http.client;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class ContentSummary {
	
	private int directoryCount;
	private int fileCount;
	private int length;
	private int quota;
	private int spaceConsumed;
	private int spaceQuota;
	
	public ContentSummary() {
	}

	public int getDirectoryCount() {
		return directoryCount;
	}

	public void setDirectoryCount(int directoryCount) {
		this.directoryCount = directoryCount;
	}

	public int getFileCount() {
		return fileCount;
	}

	public void setFileCount(int fileCount) {
		this.fileCount = fileCount;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getQuota() {
		return quota;
	}

	public void setQuota(int quota) {
		this.quota = quota;
	}

	public int getSpaceConsumed() {
		return spaceConsumed;
	}

	public void setSpaceConsumed(int spaceConsumed) {
		this.spaceConsumed = spaceConsumed;
	}

	public int getSpaceQuota() {
		return spaceQuota;
	}

	public void setSpaceQuota(int spaceQuota) {
		this.spaceQuota = spaceQuota;
	}
	
	public static TypeAdapter adapter() {
		return new TypeAdapter<ContentSummary>() {
			@Override
			public void write(JsonWriter out, ContentSummary value) throws IOException {
				// not implemented
			}

			@Override
			public ContentSummary read(JsonReader in) throws IOException {
				ContentSummary instance = null;
				in.setLenient(true);
				
				if(in.peek() == JsonToken.BEGIN_OBJECT) {
					in.beginObject();
					
					if(in.nextName().equalsIgnoreCase("ContentSummary")) {
						String name;
						in.beginObject();
						instance = new ContentSummary();

						while(in.hasNext()) {
							name = in.nextName();
							
							if(name.equalsIgnoreCase("directoryCount")) {
								instance.directoryCount = in.nextInt();
							}
							else if(name.equalsIgnoreCase("fileCount")) {
								instance.fileCount = in.nextInt();
							}
							else if(name.equalsIgnoreCase("length")) {
								instance.length = in.nextInt();
							}
							else if(name.equalsIgnoreCase("quota")) {
								instance.quota = in.nextInt();
							}
							else if(name.equalsIgnoreCase("spaceConsumed")) {
								instance.spaceConsumed = in.nextInt();
							}
							else if(name.equalsIgnoreCase("spaceQuota")) {
								instance.spaceQuota = in.nextInt();
							}
						}
						
						in.endObject();
					}
	
					in.endObject();
				}
				return instance;
			}
		};
	}
}
