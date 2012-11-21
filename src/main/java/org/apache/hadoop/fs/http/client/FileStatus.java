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

public class FileStatus {

	private long accessTime;
	private int blockSize;
	private String group;
	private long length;
	private long modTime;
	private String owner;
	private String suffix;
	private String permission;
	private int replication;
	private FileType type;
	
	public FileStatus() {
	}

	public long getAccessTime() {
		return accessTime;
	}

	public void setAccessTime(long accessTime) {
		this.accessTime = accessTime;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public long getModificationTime() {
		return modTime;
	}

	public void setModificationTime(long modTime) {
		this.modTime = modTime;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	public String getPermission() {
		return permission;
	}

	public void setPermission(String permission) {
		this.permission = permission;
	}

	public int getReplication() {
		return replication;
	}

	public void setReplication(int replication) {
		this.replication = replication;
	}

	public FileType getType() {
		return type;
	}

	public void setType(FileType type) {
		this.type = type;
	}
	
	public static TypeAdapter adapter() {
		return new TypeAdapter<FileStatus>() {
			@Override
			public void write(JsonWriter out, FileStatus value) throws IOException {
				/* not implemented */
			}

			@Override
			public FileStatus read(JsonReader in) throws IOException {
				FileStatus instance = null;
				in.setLenient(true);
				
				if(in.peek() == JsonToken.BEGIN_OBJECT) {
					in.beginObject();
					
					if(in.nextName().equalsIgnoreCase("FileStatus")) {
						String name;
						in.beginObject();
						instance = new FileStatus();

						while(in.hasNext()) {
							name = in.nextName();
							
							if(name.equalsIgnoreCase("accessTime")) {
								instance.accessTime = in.nextLong();
							}
							else if(name.equalsIgnoreCase("blockSize")) {
								instance.blockSize = in.nextInt();
							}
							else if(name.equalsIgnoreCase("length")) {
								instance.length = in.nextLong();
							}
							else if(name.equalsIgnoreCase("modificationTime")) {
								instance.modTime = in.nextLong();
							}
							else if(name.equalsIgnoreCase("replication")) {
								instance.replication = in.nextInt();
							}
							else if(name.equalsIgnoreCase("group")) {
								instance.group = in.nextString();
							}
							else if(name.equalsIgnoreCase("owner")) {
								instance.owner = in.nextString();
							}
							else if(name.equalsIgnoreCase("pathSuffix")) {
								instance.suffix = in.nextString();
							}
							else if(name.equalsIgnoreCase("permission")) {
								instance.permission = in.nextString();
							}
							else if(name.equalsIgnoreCase("type")) {
								instance.type = FileType.valueOf(in.nextString());
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
