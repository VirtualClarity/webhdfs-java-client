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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.http.client.ContentSummary;
import org.apache.hadoop.fs.http.client.FileStatus;
import org.apache.hadoop.fs.http.client.FileType;
import org.junit.Test;

public class SerializationUtilsTest {

	@Test
	public void deserializeFileStatus() {
		FileStatus status = SerializationUtils.getFileStatus("{\"FileStatus\":{\"pathSuffix\":\"temporary\",\"type\":\"DIRECTORY\",\"length\":0,\"owner\":\"test\",\"group\":\"supergroup\",\"permission\":\"755\",\"accessTime\":0,\"modificationTime\":1352084683097,\"blockSize\":0,\"replication\":0}}");
	
		assertThat(status.getSuffix(), equalTo("temporary"));
		assertThat(status.getType(), equalTo(FileType.DIRECTORY));
		assertThat(status.getLength(), equalTo(0L));
		assertThat(status.getOwner(), equalTo("test"));
		assertThat(status.getPermission(), equalTo("755"));
		assertThat(status.getGroup(), equalTo("supergroup"));
		assertThat(status.getAccessTime(), equalTo(0L));
		assertThat(status.getModificationTime(), equalTo(1352084683097L));
		assertThat(status.getBlockSize(), equalTo(0));
		assertThat(status.getReplication(), equalTo(0));
	}
	
	@Test
	public void deserializeContentSummary() {
		ContentSummary summary = SerializationUtils.getContentSummary("{\"ContentSummary\":{\"directoryCount\":2,\"fileCount\":1,\"length\":139372,\"quota\":-1,\"spaceConsumed\":139372,\"spaceQuota\":-1}}");
		
		assertThat(summary.getDirectoryCount(), equalTo(2));
		assertThat(summary.getFileCount(), equalTo(1));
		assertThat(summary.getLength(), equalTo(139372));
		assertThat(summary.getQuota(), equalTo(-1));
		assertThat(summary.getSpaceQuota(), equalTo(-1));
		assertThat(summary.getSpaceConsumed(), equalTo(139372));
	}
}
