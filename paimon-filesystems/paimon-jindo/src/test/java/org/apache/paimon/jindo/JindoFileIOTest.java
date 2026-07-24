/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.jindo;

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectMetadata;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link JindoFileIO}. */
public class JindoFileIOTest {

    @Test
    public void testCreateBlobClientUsesConfiguredSts() {
        Options options = new Options();
        options.set("fs.oss.endpoint", "https://oss.example.com");
        options.set("fs.oss.accessKeyId", "access-key");
        options.set("fs.oss.accessKeySecret", "access-secret");
        options.set("fs.oss.securityToken", "security-token");

        OSSClient client = JindoFileIO.createBlobClient(options);
        try {
            assertThat(client.getEndpoint()).isEqualTo(URI.create("https://oss.example.com"));
            assertThat(client.getCredentialsProvider().getCredentials().getAccessKeyId())
                    .isEqualTo("access-key");
            assertThat(client.getCredentialsProvider().getCredentials().getSecretAccessKey())
                    .isEqualTo("access-secret");
            assertThat(client.getCredentialsProvider().getCredentials().getSecurityToken())
                    .isEqualTo("security-token");
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testCreateBlobPresignedUrlUsesOssClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 10, 20);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(20);
        metadata.setContentType("image/jpeg");
        metadata.addUserMetadata(
                "paimon-blob-descriptor-sha256", sha256Hex(descriptor.serialize()));
        when(client.headObject(eq("bucket"), contains("_bloburl_"))).thenReturn(metadata);
        when(client.getEndpoint()).thenReturn(URI.create("https://oss.example.com"));
        when(client.generatePresignedUrl(eq("bucket"), anyString(), any(), eq(HttpMethod.GET)))
                .thenAnswer(
                        invocation ->
                                new URL(
                                        "https://bucket.oss.example.com/"
                                                + invocation.getArgument(1)));

        JindoFileIO fileIO = new JindoFileIO(client);
        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "jpg", Duration.ofHours(1)))
                .startsWith("https://bucket.oss.example.com/table/data/_bloburl_");

        fileIO.close();
        verify(client).shutdown();
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        StringBuilder result = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(bytes)) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
