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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.jindodata.types.JindoHadoopFileStatus;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link JindoFileIO}. */
public class JindoFileIOTest {

    private static final String SHOW_DIR_TIMESTAMP = "fs.oss.show-dir-timestamp";

    @Test
    public void testDisableDirectoryTimestampByDefault() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(new Options()));

        assertThat(
                        fileIO.hadoopOptions(new Path("oss://bucket/table"), "meta")
                                .get(SHOW_DIR_TIMESTAMP))
                .isEqualTo("false");
    }

    @Test
    public void testKeepExplicitDirectoryTimestampSetting() {
        Options options = new Options();
        options.set(SHOW_DIR_TIMESTAMP, "true");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        assertThat(
                        fileIO.hadoopOptions(new Path("oss://bucket/table"), "meta")
                                .get(SHOW_DIR_TIMESTAMP))
                .isEqualTo("true");
    }

    @Test
    public void testCreateBlobClientUsesConfiguredSts() {
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss.example.com");
        options.set("fs.oss.region", "cn-hangzhou");
        options.set("fs.oss.accessKeyId", "access-key");
        options.set("fs.oss.accessKeySecret", "access-secret");
        options.set("fs.oss.securityToken", "security-token");

        OSSClient client = JindoBlobPresigner.createBlobClient(options);
        try {
            assertThat(client.getEndpoint()).isEqualTo(URI.create("https://oss.example.com"));
            assertThat(client.getObjectOperation().getRegion()).isEqualTo("cn-hangzhou");
            assertThat(client.getCredentialsProvider().getCredentials().getAccessKeyId())
                    .isEqualTo("access-key");
            assertThat(client.getCredentialsProvider().getCredentials().getSecretAccessKey())
                    .isEqualTo("access-secret");
            assertThat(client.getCredentialsProvider().getCredentials().getSecurityToken())
                    .isEqualTo("security-token");
            URL signedUrl =
                    client.generatePresignedUrl(
                            "bucket", "object", new Date(System.currentTimeMillis() + 60_000));
            assertThat(signedUrl.getProtocol()).isEqualTo("https");
            assertThat(signedUrl.getHost()).isEqualTo("bucket.oss.example.com");
            assertThat(signedUrl.getPath()).isEqualTo("/object");
            assertThat(signedUrl.getQuery())
                    .contains(
                            "OSSAccessKeyId=access-key",
                            "Signature=",
                            "security-token=security-token");
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
        metadata.setContentType("application/octet-stream");
        metadata.addUserMetadata(
                "paimon-blob-descriptor-sha256", sha256Hex(descriptor.serialize()));
        when(client.headObject(eq("bucket"), contains("_bloburl_"))).thenReturn(metadata);
        when(client.getEndpoint()).thenReturn(URI.create("https://oss.example.com"));
        when(client.getClientConfiguration()).thenReturn(new ClientConfiguration());
        when(client.generatePresignedUrl(eq("bucket"), anyString(), any(), eq(HttpMethod.GET)))
                .thenAnswer(
                        invocation ->
                                new URL(
                                        "https://bucket.oss.example.com/"
                                                + invocation.getArgument(1)));

        JindoFileIO fileIO = new JindoFileIO(new JindoBlobPresigner(client));
        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, Duration.ofHours(1)))
                .startsWith("https://bucket.oss.example.com/table/data/_bloburl_");

        fileIO.close();
        verify(client).shutdown();
    }

    @Test
    public void testFallbackToRenamingWhenMultipartUploadUnsupported() throws Exception {
        JindoHadoopSystem fs = mock(JindoHadoopSystem.class);
        org.apache.hadoop.fs.Path hadoopPath = mock(org.apache.hadoop.fs.Path.class);
        when(fs.exists(any())).thenReturn(false);
        when(fs.getMpuStore(any())).thenReturn(null);
        when(fs.create(any(), eq(false))).thenReturn(mock(FSDataOutputStream.class));

        JindoFileIO fileIO = new TestingJindoFileIO(fs, hadoopPath);
        TwoPhaseOutputStream stream =
                fileIO.newTwoPhaseOutputStream(
                        new Path("oss://bucket.cn-hangzhou.oss-dls.aliyuncs.com/table/file"),
                        false);

        assertThat(stream).isInstanceOf(RenamingTwoPhaseOutputStream.class);
        stream.close();
        verify(fs).getMpuStore(hadoopPath);
    }

    @Test
    public void testDirectoryListingsWithStorageClass() throws IOException {
        org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path("oss://bucket/dir");
        JindoHadoopSystem fs = mock(JindoHadoopSystem.class);
        when(fs.makeQualified(dir)).thenReturn(dir);
        @SuppressWarnings("unchecked")
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> page =
                mock(org.apache.hadoop.fs.RemoteIterator.class);
        when(fs.listStatusIterator(dir)).thenReturn(page);
        JindoHadoopFileStatus archive =
                listedStatus(new org.apache.hadoop.fs.Path(dir, "file"), false, "Archive");
        JindoHadoopFileStatus unknown =
                listedStatus(new org.apache.hadoop.fs.Path(dir, "unknown"), false, "Unknown");
        JindoHadoopFileStatus subdir =
                listedStatus(new org.apache.hadoop.fs.Path(dir, "subdir"), true, "Standard");
        when(page.next()).thenReturn(archive, unknown, subdir);
        when(fs.listStatus(dir)).thenReturn(new JindoHadoopFileStatus[] {archive, unknown, subdir});
        IOException failure = new IOException("Failed to list next page");
        when(page.hasNext()).thenThrow(failure);

        JindoFileIO fileIO = new TestingJindoFileIO(fs, dir);
        Path path = new Path(dir.toUri());
        assertThat(fileIO.listStatus(path))
                .extracting(FileStatus::getStorageClass)
                .containsExactly("Archive", null, null);
        RemoteIterator<FileStatus> entries = fileIO.listStatusIterative(path);
        verify(page, never()).next();
        assertThat(entries.next().getStorageClass()).isEqualTo("Archive");
        assertThat(entries.next().getStorageClass()).isNull();
        FileStatus directory = entries.next();
        assertThat(directory.isDir()).isTrue();
        assertThat(directory.getStorageClass()).isNull();
        assertThatThrownBy(entries::hasNext).isSameAs(failure);
        verify(fs, never()).getFileStatus(any());
    }

    @Test
    public void testListingFileDoesNotExposeDefaultStorageClass() throws IOException {
        org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path("oss://bucket/file");
        JindoHadoopSystem fs = mock(JindoHadoopSystem.class);
        when(fs.makeQualified(file)).thenReturn(file);
        @SuppressWarnings("unchecked")
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> entries =
                mock(org.apache.hadoop.fs.RemoteIterator.class);
        JindoHadoopFileStatus basic = listedStatus(file, false, "Standard");
        when(entries.next()).thenReturn(basic);
        when(fs.listStatusIterator(file)).thenReturn(entries);
        when(fs.getFileStatus(file)).thenReturn(basic);
        when(fs.listStatus(file)).thenReturn(new JindoHadoopFileStatus[] {basic});

        JindoFileIO fileIO = new TestingJindoFileIO(fs, file);
        Path path = new Path(file.toUri());
        FileStatus listed = fileIO.listStatusIterative(path).next();
        assertThat(listed.getPath()).isEqualTo(path);
        assertThat(listed.getStorageClass()).isNull();
        assertThat(fileIO.listStatus(path))
                .extracting(FileStatus::getStorageClass)
                .containsExactly((String) null);
        assertThat(fileIO.getFileStatus(path).getStorageClass()).isNull();
    }

    private static JindoHadoopFileStatus listedStatus(
            org.apache.hadoop.fs.Path path, boolean directory, String storageClass) {
        JindoHadoopFileStatus status = mock(JindoHadoopFileStatus.class);
        when(status.getPath()).thenReturn(path);
        when(status.isFile()).thenReturn(!directory);
        when(status.isDirectory()).thenReturn(directory);
        when(status.getStorageClassName()).thenReturn(storageClass);
        return status;
    }

    private static class TestingJindoFileIO extends JindoFileIO {

        private final JindoHadoopSystem fs;
        private final org.apache.hadoop.fs.Path hadoopPath;

        private TestingJindoFileIO(JindoHadoopSystem fs, org.apache.hadoop.fs.Path hadoopPath) {
            this.fs = fs;
            this.hadoopPath = hadoopPath;
        }

        @Override
        protected org.apache.hadoop.fs.Path path(Path path) {
            return hadoopPath;
        }

        @Override
        protected Pair<JindoHadoopSystem, String> getFileSystemPair(
                org.apache.hadoop.fs.Path path, boolean enableCache) {
            return Pair.of(fs, "dls");
        }
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        StringBuilder result = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(bytes)) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
