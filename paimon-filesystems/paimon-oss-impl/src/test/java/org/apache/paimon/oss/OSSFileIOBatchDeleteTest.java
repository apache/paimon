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

package org.apache.paimon.oss;

import org.apache.paimon.fs.Path;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for OSS batch deletion. */
class OSSFileIOBatchDeleteTest {

    private static final Path FIRST = new Path("oss://bucket/table/file-0.parquet");

    @Test
    void testDeletesInProviderSizedBatches() throws Exception {
        OSSClient client = mock(OSSClient.class);
        TestOSSFileIO fileIO = new TestOSSFileIO(client);
        List<Path> files = files(1001, "bucket");
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            DeleteObjectsRequest request = invocation.getArgument(0);
                            return new DeleteObjectsResult(new ArrayList<>(request.getKeys()));
                        });

        assertThat(fileIO.deleteFilesInBatch(files)).isTrue();

        ArgumentCaptor<DeleteObjectsRequest> requests =
                ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(client, times(2)).deleteObjects(requests.capture());
        assertThat(requests.getAllValues().get(0).getKeys()).hasSize(1000);
        assertThat(requests.getAllValues().get(1).getKeys()).hasSize(1);
        assertThat(requests.getAllValues())
                .allSatisfy(
                        request -> {
                            assertThat(request.getBucketName()).isEqualTo("bucket");
                            assertThat(request.isQuiet()).isFalse();
                        });
        assertThat(fileIO.ossClientCalls).hasValue(1);
    }

    @Test
    void testValidatesWholeRequestBeforeAccessingStorage() {
        TestOSSFileIO fileIO = new TestOSSFileIO(mock(OSSClient.class));

        assertThatThrownBy(
                        () ->
                                fileIO.deleteFilesInBatch(
                                        Arrays.asList(
                                                FIRST,
                                                new Path(
                                                        "oss://other-bucket/table/file-1.parquet"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same OSS bucket");

        assertThat(fileIO.ossClientCalls).hasValue(0);
    }

    @Test
    void testValidatesEveryKeyBeforeAccessingStorage() {
        OSSClient client = mock(OSSClient.class);
        TestOSSFileIO fileIO = new TestOSSFileIO(client);
        List<Path> files = files(1000, "bucket");
        files.add(new Path("oss://bucket/" + String.join("", Collections.nCopies(1024, "a"))));

        assertThatThrownBy(() -> fileIO.deleteFilesInBatch(files))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(fileIO.ossClientCalls).hasValue(0);
        verify(client, never()).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testIncompleteResponseFails() throws Exception {
        OSSClient client = mock(OSSClient.class);
        TestOSSFileIO fileIO = new TestOSSFileIO(client);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(Collections.singletonList(key(FIRST))));

        assertThatThrownBy(
                        () ->
                                fileIO.deleteFilesInBatch(
                                        Arrays.asList(
                                                FIRST,
                                                new Path("oss://bucket/table/file-1.parquet"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("incomplete acknowledgement");
    }

    @Test
    void testWrongResponseKeysFail() throws Exception {
        OSSClient client = mock(OSSClient.class);
        TestOSSFileIO fileIO = new TestOSSFileIO(client);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(
                        new DeleteObjectsResult(
                                Arrays.asList(key(FIRST), "table/different.parquet")));

        assertThatThrownBy(
                        () ->
                                fileIO.deleteFilesInBatch(
                                        Arrays.asList(
                                                FIRST,
                                                new Path("oss://bucket/table/file-1.parquet"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid acknowledgement");
    }

    private static List<Path> files(int count, String bucket) {
        List<Path> files = new ArrayList<>(count);
        IntStream.range(0, count)
                .mapToObj(i -> new Path("oss://" + bucket + "/table/file-" + i + ".parquet"))
                .forEach(files::add);
        return files;
    }

    private static String key(Path path) {
        return path.toUri().getPath().substring(1);
    }

    private static class TestOSSFileIO extends OSSFileIO {

        private final OSSClient client;
        private final AtomicInteger ossClientCalls = new AtomicInteger();

        private TestOSSFileIO(OSSClient client) {
            this.client = client;
        }

        @Override
        OSSClient ossClient(Path path) {
            ossClientCalls.incrementAndGet();
            return client;
        }
    }
}
