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

import org.apache.paimon.fs.BatchDeleteResult;
import org.apache.paimon.fs.BatchFileDeleter;
import org.apache.paimon.fs.Path;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.OSSUtils;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.GenericRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Strict batch-delete contract tests for {@link OSSFileIO}. */
class OSSFileIOBatchDeleteTest {

    private static final Path FIRST = new Path("oss://bucket/table/file-0.parquet");

    @Test
    void testDeletesOneObjectWithVerboseResponseValidation() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(Collections.singletonList(key(FIRST))));

        BatchFileDeleter deleter = capability(fileIO);
        BatchDeleteResult result = deleter.delete(Collections.singletonList(FIRST));

        assertThat(deleter.maxBatchSize()).isEqualTo(1000);
        assertThat(result.deletedOrNotFound()).containsExactly(FIRST);
        ArgumentCaptor<DeleteObjectsRequest> request =
                ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(client).deleteObjects(request.capture());
        assertThat(request.getValue().getBucketName()).isEqualTo("bucket");
        assertThat(request.getValue().getKeys()).containsExactly("table/file-0.parquet");
        assertThat(request.getValue().isQuiet()).isFalse();
        assertThat(fileIO.ossClientCalls).hasValue(1);
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testDeletesExactlyOneThousandObjectsInOneRequest() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        List<Path> files = files(1000, "bucket");
        List<String> keys = keys(files);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(keys));

        BatchDeleteResult result = capability(fileIO).delete(files);

        assertThat(result.deletedOrNotFound()).containsExactlyElementsOf(files);
        ArgumentCaptor<DeleteObjectsRequest> request =
                ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(client, times(1)).deleteObjects(request.capture());
        assertThat(request.getValue().getKeys()).hasSize(1000).containsExactlyElementsOf(keys);
        assertThat(request.getValue().isQuiet()).isFalse();
        assertThat(fileIO.ossClientCalls).hasValue(1);
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testReverseOrderAcknowledgementReturnsInputOrder() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        List<Path> files = Arrays.asList(FIRST, new Path("oss://bucket/table/file-1.parquet"));
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(
                        new DeleteObjectsResult(
                                Arrays.asList("table/file-1.parquet", "table/file-0.parquet")));

        BatchDeleteResult result = capability(fileIO).delete(files);

        assertThat(result.deletedOrNotFound()).containsExactlyElementsOf(files);
        ArgumentCaptor<DeleteObjectsRequest> request =
                ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(client).deleteObjects(request.capture());
        assertThat(request.getValue().getKeys())
                .containsExactly("table/file-0.parquet", "table/file-1.parquet");
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testRejectsEmptyBatchBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.emptyList()))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsOneThousandAndOneObjectsBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(() -> capability(fileIO).delete(files(1001, "bucket")))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsMixedBucketsBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        List<Path> files =
                Arrays.asList(FIRST, new Path("oss://other-bucket/table/file-1.parquet"));

        assertThatThrownBy(() -> capability(fileIO).delete(files))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @ParameterizedTest(name = "rejects invalid OSS bucket: {0}")
    @MethodSource("invalidBuckets")
    void testRejectsInvalidBucketBeforeObtainingClient(String description, String bucket)
            throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        Path file = new Path("oss://" + bucket + "/table/file.parquet");
        assertThat(OSSUtils.validateBucketName(bucket)).as(description).isFalse();

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(file)))
                .as(description)
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @ParameterizedTest(name = "rejects non-bucket OSS authority: {0}")
    @MethodSource("invalidAuthorities")
    void testRejectsNonBucketAuthorityBeforeObtainingClient(String description, String location)
            throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        Path file = new Path(location);
        assertThat(file.toUri().getHost()).isEqualTo("bucket");
        assertThat(file.toUri().getAuthority()).as(description).isNotEqualTo("bucket");

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(file)))
                .as(description)
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsDuplicatePathsBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(() -> capability(fileIO).delete(Arrays.asList(FIRST, FIRST)))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsWrongSchemeBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(
                        () ->
                                capability(fileIO)
                                        .delete(
                                                Collections.singletonList(
                                                        new Path(
                                                                "s3://bucket/table/file-0.parquet"))))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsEmptyObjectKeyBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(
                        () ->
                                capability(fileIO)
                                        .delete(
                                                Collections.singletonList(
                                                        new Path("oss://bucket/"))))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsDistinctPathsWithSameObjectKeyBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        Path first = new Path(URI.create("oss://bucket/table/same.parquet#first"));
        Path second = new Path(URI.create("oss://bucket/table/same.parquet#second"));
        assertThat(first).isNotEqualTo(second);
        assertThat(key(first)).isEqualTo(key(second));

        assertThatThrownBy(() -> capability(fileIO).delete(Arrays.asList(first, second)))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsNullBatchBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(() -> capability(fileIO).delete(null))
                .isInstanceOfAny(
                        NullPointerException.class,
                        IllegalArgumentException.class,
                        IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testRejectsNullPathBeforeObtainingClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);

        assertThatThrownBy(() -> capability(fileIO).delete(Arrays.asList(FIRST, null)))
                .isInstanceOfAny(
                        NullPointerException.class,
                        IllegalArgumentException.class,
                        IOException.class);

        assertNoRemoteRequest(fileIO, client);
    }

    @Test
    void testSdkExceptionIsHardFailureWithoutSingleDeleteFallback() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        OSSException failure = new OSSException("service failed");
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(failure);

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(FIRST)))
                .isInstanceOf(IOException.class)
                .hasCause(failure);

        verify(client).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testClientExceptionIsHardFailureWithoutSingleDeleteFallback() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        ClientException failure = new ClientException("client failed");
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(failure);

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(FIRST)))
                .isInstanceOf(IOException.class)
                .hasCause(failure);

        verify(client).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testClientAcquisitionFailureIsHardFailureWithoutRemoteRequestOrFallback()
            throws Exception {
        OSSClient client = mock(OSSClient.class);
        ClientException failure = new ClientException("client acquisition failed");
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client, failure);

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(FIRST)))
                .isInstanceOf(IOException.class)
                .hasCause(failure);

        assertThat(fileIO.ossClientCalls).hasValue(1);
        verify(client, never()).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testRetryAfterIndeterminatePartialSuccessResubmitsCompleteBatch() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        List<Path> files = Arrays.asList(FIRST, new Path("oss://bucket/table/file-1.parquet"));
        List<String> expectedKeys = keys(files);
        List<List<String>> submittedKeys = new ArrayList<>();
        List<String> simulatedRemoteDeleted = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        ClientException failure = new ClientException("response lost after partial success");
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            DeleteObjectsRequest request = invocation.getArgument(0);
                            submittedKeys.add(new ArrayList<>(request.getKeys()));
                            if (attempts.getAndIncrement() == 0) {
                                simulatedRemoteDeleted.add(request.getKeys().get(0));
                                throw failure;
                            }
                            simulatedRemoteDeleted.clear();
                            simulatedRemoteDeleted.addAll(request.getKeys());
                            return new DeleteObjectsResult(new ArrayList<>(request.getKeys()));
                        });
        BatchFileDeleter deleter = capability(fileIO);

        assertThatThrownBy(() -> deleter.delete(files))
                .isInstanceOf(IOException.class)
                .hasCause(failure);
        assertThat(simulatedRemoteDeleted).containsExactly(expectedKeys.get(0));
        assertNoSingleDeleteFallback(fileIO);

        BatchDeleteResult result = deleter.delete(files);

        assertThat(result.deletedOrNotFound()).containsExactlyElementsOf(files);
        assertThat(submittedKeys).containsExactly(expectedKeys, expectedKeys);
        assertThat(simulatedRemoteDeleted).containsExactlyElementsOf(expectedKeys);
        verify(client, times(2)).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testNullSdkResponseIsHardFailureWithoutFallback() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(null);

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(FIRST)))
                .isInstanceOf(IOException.class);

        verify(client).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    @Test
    void testNullDeletedObjectsIsHardFailureWithoutFallback() throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        DeleteObjectsResult response = mock(DeleteObjectsResult.class);
        when(response.getDeletedObjects()).thenReturn(null);
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(response);

        assertThatThrownBy(() -> capability(fileIO).delete(Collections.singletonList(FIRST)))
                .isInstanceOf(IOException.class);

        verify(client).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    @ParameterizedTest(name = "rejects malformed response: {0}")
    @MethodSource("malformedResponses")
    void testRejectsMalformedAcknowledgementWithoutFallback(
            String description, List<String> responseKeys) throws Exception {
        OSSClient client = mock(OSSClient.class);
        StrictTestOSSFileIO fileIO = new StrictTestOSSFileIO(client);
        List<Path> files = Arrays.asList(FIRST, new Path("oss://bucket/table/file-1.parquet"));
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(responseKeys));

        assertThatThrownBy(() -> capability(fileIO).delete(files))
                .as(description)
                .isInstanceOf(IOException.class);

        verify(client).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    private static Stream<Arguments> malformedResponses() {
        return Stream.of(
                Arguments.of("missing key", Collections.singletonList("table/file-0.parquet")),
                Arguments.of(
                        "same-length replacement",
                        Arrays.asList("table/file-0.parquet", "table/unrequested.parquet")),
                Arguments.of(
                        "same-length duplicate",
                        Arrays.asList("table/file-0.parquet", "table/file-0.parquet")),
                Arguments.of(
                        "extra key",
                        Arrays.asList(
                                "table/file-0.parquet",
                                "table/file-1.parquet",
                                "table/unrequested.parquet")),
                Arguments.of(
                        "duplicate key",
                        Arrays.asList(
                                "table/file-0.parquet",
                                "table/file-1.parquet",
                                "table/file-1.parquet")),
                Arguments.of("null acknowledgement", Arrays.asList("table/file-0.parquet", null)));
    }

    private static Stream<Arguments> invalidBuckets() {
        return Stream.of(
                Arguments.of("uppercase", "Bucket"),
                Arguments.of("shorter than three characters", "ab"),
                Arguments.of(
                        "longer than sixty-three characters",
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Arguments.of("leading hyphen", "-bucket"),
                Arguments.of("trailing hyphen", "bucket-"));
    }

    private static Stream<Arguments> invalidAuthorities() {
        return Stream.of(
                Arguments.of("userinfo", "oss://user@bucket/table/file.parquet"),
                Arguments.of("port", "oss://bucket:123/table/file.parquet"));
    }

    private static BatchFileDeleter capability(OSSFileIO fileIO) throws IOException {
        return fileIO.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
    }

    private static List<Path> files(int count, String bucket) {
        List<Path> files = new ArrayList<>(count);
        IntStream.range(0, count)
                .mapToObj(i -> new Path("oss://" + bucket + "/table/file-" + i + ".parquet"))
                .forEach(files::add);
        return files;
    }

    private static List<String> keys(List<Path> files) {
        List<String> keys = new ArrayList<>(files.size());
        for (Path file : files) {
            keys.add(key(file));
        }
        return keys;
    }

    private static String key(Path path) {
        return path.toUri().getPath().substring(1);
    }

    private static void assertNoRemoteRequest(StrictTestOSSFileIO fileIO, OSSClient client) {
        assertThat(fileIO.ossClientCalls).hasValue(0);
        verify(client, never()).deleteObjects(any(DeleteObjectsRequest.class));
        assertNoSingleDeleteFallback(fileIO);
    }

    private static void assertNoSingleDeleteFallback(StrictTestOSSFileIO fileIO) {
        assertThat(fileIO.singleDeleteCalls).hasValue(0);
        assertThat(fileIO.hadoopFileSystemCalls).hasValue(0);
        verify(fileIO.client, never()).deleteObject(anyString(), anyString());
        verify(fileIO.client, never()).deleteObject(any(GenericRequest.class));
    }

    private static class StrictTestOSSFileIO extends OSSFileIO {

        private final OSSClient client;
        private final RuntimeException clientFailure;
        private final AtomicInteger ossClientCalls = new AtomicInteger();
        private final AtomicInteger singleDeleteCalls = new AtomicInteger();
        private final AtomicInteger hadoopFileSystemCalls = new AtomicInteger();

        private StrictTestOSSFileIO(OSSClient client) {
            this(client, null);
        }

        private StrictTestOSSFileIO(OSSClient client, RuntimeException clientFailure) {
            this.client = client;
            this.clientFailure = clientFailure;
        }

        @Override
        OSSClient ossClient(Path path) {
            ossClientCalls.incrementAndGet();
            if (clientFailure != null) {
                throw clientFailure;
            }
            return client;
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            singleDeleteCalls.incrementAndGet();
            return false;
        }

        @Override
        protected org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem createFileSystem(
                org.apache.hadoop.fs.Path path) {
            hadoopFileSystemCalls.incrementAndGet();
            throw new AssertionError("Strict batch delete attempted Hadoop single-file fallback");
        }
    }
}
