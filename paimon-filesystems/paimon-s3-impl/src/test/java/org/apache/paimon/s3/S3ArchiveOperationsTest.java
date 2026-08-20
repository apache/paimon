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

package org.apache.paimon.s3;

import org.apache.paimon.fs.StorageType;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.TaggingDirective;
import software.amazon.awssdk.services.s3.model.Tier;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3ArchiveOperations}. */
class S3ArchiveOperationsTest {

    @Test
    void testArchiveStorageClassMapping() {
        assertThat(S3ArchiveOperations.archiveStorageClass(StorageType.ARCHIVE))
                .isEqualTo(StorageClass.GLACIER);
        assertThat(S3ArchiveOperations.archiveStorageClass(StorageType.COLD_ARCHIVE))
                .isEqualTo(StorageClass.DEEP_ARCHIVE);

        assertThatThrownBy(() -> S3ArchiveOperations.archiveStorageClass(StorageType.STANDARD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Use unarchive");
    }

    @Test
    void testUnarchiveStorageClassMapping() {
        assertThat(S3ArchiveOperations.unarchiveStorageClass(StorageType.STANDARD))
                .isEqualTo(StorageClass.STANDARD);

        assertThatThrownBy(() -> S3ArchiveOperations.unarchiveStorageClass(StorageType.ARCHIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported S3 unarchive storage type");
        assertThatThrownBy(
                        () -> S3ArchiveOperations.unarchiveStorageClass(StorageType.COLD_ARCHIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported S3 unarchive storage type");
    }

    @Test
    void testRestoreDays() {
        assertThat(S3ArchiveOperations.restoreDays(Duration.ofNanos(1))).isEqualTo(1);
        assertThat(S3ArchiveOperations.restoreDays(Duration.ofHours(24))).isEqualTo(1);
        assertThat(S3ArchiveOperations.restoreDays(Duration.ofHours(25))).isEqualTo(2);
        assertThat(S3ArchiveOperations.restoreDays(Duration.ofDays(7))).isEqualTo(7);

        assertThatThrownBy(() -> S3ArchiveOperations.restoreDays(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
        assertThatThrownBy(() -> S3ArchiveOperations.restoreDays(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
    }

    @Test
    void testRestoreObjectRequest() {
        RestoreObjectRequest request =
                S3ArchiveOperations.restoreObjectRequest("bucket", "partition/data.orc", 7);

        assertThat(request.bucket()).isEqualTo("bucket");
        assertThat(request.key()).isEqualTo("partition/data.orc");
        assertThat(request.restoreRequest().days()).isEqualTo(7);
        assertThat(request.restoreRequest().glacierJobParameters().tier()).isEqualTo(Tier.STANDARD);
    }

    @Test
    void testCopyObjectRequest() {
        AtomicReference<String> source = new AtomicReference<>();
        AtomicReference<String> destination = new AtomicReference<>();

        CopyObjectRequest request =
                S3ArchiveOperations.copyObjectRequest(
                        (sourceKey, destinationKey, metadata) -> {
                            source.set(sourceKey);
                            destination.set(destinationKey);
                            return CopyObjectRequest.builder()
                                    .sourceKey(sourceKey)
                                    .destinationKey(destinationKey);
                        },
                        "partition/data.orc",
                        HeadObjectResponse.builder().build(),
                        StorageClass.GLACIER);

        assertThat(source).hasValue("partition/data.orc");
        assertThat(destination).hasValue("partition/data.orc");
        assertThat(request.sourceKey()).isEqualTo("partition/data.orc");
        assertThat(request.destinationKey()).isEqualTo("partition/data.orc");
        assertThat(request.storageClass()).isEqualTo(StorageClass.GLACIER);
        assertThat(request.metadataDirective()).isEqualTo(MetadataDirective.COPY);
        assertThat(request.taggingDirective()).isEqualTo(TaggingDirective.COPY);
    }

    @Test
    void testChangeStorageClassUsesSingleCopyAtMaxSingleCopySize() throws IOException {
        RecordingS3Client client = new RecordingS3Client();
        TestArchiveRequestBuilderFactory factory = new TestArchiveRequestBuilderFactory("bucket");

        S3ArchiveOperations.changeStorageClass(
                client,
                factory,
                "bucket",
                "partition/data.orc",
                HeadObjectResponse.builder()
                        .contentLength(S3ArchiveOperations.MAX_SINGLE_COPY_SIZE)
                        .build(),
                StorageClass.GLACIER,
                "archive",
                S3ArchiveOperations.DEFAULT_MULTIPART_COPY_PART_SIZE);

        assertThat(client.copyObjectRequests).hasSize(1);
        assertThat(client.createMultipartUploadRequests).isEmpty();
        assertThat(client.uploadPartCopyRequests).isEmpty();
        assertThat(client.getObjectTaggingRequests).isEmpty();
        assertThat(client.copyObjectRequests.get(0).storageClass()).isEqualTo(StorageClass.GLACIER);
    }

    @Test
    void testChangeStorageClassUsesMultipartCopyForLargeObject() throws IOException {
        RecordingS3Client client = new RecordingS3Client();
        client.tags.add(Tag.builder().key("project").value("paimon").build());
        TestArchiveRequestBuilderFactory factory = new TestArchiveRequestBuilderFactory("bucket");
        long objectSize = S3ArchiveOperations.MAX_SINGLE_COPY_SIZE + 1;

        S3ArchiveOperations.changeStorageClass(
                client,
                factory,
                "bucket",
                "partition/data.orc",
                HeadObjectResponse.builder()
                        .contentLength(objectSize)
                        .eTag("\"etag\"")
                        .contentType("application/octet-stream")
                        .cacheControl("max-age=60")
                        .metadata(Collections.singletonMap("owner", "paimon"))
                        .build(),
                StorageClass.DEEP_ARCHIVE,
                "archive",
                S3ArchiveOperations.DEFAULT_MULTIPART_COPY_PART_SIZE);

        assertThat(client.copyObjectRequests).isEmpty();
        assertThat(client.getObjectTaggingRequests).hasSize(1);
        assertThat(client.createMultipartUploadRequests).hasSize(1);
        assertThat(client.uploadPartCopyRequests).isNotEmpty();
        assertThat(client.completeMultipartUploadRequests).hasSize(1);
        assertThat(client.abortMultipartUploadRequests).isEmpty();

        CreateMultipartUploadRequest createRequest = client.createMultipartUploadRequests.get(0);
        assertThat(createRequest.bucket()).isEqualTo("bucket");
        assertThat(createRequest.key()).isEqualTo("partition/data.orc");
        assertThat(createRequest.storageClass()).isEqualTo(StorageClass.DEEP_ARCHIVE);
        assertThat(createRequest.contentType()).isEqualTo("application/octet-stream");
        assertThat(createRequest.cacheControl()).isEqualTo("max-age=60");
        assertThat(createRequest.metadata()).containsEntry("owner", "paimon");
        assertThat(createRequest.tagging()).contains("project=paimon");

        UploadPartCopyRequest firstPart = client.uploadPartCopyRequests.get(0);
        assertThat(firstPart.sourceBucket()).isEqualTo("bucket");
        assertThat(firstPart.sourceKey()).isEqualTo("partition/data.orc");
        assertThat(firstPart.destinationBucket()).isEqualTo("bucket");
        assertThat(firstPart.destinationKey()).isEqualTo("partition/data.orc");
        assertThat(firstPart.copySourceIfMatch()).isEqualTo("\"etag\"");
        assertThat(firstPart.copySourceRange())
                .isEqualTo("bytes=0-" + (S3ArchiveOperations.DEFAULT_MULTIPART_COPY_PART_SIZE - 1));

        UploadPartCopyRequest lastPart =
                client.uploadPartCopyRequests.get(client.uploadPartCopyRequests.size() - 1);
        assertThat(lastPart.copySourceRange()).endsWith("-" + (objectSize - 1));

        List<CompletedPart> completedParts =
                client.completeMultipartUploadRequests.get(0).multipartUpload().parts();
        assertThat(completedParts).hasSize(client.uploadPartCopyRequests.size());
        assertThat(completedParts.get(0).partNumber()).isEqualTo(1);
        assertThat(completedParts.get(0).eTag()).isEqualTo("etag-1");
    }

    @Test
    void testCopyPartRanges() {
        long partSize = 5L * 1024 * 1024;
        List<S3ArchiveOperations.CopyPartRange> ranges =
                S3ArchiveOperations.copyPartRanges(partSize * 2 + 1, partSize);

        assertThat(ranges)
                .extracting(S3ArchiveOperations.CopyPartRange::header)
                .containsExactly(
                        "bytes=0-" + (partSize - 1),
                        "bytes=" + partSize + "-" + (partSize * 2 - 1),
                        "bytes=" + (partSize * 2) + "-" + (partSize * 2));
    }

    @Test
    void testMultipartCopyAbortsOnPartFailure() {
        RecordingS3Client client = new RecordingS3Client();
        client.failPartNumber = 2;
        TestArchiveRequestBuilderFactory factory = new TestArchiveRequestBuilderFactory("bucket");
        long partSize = S3ArchiveOperations.DEFAULT_MULTIPART_COPY_PART_SIZE;

        assertThatThrownBy(
                        () ->
                                S3ArchiveOperations.changeStorageClass(
                                        client,
                                        factory,
                                        "bucket",
                                        "partition/data.orc",
                                        HeadObjectResponse.builder()
                                                .contentLength(
                                                        S3ArchiveOperations.MAX_SINGLE_COPY_SIZE
                                                                + partSize)
                                                .build(),
                                        StorageClass.GLACIER,
                                        "archive",
                                        partSize))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to archive");

        assertThat(client.createMultipartUploadRequests).hasSize(1);
        assertThat(client.uploadPartCopyRequests).hasSize(2);
        assertThat(client.completeMultipartUploadRequests).isEmpty();
        assertThat(client.abortMultipartUploadRequests).hasSize(1);
        assertThat(client.abortMultipartUploadRequests.get(0).uploadId()).isEqualTo("upload-id");
    }

    @Test
    void testRestoreAlreadyInProgress() {
        assertThat(
                        S3ArchiveOperations.isRestoreAlreadyInProgress(
                                s3Exception("RestoreAlreadyInProgress")))
                .isTrue();
        assertThat(S3ArchiveOperations.isRestoreAlreadyInProgress(s3Exception("OtherError")))
                .isFalse();
    }

    private static class TestArchiveRequestBuilderFactory
            implements S3ArchiveOperations.ArchiveRequestBuilderFactory {

        private final String bucket;

        private TestArchiveRequestBuilderFactory(String bucket) {
            this.bucket = bucket;
        }

        @Override
        public CopyObjectRequest.Builder create(
                String sourceKey, String destinationKey, HeadObjectResponse metadata) {
            return CopyObjectRequest.builder()
                    .sourceBucket(bucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucket)
                    .destinationKey(destinationKey);
        }

        @Override
        public CreateMultipartUploadRequest.Builder newMultipartUploadRequestBuilder(String key) {
            return CreateMultipartUploadRequest.builder().bucket(bucket).key(key);
        }

        @Override
        public CompleteMultipartUploadRequest.Builder newCompleteMultipartUploadRequestBuilder(
                String key, String uploadId, List<CompletedPart> parts) {
            return CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build());
        }

        @Override
        public AbortMultipartUploadRequest.Builder newAbortMultipartUploadRequestBuilder(
                String key, String uploadId) {
            return AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId);
        }
    }

    private static class RecordingS3Client implements S3Client {

        private final List<CopyObjectRequest> copyObjectRequests = new ArrayList<>();
        private final List<GetObjectTaggingRequest> getObjectTaggingRequests = new ArrayList<>();
        private final List<CreateMultipartUploadRequest> createMultipartUploadRequests =
                new ArrayList<>();
        private final List<UploadPartCopyRequest> uploadPartCopyRequests = new ArrayList<>();
        private final List<CompleteMultipartUploadRequest> completeMultipartUploadRequests =
                new ArrayList<>();
        private final List<AbortMultipartUploadRequest> abortMultipartUploadRequests =
                new ArrayList<>();
        private final List<Tag> tags = new ArrayList<>();

        private int failPartNumber = -1;

        @Override
        public String serviceName() {
            return S3Client.SERVICE_NAME;
        }

        @Override
        public void close() {}

        @Override
        public CopyObjectResponse copyObject(CopyObjectRequest copyObjectRequest) {
            copyObjectRequests.add(copyObjectRequest);
            return CopyObjectResponse.builder().build();
        }

        @Override
        public GetObjectTaggingResponse getObjectTagging(GetObjectTaggingRequest request) {
            getObjectTaggingRequests.add(request);
            return GetObjectTaggingResponse.builder().tagSet(tags).build();
        }

        @Override
        public CreateMultipartUploadResponse createMultipartUpload(
                CreateMultipartUploadRequest request) {
            createMultipartUploadRequests.add(request);
            return CreateMultipartUploadResponse.builder().uploadId("upload-id").build();
        }

        @Override
        public UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest request) {
            uploadPartCopyRequests.add(request);
            if (request.partNumber() == failPartNumber) {
                throw S3Exception.builder().message("copy failed").build();
            }
            return UploadPartCopyResponse.builder()
                    .copyPartResult(
                            CopyPartResult.builder().eTag("etag-" + request.partNumber()).build())
                    .build();
        }

        @Override
        public CompleteMultipartUploadResponse completeMultipartUpload(
                CompleteMultipartUploadRequest request) {
            completeMultipartUploadRequests.add(request);
            return CompleteMultipartUploadResponse.builder().build();
        }

        @Override
        public AbortMultipartUploadResponse abortMultipartUpload(
                AbortMultipartUploadRequest request) {
            abortMultipartUploadRequests.add(request);
            return AbortMultipartUploadResponse.builder().build();
        }
    }

    private static S3Exception s3Exception(String errorCode) {
        return (S3Exception)
                S3Exception.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
                        .build();
    }
}
