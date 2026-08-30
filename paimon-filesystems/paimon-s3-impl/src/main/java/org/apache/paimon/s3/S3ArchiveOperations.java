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

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.GlacierJobParameters;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectAlreadyInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.ObjectNotInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.TaggingDirective;
import software.amazon.awssdk.services.s3.model.Tier;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** S3 archive operation helpers. */
class S3ArchiveOperations {

    private static final long SECONDS_PER_DAY = 24 * 60 * 60;

    private static final String RESTORE_ALREADY_IN_PROGRESS = "RestoreAlreadyInProgress";

    static final long MAX_SINGLE_COPY_SIZE = 5_000_000_000L;

    static final long DEFAULT_MULTIPART_COPY_PART_SIZE = 128L * 1024 * 1024;

    private static final long MIN_MULTIPART_COPY_PART_SIZE = 5L * 1024 * 1024;

    private static final long MAX_MULTIPART_COPY_PART_SIZE = 5L * 1024 * 1024 * 1024;

    private static final int MAX_MULTIPART_COPY_PARTS = 10_000;

    private S3ArchiveOperations() {}

    static StorageClass archiveStorageClass(StorageType type) {
        switch (checkNotNull(type, "Storage type must not be null.")) {
            case ARCHIVE:
                return StorageClass.GLACIER;
            case COLD_ARCHIVE:
                return StorageClass.DEEP_ARCHIVE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported S3 archive storage type: " + type + ". Use unarchive.");
        }
    }

    static StorageClass unarchiveStorageClass(StorageType type) {
        if (checkNotNull(type, "Storage type must not be null.") == StorageType.STANDARD) {
            return StorageClass.STANDARD;
        }
        throw new IllegalArgumentException("Unsupported S3 unarchive storage type: " + type);
    }

    static int restoreDays(Duration duration) {
        checkNotNull(duration, "Restore duration must not be null.");
        checkArgument(
                !duration.isZero() && !duration.isNegative(),
                "Restore duration must be greater than zero.");

        long seconds = duration.getSeconds();
        long days = seconds / SECONDS_PER_DAY;
        if (seconds % SECONDS_PER_DAY != 0 || duration.getNano() > 0) {
            days++;
        }

        checkArgument(days <= Integer.MAX_VALUE, "Restore duration is too large: %s", duration);
        return (int) days;
    }

    static void changeStorageClass(
            S3AFileSystem fileSystem,
            org.apache.hadoop.fs.Path path,
            StorageClass storageClass,
            String operation)
            throws IOException {
        String key = fileSystem.pathToKey(path);
        changeStorageClass(
                s3Client(fileSystem),
                requestBuilderFactory(fileSystem),
                fileSystem.getBucket(),
                key,
                fileSystem.getObjectMetadata(path),
                storageClass,
                operation,
                DEFAULT_MULTIPART_COPY_PART_SIZE);
    }

    static void changeStorageClass(
            S3Client client,
            ArchiveRequestBuilderFactory factory,
            String bucket,
            String key,
            HeadObjectResponse metadata,
            StorageClass storageClass,
            String operation,
            long multipartCopyPartSize)
            throws IOException {
        try {
            if (useSingleCopy(metadata)) {
                client.copyObject(copyObjectRequest(factory, key, metadata, storageClass));
            } else {
                copyObjectWithMultipartUpload(
                        client,
                        factory,
                        bucket,
                        key,
                        metadata,
                        storageClass,
                        multipartCopyPartSize);
            }
        } catch (ObjectNotInActiveTierErrorException e) {
            throw new IOException(
                    "S3 object " + key + " is not in active tier. Restore it before unarchiving.",
                    e);
        } catch (S3Exception e) {
            throw new IOException(
                    "Failed to " + operation + " S3 object " + key + " to " + storageClass + ".",
                    e);
        } catch (SdkException e) {
            throw new IOException(
                    "Failed to " + operation + " S3 object " + key + " to " + storageClass + ".",
                    e);
        }
    }

    static CopyObjectRequest copyObjectRequest(
            CopyObjectRequestBuilderFactory factory,
            String key,
            HeadObjectResponse metadata,
            StorageClass storageClass) {
        return factory.create(key, key, metadata)
                .storageClass(storageClass)
                .metadataDirective(MetadataDirective.COPY)
                .taggingDirective(TaggingDirective.COPY)
                .build();
    }

    private static boolean useSingleCopy(HeadObjectResponse metadata) {
        Long contentLength =
                checkNotNull(
                        metadata.contentLength(), "S3 object content length must not be null.");
        return contentLength <= MAX_SINGLE_COPY_SIZE;
    }

    private static void copyObjectWithMultipartUpload(
            S3Client client,
            ArchiveRequestBuilderFactory factory,
            String bucket,
            String key,
            HeadObjectResponse metadata,
            StorageClass storageClass,
            long partSize)
            throws IOException {
        String uploadId = null;
        try {
            CreateMultipartUploadResponse createResponse =
                    client.createMultipartUpload(
                            createMultipartUploadRequest(
                                    factory, client, bucket, key, metadata, storageClass));
            uploadId =
                    checkNotNull(
                            createResponse.uploadId(), "S3 multipart upload ID must not be null.");

            List<CompletedPart> completedParts = new ArrayList<>();
            for (CopyPartRange range : copyPartRanges(metadata.contentLength(), partSize)) {
                UploadPartCopyResponse partResponse =
                        client.uploadPartCopy(
                                uploadPartCopyRequest(
                                        bucket, key, uploadId, range, metadata.eTag()));
                CopyPartResult copyPartResult =
                        checkNotNull(
                                partResponse.copyPartResult(),
                                "S3 copy part result must not be null.");
                completedParts.add(
                        CompletedPart.builder()
                                .partNumber(range.partNumber)
                                .eTag(copyPartResult.eTag())
                                .build());
            }

            client.completeMultipartUpload(
                    factory.newCompleteMultipartUploadRequestBuilder(key, uploadId, completedParts)
                            .build());
        } catch (IOException | RuntimeException e) {
            if (uploadId != null) {
                try {
                    client.abortMultipartUpload(
                            factory.newAbortMultipartUploadRequestBuilder(key, uploadId).build());
                } catch (RuntimeException abortException) {
                    e.addSuppressed(abortException);
                }
            }
            throw e;
        }
    }

    @SuppressWarnings("deprecation")
    static CreateMultipartUploadRequest createMultipartUploadRequest(
            ArchiveRequestBuilderFactory factory,
            S3Client client,
            String bucket,
            String key,
            HeadObjectResponse metadata,
            StorageClass storageClass)
            throws IOException {
        CreateMultipartUploadRequest.Builder builder =
                factory.newMultipartUploadRequestBuilder(key)
                        .storageClass(storageClass)
                        .metadata(metadata.metadata());
        if (metadata.cacheControl() != null) {
            builder.cacheControl(metadata.cacheControl());
        }
        if (metadata.contentDisposition() != null) {
            builder.contentDisposition(metadata.contentDisposition());
        }
        if (metadata.contentEncoding() != null) {
            builder.contentEncoding(metadata.contentEncoding());
        }
        if (metadata.contentLanguage() != null) {
            builder.contentLanguage(metadata.contentLanguage());
        }
        if (metadata.contentType() != null) {
            builder.contentType(metadata.contentType());
        }
        if (metadata.expires() != null) {
            builder.expires(metadata.expires());
        }
        if (metadata.websiteRedirectLocation() != null) {
            builder.websiteRedirectLocation(metadata.websiteRedirectLocation());
        }
        if (metadata.objectLockMode() != null) {
            builder.objectLockMode(metadata.objectLockMode());
        }
        if (metadata.objectLockRetainUntilDate() != null) {
            builder.objectLockRetainUntilDate(metadata.objectLockRetainUntilDate());
        }
        if (metadata.objectLockLegalHoldStatus() != null) {
            builder.objectLockLegalHoldStatus(metadata.objectLockLegalHoldStatus());
        }

        Tagging tagging = objectTagging(client, bucket, key);
        if (tagging.hasTagSet() && !tagging.tagSet().isEmpty()) {
            builder.tagging(tagging);
        }
        return builder.build();
    }

    private static Tagging objectTagging(S3Client client, String bucket, String key) {
        GetObjectTaggingResponse response =
                client.getObjectTagging(
                        GetObjectTaggingRequest.builder().bucket(bucket).key(key).build());
        return Tagging.builder().tagSet(response.tagSet()).build();
    }

    static UploadPartCopyRequest uploadPartCopyRequest(
            String bucket, String key, String uploadId, CopyPartRange range, String eTag) {
        UploadPartCopyRequest.Builder builder =
                UploadPartCopyRequest.builder()
                        .sourceBucket(bucket)
                        .sourceKey(key)
                        .destinationBucket(bucket)
                        .destinationKey(key)
                        .uploadId(uploadId)
                        .partNumber(range.partNumber)
                        .copySourceRange(range.header());
        if (eTag != null) {
            builder.copySourceIfMatch(eTag);
        }
        return builder.build();
    }

    static List<CopyPartRange> copyPartRanges(long objectSize, long configuredPartSize) {
        checkArgument(objectSize > 0, "Object size must be greater than zero.");
        long partSize = multipartCopyPartSize(objectSize, configuredPartSize);
        List<CopyPartRange> ranges = new ArrayList<>();
        long start = 0;
        int partNumber = 1;
        while (start < objectSize) {
            long end = Math.min(start + partSize, objectSize) - 1;
            ranges.add(new CopyPartRange(partNumber, start, end));
            start = end + 1;
            partNumber++;
        }
        return ranges;
    }

    private static long multipartCopyPartSize(long objectSize, long configuredPartSize) {
        checkArgument(
                configuredPartSize > 0, "Multipart copy part size must be greater than zero.");
        long minimumPartSize =
                (objectSize + MAX_MULTIPART_COPY_PARTS - 1) / MAX_MULTIPART_COPY_PARTS;
        long partSize =
                Math.max(
                        Math.max(configuredPartSize, MIN_MULTIPART_COPY_PART_SIZE),
                        minimumPartSize);
        checkArgument(
                partSize <= MAX_MULTIPART_COPY_PART_SIZE,
                "S3 object is too large for multipart copy: %s bytes.",
                objectSize);
        return partSize;
    }

    static void restoreArchive(S3AFileSystem fileSystem, org.apache.hadoop.fs.Path path, int days)
            throws IOException {
        RestoreObjectRequest request =
                restoreObjectRequest(fileSystem.getBucket(), fileSystem.pathToKey(path), days);

        try {
            s3Client(fileSystem).restoreObject(request);
        } catch (ObjectAlreadyInActiveTierErrorException e) {
            throw new IOException("S3 object " + path + " is already in active tier.", e);
        } catch (S3Exception e) {
            if (isRestoreAlreadyInProgress(e)) {
                return;
            }
            throw new IOException("Failed to restore archived S3 object " + path + ".", e);
        } catch (SdkException e) {
            throw new IOException("Failed to restore archived S3 object " + path + ".", e);
        }
    }

    static RestoreObjectRequest restoreObjectRequest(String bucket, String key, int days) {
        return RestoreObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .restoreRequest(
                        RestoreRequest.builder()
                                .days(days)
                                .glacierJobParameters(
                                        GlacierJobParameters.builder().tier(Tier.STANDARD).build())
                                .build())
                .build();
    }

    static boolean isRestoreAlreadyInProgress(S3Exception exception) {
        return exception.awsErrorDetails() != null
                && RESTORE_ALREADY_IN_PROGRESS.equals(exception.awsErrorDetails().errorCode());
    }

    private static ArchiveRequestBuilderFactory requestBuilderFactory(S3AFileSystem fileSystem) {
        RequestFactory requestFactory = fileSystem.getRequestFactory();
        return new ArchiveRequestBuilderFactory() {
            @Override
            public CopyObjectRequest.Builder create(
                    String sourceKey, String destinationKey, HeadObjectResponse metadata) {
                return requestFactory.newCopyObjectRequestBuilder(
                        sourceKey, destinationKey, metadata);
            }

            @Override
            public CreateMultipartUploadRequest.Builder newMultipartUploadRequestBuilder(String key)
                    throws IOException {
                return requestFactory.newMultipartUploadRequestBuilder(
                        key, PutObjectOptions.keepingDirs());
            }

            @Override
            public CompleteMultipartUploadRequest.Builder newCompleteMultipartUploadRequestBuilder(
                    String key, String uploadId, List<CompletedPart> parts) {
                return requestFactory.newCompleteMultipartUploadRequestBuilder(
                        key, uploadId, parts, PutObjectOptions.keepingDirs());
            }

            @Override
            public AbortMultipartUploadRequest.Builder newAbortMultipartUploadRequestBuilder(
                    String key, String uploadId) {
                return requestFactory.newAbortMultipartUploadRequestBuilder(key, uploadId);
            }
        };
    }

    private static S3Client s3Client(S3AFileSystem fileSystem) throws IOException {
        try {
            Method method = S3AFileSystem.class.getDeclaredMethod("getS3Client");
            method.setAccessible(true);
            return (S3Client) method.invoke(fileSystem);
        } catch (NoSuchMethodException e) {
            throw new IOException("S3AFileSystem does not expose an S3 client.", e);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed to access S3 client from S3AFileSystem.", e);
        } catch (InvocationTargetException e) {
            throw new IOException("Failed to get S3 client from S3AFileSystem.", e);
        }
    }

    interface CopyObjectRequestBuilderFactory {
        CopyObjectRequest.Builder create(
                String sourceKey, String destinationKey, HeadObjectResponse metadata);
    }

    interface ArchiveRequestBuilderFactory extends CopyObjectRequestBuilderFactory {

        CreateMultipartUploadRequest.Builder newMultipartUploadRequestBuilder(String key)
                throws IOException;

        CompleteMultipartUploadRequest.Builder newCompleteMultipartUploadRequestBuilder(
                String key, String uploadId, List<CompletedPart> parts);

        AbortMultipartUploadRequest.Builder newAbortMultipartUploadRequestBuilder(
                String key, String uploadId);
    }

    static class CopyPartRange {

        private final int partNumber;
        private final long start;
        private final long end;

        private CopyPartRange(int partNumber, long start, long end) {
            this.partNumber = partNumber;
            this.start = start;
            this.end = end;
        }

        String header() {
            return "bytes=" + start + "-" + end;
        }
    }
}
