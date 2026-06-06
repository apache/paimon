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

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GlacierJobParameters;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectAlreadyInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.ObjectNotInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.TaggingDirective;
import software.amazon.awssdk.services.s3.model.Tier;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** S3 archive operation helpers. */
class S3ArchiveOperations {

    private static final long SECONDS_PER_DAY = 24 * 60 * 60;

    private static final String RESTORE_ALREADY_IN_PROGRESS = "RestoreAlreadyInProgress";

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
        CopyObjectRequest request =
                fileSystem
                        .getRequestFactory()
                        .newCopyObjectRequestBuilder(key, key, fileSystem.getObjectMetadata(path))
                        .storageClass(storageClass)
                        .metadataDirective(MetadataDirective.COPY)
                        .taggingDirective(TaggingDirective.COPY)
                        .build();

        try {
            s3Client(fileSystem).copyObject(request);
        } catch (ObjectNotInActiveTierErrorException e) {
            throw new IOException(
                    "S3 object "
                            + path
                            + " is not in active tier. Restore it before unarchiving.",
                    e);
        } catch (S3Exception e) {
            throw new IOException(
                    "Failed to " + operation + " S3 object " + path + " to " + storageClass + ".",
                    e);
        } catch (SdkException e) {
            throw new IOException(
                    "Failed to " + operation + " S3 object " + path + " to " + storageClass + ".",
                    e);
        }
    }

    static void restoreArchive(S3AFileSystem fileSystem, org.apache.hadoop.fs.Path path, int days)
            throws IOException {
        RestoreObjectRequest request = restoreObjectRequest(fileSystem.getBucket(), fileSystem.pathToKey(path), days);

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
}
