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

import org.apache.paimon.fs.StorageType;
import org.apache.paimon.utils.ReflectionUtils;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.RestoreConfiguration;
import com.aliyun.oss.model.RestoreJobParameters;
import com.aliyun.oss.model.RestoreObjectRequest;
import com.aliyun.oss.model.RestoreTier;
import com.aliyun.oss.model.StorageClass;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;

import java.io.IOException;
import java.time.Duration;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** OSS archive operation helpers. */
class OSSArchiveOperations {

    private static final long SECONDS_PER_DAY = 24 * 60 * 60;

    private static final String RESTORE_ALREADY_IN_PROGRESS = "RestoreAlreadyInProgress";

    private OSSArchiveOperations() {}

    static StorageClass archiveStorageClass(StorageType type) {
        switch (checkNotNull(type, "Storage type must not be null.")) {
            case ARCHIVE:
                return StorageClass.Archive;
            case COLD_ARCHIVE:
                return StorageClass.ColdArchive;
            default:
                throw new IllegalArgumentException(
                        "Unsupported OSS archive storage type: " + type + ". Use unarchive.");
        }
    }

    static StorageClass unarchiveStorageClass(StorageType type) {
        if (checkNotNull(type, "Storage type must not be null.") == StorageType.STANDARD) {
            return StorageClass.Standard;
        }
        throw new IllegalArgumentException("Unsupported OSS unarchive storage type: " + type);
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

    static CopyObjectRequest copyObjectRequest(String bucket, String key, StorageClass storageClass) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, storageClass);

        CopyObjectRequest request = new CopyObjectRequest(bucket, key, bucket, key);
        request.setNewObjectMetadata(metadata);
        return request;
    }

    static RestoreObjectRequest restoreObjectRequest(String bucket, String key, int days) {
        return new RestoreObjectRequest(
                bucket,
                key,
                new RestoreConfiguration(
                        days, new RestoreJobParameters(RestoreTier.RESTORE_TIER_STANDARD)));
    }

    static void changeStorageClass(
            AliyunOSSFileSystem fileSystem,
            org.apache.hadoop.fs.Path path,
            StorageClass storageClass,
            String operation)
            throws IOException {
        String bucket = bucket(fileSystem);
        String key = pathToKey(fileSystem, path);

        try {
            ossClient(fileSystem).copyObject(copyObjectRequest(bucket, key, storageClass));
        } catch (OSSException e) {
            throw new IOException(failureMessage(operation, path, storageClass), e);
        } catch (ClientException e) {
            throw new IOException(failureMessage(operation, path, storageClass), e);
        }
    }

    static void restoreArchive(AliyunOSSFileSystem fileSystem, org.apache.hadoop.fs.Path path, int days)
            throws IOException {
        String bucket = bucket(fileSystem);
        String key = pathToKey(fileSystem, path);

        try {
            ossClient(fileSystem).restoreObject(restoreObjectRequest(bucket, key, days));
        } catch (OSSException e) {
            if (isRestoreAlreadyInProgress(e)) {
                return;
            }
            throw new IOException("Failed to restore archived OSS object " + path + ".", e);
        } catch (ClientException e) {
            throw new IOException("Failed to restore archived OSS object " + path + ".", e);
        }
    }

    static boolean isRestoreAlreadyInProgress(OSSException exception) {
        return RESTORE_ALREADY_IN_PROGRESS.equals(exception.getErrorCode());
    }

    static String pathToKey(AliyunOSSFileSystem fileSystem, org.apache.hadoop.fs.Path path) {
        org.apache.hadoop.fs.Path qualified = path;
        if (!qualified.isAbsolute()) {
            qualified = new org.apache.hadoop.fs.Path(fileSystem.getWorkingDirectory(), path);
        }

        String key = qualified.toUri().getPath();
        return key.startsWith("/") ? key.substring(1) : key;
    }

    private static String bucket(AliyunOSSFileSystem fileSystem) {
        return fileSystem.getUri().getHost();
    }

    private static OSSClient ossClient(AliyunOSSFileSystem fileSystem) throws IOException {
        AliyunOSSFileSystemStore store = fileSystem.getStore();
        try {
            return ReflectionUtils.getPrivateFieldValue(store, "ossClient");
        } catch (Exception e) {
            throw new IOException("Failed to access OSS client from AliyunOSSFileSystem.", e);
        }
    }

    private static String failureMessage(
            String operation, org.apache.hadoop.fs.Path path, StorageClass storageClass) {
        String message =
                "Failed to " + operation + " OSS object " + path + " to " + storageClass + ".";
        if ("unarchive".equals(operation)) {
            message += " Restore it before unarchiving if it is archived.";
        }
        return message;
    }
}
