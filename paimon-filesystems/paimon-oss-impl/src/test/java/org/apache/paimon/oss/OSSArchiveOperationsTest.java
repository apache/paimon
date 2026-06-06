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

import com.aliyun.oss.OSSException;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.RestoreObjectRequest;
import com.aliyun.oss.model.RestoreTier;
import com.aliyun.oss.model.StorageClass;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OSSArchiveOperations}. */
class OSSArchiveOperationsTest {

    @Test
    void testArchiveStorageClassMapping() {
        assertThat(OSSArchiveOperations.archiveStorageClass(StorageType.ARCHIVE))
                .isEqualTo(StorageClass.Archive);
        assertThat(OSSArchiveOperations.archiveStorageClass(StorageType.COLD_ARCHIVE))
                .isEqualTo(StorageClass.ColdArchive);

        assertThatThrownBy(() -> OSSArchiveOperations.archiveStorageClass(StorageType.STANDARD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Use unarchive");
    }

    @Test
    void testUnarchiveStorageClassMapping() {
        assertThat(OSSArchiveOperations.unarchiveStorageClass(StorageType.STANDARD))
                .isEqualTo(StorageClass.Standard);

        assertThatThrownBy(() -> OSSArchiveOperations.unarchiveStorageClass(StorageType.ARCHIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported OSS unarchive storage type");
        assertThatThrownBy(() -> OSSArchiveOperations.unarchiveStorageClass(StorageType.COLD_ARCHIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported OSS unarchive storage type");
    }

    @Test
    void testRestoreDays() {
        assertThat(OSSArchiveOperations.restoreDays(Duration.ofNanos(1))).isEqualTo(1);
        assertThat(OSSArchiveOperations.restoreDays(Duration.ofHours(24))).isEqualTo(1);
        assertThat(OSSArchiveOperations.restoreDays(Duration.ofHours(25))).isEqualTo(2);
        assertThat(OSSArchiveOperations.restoreDays(Duration.ofDays(7))).isEqualTo(7);

        assertThatThrownBy(() -> OSSArchiveOperations.restoreDays(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
        assertThatThrownBy(() -> OSSArchiveOperations.restoreDays(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than zero");
    }

    @Test
    void testCopyObjectRequest() {
        CopyObjectRequest request =
                OSSArchiveOperations.copyObjectRequest(
                        "bucket", "partition/data.orc", StorageClass.Archive);

        assertThat(request.getSourceBucketName()).isEqualTo("bucket");
        assertThat(request.getSourceKey()).isEqualTo("partition/data.orc");
        assertThat(request.getDestinationBucketName()).isEqualTo("bucket");
        assertThat(request.getDestinationKey()).isEqualTo("partition/data.orc");
        assertThat(request.getNewObjectMetadata().getRawMetadata())
                .containsEntry(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Archive);
    }

    @Test
    void testRestoreObjectRequest() {
        RestoreObjectRequest request =
                OSSArchiveOperations.restoreObjectRequest("bucket", "partition/data.orc", 7);

        assertThat(request.getBucketName()).isEqualTo("bucket");
        assertThat(request.getKey()).isEqualTo("partition/data.orc");
        assertThat(request.getRestoreConfiguration().getDays()).isEqualTo(7);
        assertThat(request.getRestoreConfiguration().getRestoreJobParameters().getRestoreTier())
                .isEqualTo(RestoreTier.RESTORE_TIER_STANDARD);
    }

    @Test
    void testRestoreAlreadyInProgress() {
        assertThat(
                        OSSArchiveOperations.isRestoreAlreadyInProgress(
                                ossException("RestoreAlreadyInProgress")))
                .isTrue();
        assertThat(OSSArchiveOperations.isRestoreAlreadyInProgress(ossException("OtherError")))
                .isFalse();
    }

    private static OSSException ossException(String errorCode) {
        return new OSSException(
                "message", errorCode, "request-id", "host-id", "raw", "resource", "header");
    }
}
