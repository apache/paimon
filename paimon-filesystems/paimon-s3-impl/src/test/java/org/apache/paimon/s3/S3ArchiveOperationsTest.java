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
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tier;

import java.time.Duration;

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
    void testRestoreAlreadyInProgress() {
        assertThat(
                        S3ArchiveOperations.isRestoreAlreadyInProgress(
                                s3Exception("RestoreAlreadyInProgress")))
                .isTrue();
        assertThat(S3ArchiveOperations.isRestoreAlreadyInProgress(s3Exception("OtherError")))
                .isFalse();
    }

    private static S3Exception s3Exception(String errorCode) {
        return (S3Exception)
                S3Exception.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
                        .build();
    }
}
