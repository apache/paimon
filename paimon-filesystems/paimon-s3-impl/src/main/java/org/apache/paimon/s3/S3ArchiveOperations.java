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

import software.amazon.awssdk.services.s3.model.StorageClass;

import java.time.Duration;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** S3 archive operation helpers. */
class S3ArchiveOperations {

    private static final long SECONDS_PER_DAY = 24 * 60 * 60;

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
}
