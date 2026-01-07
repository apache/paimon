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

package org.apache.paimon.partition;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Statistics of a partition, fields inside may be negative, indicating that some data has been
 * removed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class PartitionStatistics implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_SPEC = "spec";
    public static final String FIELD_RECORD_COUNT = "recordCount";
    public static final String FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
    public static final String FIELD_FILE_COUNT = "fileCount";
    public static final String FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";
    public static final String FIELD_BUCKET_COUNT = "bucketCount";

    @JsonProperty(FIELD_SPEC)
    protected final Map<String, String> spec;

    @JsonProperty(FIELD_RECORD_COUNT)
    protected final long recordCount;

    @JsonProperty(FIELD_FILE_SIZE_IN_BYTES)
    protected final long fileSizeInBytes;

    @JsonProperty(FIELD_FILE_COUNT)
    protected final long fileCount;

    @JsonProperty(FIELD_LAST_FILE_CREATION_TIME)
    protected final long lastFileCreationTime;

    @JsonProperty(FIELD_BUCKET_COUNT)
    protected final int bucketCount;

    @JsonCreator
    public PartitionStatistics(
            @JsonProperty(FIELD_SPEC) Map<String, String> spec,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime,
            @JsonProperty(FIELD_BUCKET_COUNT) int bucketCount) {
        this.spec = spec;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
        this.bucketCount = bucketCount;
    }

    @JsonGetter(FIELD_SPEC)
    public Map<String, String> spec() {
        return spec;
    }

    @JsonGetter(FIELD_RECORD_COUNT)
    public long recordCount() {
        return recordCount;
    }

    @JsonGetter(FIELD_FILE_SIZE_IN_BYTES)
    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    @JsonGetter(FIELD_FILE_COUNT)
    public long fileCount() {
        return fileCount;
    }

    @JsonGetter(FIELD_LAST_FILE_CREATION_TIME)
    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }

    @JsonGetter(FIELD_BUCKET_COUNT)
    public int bucketCount() {
        return bucketCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionStatistics that = (PartitionStatistics) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && Objects.equals(spec, that.spec)
                && Objects.equals(bucketCount, that.bucketCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                spec, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime, bucketCount);
    }

    @Override
    public String toString() {
        return "{"
                + "spec="
                + spec
                + ", recordCount="
                + recordCount
                + ", fileSizeInBytes="
                + fileSizeInBytes
                + ", fileCount="
                + fileCount
                + ", lastFileCreationTime="
                + lastFileCreationTime
                + ", bucketCount="
                + bucketCount
                + '}';
    }
}
