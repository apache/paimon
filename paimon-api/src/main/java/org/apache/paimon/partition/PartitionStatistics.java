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
 * Statistics of a partition.
 *
 * <p>The numeric fields are read on two planes, and a negative value means a different thing on
 * each. Which plane an instance belongs to follows from where it came from, never from the value:
 *
 * <ul>
 *   <li><b>Delta plane</b> — what a commit changed. A negative value is a decrement, and the server
 *       adds it to what it already holds. This is what a table snapshot commit reports.
 *   <li><b>Observation plane</b> — what a partition currently holds, as returned by {@code
 *       listPartitions}. A negative value ({@link #UNKNOWN}) means nobody ever reported that field,
 *       and {@code 0} means an exact zero. The two are not interchangeable: a consumer that treats
 *       unknown as zero plans against an empty partition that may hold a billion rows.
 * </ul>
 *
 * <p>Unknown is per field, not per partition: a reporter that only knows the file count leaves the
 * record count {@link #UNKNOWN} and fills the rest. Use {@link #isKnown(long)} rather than
 * comparing against {@code -1}; any negative value on the observation plane is unknown.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class PartitionStatistics implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Canonical encoding of "this field was never reported" on the observation plane. Any negative
     * value carries the same meaning; this is the one to write.
     */
    public static final long UNKNOWN = -1L;

    /** Format tables have no buckets, so their bucket count is always unknown. */
    public static final int UNKNOWN_TOTAL_BUCKETS = -1;

    public static final String FIELD_SPEC = "spec";
    public static final String FIELD_RECORD_COUNT = "recordCount";
    public static final String FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
    public static final String FIELD_FILE_COUNT = "fileCount";
    public static final String FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";
    public static final String FIELD_TOTAL_BUCKETS = "totalBuckets";

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

    // defaults to 0 if this field is absent in the serialized data (e.g., from an older Paimon
    // version)
    @JsonProperty(FIELD_TOTAL_BUCKETS)
    protected final int totalBuckets;

    @JsonCreator
    public PartitionStatistics(
            @JsonProperty(FIELD_SPEC) Map<String, String> spec,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime,
            @JsonProperty(FIELD_TOTAL_BUCKETS) int totalBuckets) {
        this.spec = spec;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
        this.totalBuckets = totalBuckets;
    }

    /** Statistics of a partition nobody ever reported on: every field {@link #UNKNOWN}. */
    public static PartitionStatistics unknown(Map<String, String> spec) {
        return new PartitionStatistics(
                spec, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN_TOTAL_BUCKETS);
    }

    /**
     * Whether an observation-plane field carries a real measurement. Never apply this to a
     * delta-plane value, where a negative number is a decrement rather than a missing measurement.
     */
    public static boolean isKnown(long value) {
        return value >= 0;
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

    @JsonGetter(FIELD_TOTAL_BUCKETS)
    public int totalBuckets() {
        return totalBuckets;
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
                && totalBuckets == that.totalBuckets
                && Objects.equals(spec, that.spec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                spec, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime, totalBuckets);
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
                + ", totalBuckets="
                + totalBuckets
                + '}';
    }
}
