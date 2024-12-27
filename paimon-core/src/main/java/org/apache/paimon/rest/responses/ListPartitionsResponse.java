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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Response for listing partitions. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListPartitionsResponse implements RESTResponse {
    public static final String FIELD_PARTITIONS = "partitions";

    @JsonProperty(FIELD_PARTITIONS)
    private final List<Partition> partitions;

    @JsonCreator
    public ListPartitionsResponse(@JsonProperty(FIELD_PARTITIONS) List<Partition> partitions) {
        this.partitions = partitions;
    }

    @JsonGetter(FIELD_PARTITIONS)
    public List<Partition> getPartitions() {
        return partitions;
    }

    /** Partition for rest api. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Partition implements RESTResponse {

        private static final String FIELD_SPEC = "spec";
        public static final String FIELD_PARTITION_TYPE = "partitionType";
        public static final String FIELD_RECORD_COUNT = "recordCount";
        public static final String FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
        public static final String FIELD_FILE_COUNT = "fileCount";
        public static final String FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";

        @JsonProperty(FIELD_SPEC)
        private final Map<String, String> spec;

        @JsonProperty(FIELD_PARTITION_TYPE)
        private final RowType partitionType;

        @JsonProperty(FIELD_RECORD_COUNT)
        private final long recordCount;

        @JsonProperty(FIELD_FILE_SIZE_IN_BYTES)
        private final long fileSizeInBytes;

        @JsonProperty(FIELD_FILE_COUNT)
        private final long fileCount;

        @JsonProperty(FIELD_LAST_FILE_CREATION_TIME)
        private final long lastFileCreationTime;

        @JsonCreator
        public Partition(
                @JsonProperty(FIELD_SPEC) Map<String, String> spec,
                @JsonProperty(FIELD_PARTITION_TYPE) RowType partitionType,
                @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
                @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
                @JsonProperty(FIELD_FILE_COUNT) long fileCount,
                @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime) {
            this.spec = spec;
            this.partitionType = partitionType;
            this.recordCount = recordCount;
            this.fileSizeInBytes = fileSizeInBytes;
            this.fileCount = fileCount;
            this.lastFileCreationTime = lastFileCreationTime;
        }

        @JsonGetter(FIELD_SPEC)
        public Map<String, String> getSpec() {
            return spec;
        }

        @JsonGetter(FIELD_PARTITION_TYPE)
        public RowType getPartitionType() {
            return partitionType;
        }

        @JsonGetter(FIELD_RECORD_COUNT)
        public long getRecordCount() {
            return recordCount;
        }

        @JsonGetter(FIELD_FILE_SIZE_IN_BYTES)
        public long getFileSizeInBytes() {
            return fileSizeInBytes;
        }

        @JsonGetter(FIELD_FILE_COUNT)
        public long getFileCount() {
            return fileCount;
        }

        @JsonGetter(FIELD_LAST_FILE_CREATION_TIME)
        public long getLastFileCreationTime() {
            return lastFileCreationTime;
        }
    }
}
