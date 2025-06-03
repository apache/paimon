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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/** Snapshot of a table, including basic statistics of this table. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class TableSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_SNAPSHOT = "snapshot";
    public static final String FIELD_RECORD_COUNT = "recordCount";
    public static final String FIELD_FILE_SIZE_IN_BYTES = "fileSizeInBytes";
    public static final String FIELD_FILE_COUNT = "fileCount";
    public static final String FIELD_LAST_FILE_CREATION_TIME = "lastFileCreationTime";

    @JsonProperty(FIELD_SNAPSHOT)
    private final Snapshot snapshot;

    @JsonProperty(FIELD_RECORD_COUNT)
    private final long recordCount;

    @JsonProperty(FIELD_FILE_SIZE_IN_BYTES)
    private final long fileSizeInBytes;

    @JsonProperty(FIELD_FILE_COUNT)
    private final long fileCount;

    @JsonProperty(FIELD_LAST_FILE_CREATION_TIME)
    private final long lastFileCreationTime;

    @JsonCreator
    public TableSnapshot(
            @JsonProperty(FIELD_SNAPSHOT) Snapshot snapshot,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime) {
        this.snapshot = snapshot;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
    }

    @JsonGetter(FIELD_SNAPSHOT)
    public Snapshot snapshot() {
        return snapshot;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSnapshot that = (TableSnapshot) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && Objects.equals(snapshot, that.snapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                snapshot, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
    }

    @Override
    public String toString() {
        return "{"
                + "snapshot="
                + snapshot
                + ", recordCount="
                + recordCount
                + ", fileSizeInBytes="
                + fileSizeInBytes
                + ", fileCount="
                + fileCount
                + ", lastFileCreationTime="
                + lastFileCreationTime
                + '}';
    }
}
