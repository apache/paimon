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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Snapshot in Iceberg's metadata.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#snapshots">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergSnapshot {

    private static final String FIELD_SEQUENCE_NUMBER = "sequence-number";
    private static final String FIELD_SNAPSHOT_ID = "snapshot-id";
    private static final String FIELD_PARENT_SNAPSHOT_ID = "parent-snapshot-id";
    private static final String FIELD_TIMESTAMP_MS = "timestamp-ms";
    private static final String FIELD_SUMMARY = "summary";
    private static final String FIELD_MANIFEST_LIST = "manifest-list";
    private static final String FIELD_SCHEMA_ID = "schema-id";
    private static final String FIELD_FIRST_ROW_ID = "first-row-id";
    private static final String FIELD_ADDED_ROWS = "added-rows";

    @JsonProperty(FIELD_SEQUENCE_NUMBER)
    private final long sequenceNumber;

    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final long snapshotId;

    @JsonProperty(FIELD_PARENT_SNAPSHOT_ID)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long parentSnapshotId;

    @JsonProperty(FIELD_TIMESTAMP_MS)
    private final long timestampMs;

    @JsonProperty(FIELD_SUMMARY)
    private final IcebergSnapshotSummary summary;

    @JsonProperty(FIELD_MANIFEST_LIST)
    private final String manifestList;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final int schemaId;

    @JsonProperty(FIELD_FIRST_ROW_ID)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long firstRowId;

    @JsonProperty(FIELD_ADDED_ROWS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long addedRows;

    @JsonCreator
    public IcebergSnapshot(
            @JsonProperty(FIELD_SEQUENCE_NUMBER) long sequenceNumber,
            @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId,
            @JsonProperty(FIELD_PARENT_SNAPSHOT_ID) Long parentSnapshotId,
            @JsonProperty(FIELD_TIMESTAMP_MS) long timestampMs,
            @JsonProperty(FIELD_SUMMARY) IcebergSnapshotSummary summary,
            @JsonProperty(FIELD_MANIFEST_LIST) String manifestList,
            @JsonProperty(FIELD_SCHEMA_ID) int schemaId,
            @JsonProperty(FIELD_FIRST_ROW_ID) Long firstRowId,
            @JsonProperty(FIELD_ADDED_ROWS) Long addedRows) {
        this.sequenceNumber = sequenceNumber;
        this.snapshotId = snapshotId;
        this.parentSnapshotId = parentSnapshotId;
        this.timestampMs = timestampMs;
        this.summary = summary;
        this.manifestList = manifestList;
        this.schemaId = schemaId;
        this.firstRowId = firstRowId;
        this.addedRows = addedRows;
    }

    @JsonGetter(FIELD_SEQUENCE_NUMBER)
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    public long snapshotId() {
        return snapshotId;
    }

    @JsonGetter(FIELD_TIMESTAMP_MS)
    public long timestampMs() {
        return timestampMs;
    }

    @JsonGetter(FIELD_SUMMARY)
    public IcebergSnapshotSummary summary() {
        return summary;
    }

    @JsonGetter(FIELD_MANIFEST_LIST)
    public String manifestList() {
        return manifestList;
    }

    @JsonGetter(FIELD_SCHEMA_ID)
    public int schemaId() {
        return schemaId;
    }

    @JsonGetter(FIELD_PARENT_SNAPSHOT_ID)
    public Long parentSnapshotId() {
        return parentSnapshotId;
    }

    @JsonGetter(FIELD_ADDED_ROWS)
    public Long addedRows() {
        return addedRows;
    }

    @JsonGetter(FIELD_FIRST_ROW_ID)
    public Long firstRowId() {
        return firstRowId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sequenceNumber,
                snapshotId,
                parentSnapshotId,
                timestampMs,
                summary,
                manifestList,
                schemaId,
                addedRows,
                firstRowId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSnapshot)) {
            return false;
        }

        IcebergSnapshot that = (IcebergSnapshot) o;
        return sequenceNumber == that.sequenceNumber
                && snapshotId == that.snapshotId
                && Objects.equals(parentSnapshotId, that.parentSnapshotId)
                && timestampMs == that.timestampMs
                && Objects.equals(summary, that.summary)
                && Objects.equals(manifestList, that.manifestList)
                && schemaId == that.schemaId
                && Objects.equals(addedRows, that.addedRows)
                && Objects.equals(firstRowId, that.firstRowId);
    }
}
