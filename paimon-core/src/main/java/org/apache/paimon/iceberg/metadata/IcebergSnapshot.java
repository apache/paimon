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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

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
    private static final String FIELD_TIMESTAMP_MS = "timestamp-ms";
    private static final String FIELD_SUMMARY = "summary";
    private static final String FIELD_MANIFEST_LIST = "manifest-list";
    private static final String FIELD_SCHEMA_ID = "schema-id";

    @JsonProperty(FIELD_SEQUENCE_NUMBER)
    private final long sequenceNumber;

    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final long snapshotId;

    @JsonProperty(FIELD_TIMESTAMP_MS)
    private final long timestampMs;

    @JsonProperty(FIELD_SUMMARY)
    private final IcebergSnapshotSummary summary;

    @JsonProperty(FIELD_MANIFEST_LIST)
    private final String manifestList;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final int schemaId;

    @JsonCreator
    public IcebergSnapshot(
            @JsonProperty(FIELD_SEQUENCE_NUMBER) long sequenceNumber,
            @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId,
            @JsonProperty(FIELD_TIMESTAMP_MS) long timestampMs,
            @JsonProperty(FIELD_SUMMARY) IcebergSnapshotSummary summary,
            @JsonProperty(FIELD_MANIFEST_LIST) String manifestList,
            @JsonProperty(FIELD_SCHEMA_ID) int schemaId) {
        this.sequenceNumber = sequenceNumber;
        this.snapshotId = snapshotId;
        this.timestampMs = timestampMs;
        this.summary = summary;
        this.manifestList = manifestList;
        this.schemaId = schemaId;
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

    @Override
    public int hashCode() {
        return Objects.hash(
                sequenceNumber, snapshotId, timestampMs, summary, manifestList, schemaId);
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
                && timestampMs == that.timestampMs
                && Objects.equals(summary, that.summary)
                && Objects.equals(manifestList, that.manifestList)
                && schemaId == that.schemaId;
    }
}
