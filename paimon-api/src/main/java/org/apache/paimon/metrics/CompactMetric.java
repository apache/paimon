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

package org.apache.paimon.metrics;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** @since 1.4.0 */
@Public
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompactMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final long FIRST_SNAPSHOT_ID = 1;

    public static final int TABLE_STORE_02_VERSION = 1;
    protected static final int CURRENT_VERSION = 3;

    protected static final String FIELD_VERSION = "version";
    protected static final String FIELD_SNAPSHOT_ID = "snapshotId";
    protected static final String FIELD_COMMIT_TIME = "commitTime";
    protected static final String FIELD_COMPACT_AVG_DURATION = "compactAvgDuration";
    protected static final String FIELD_COMPACTION_MAX_DURATION = "compactMaxDuration";
    protected static final String FIELD_COMPACTION_MIN_DURATION = "compactMinDuration";
    protected static final String FIELD_BUCKETS = "compactBuckets";
    protected static final String FIELD_COMPACTION_TYPE = "compactType";
    protected static final String FIELD_IDENTIFIER = "identifier";
    protected static final String FIELD_COMMIT_USER = "commitUser";

    @JsonProperty(FIELD_VERSION)
    @Nullable
    protected final Integer version;

    @JsonProperty(FIELD_SNAPSHOT_ID)
    protected final long snapshotId;

    @JsonProperty(FIELD_COMMIT_TIME)
    protected final long commitTime;

    @JsonProperty(FIELD_COMPACT_AVG_DURATION)
    protected final long compactDuration;

    // a manifest list recording all new changes occurred in this snapshot
    // for faster expire and streaming reads
    @JsonProperty(FIELD_COMPACTION_MAX_DURATION)
    protected final long compactMaxDuration;

    @JsonProperty(FIELD_COMPACTION_MIN_DURATION)
    protected final long compactMinDuration;

    // a manifest list recording all changelog produced in this snapshot
    // null if no changelog is produced, or for paimon <= 0.2
    @JsonProperty(FIELD_BUCKETS)
    protected final String buckets;

    @JsonProperty(FIELD_IDENTIFIER)
    protected final long identifier;

    @JsonProperty(FIELD_COMMIT_USER)
    protected final String commitUser;

    @JsonProperty(FIELD_COMPACTION_TYPE)
    protected final String compactType;

    public CompactMetric(
            long snapshotId,
            long commitTime,
            long compactDuration,
            long compactMaxDuration,
            long compactMinDuration,
            String buckets,
            String compactType,
            long identifier,
            String commitUser) {
        this(
                CURRENT_VERSION,
                snapshotId,
                commitTime,
                compactDuration,
                compactMaxDuration,
                compactMinDuration,
                buckets,
                compactType,
                identifier,
                commitUser);
    }

    @JsonCreator
    public CompactMetric(
            @JsonProperty(FIELD_VERSION) @Nullable Integer version,
            @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId,
            @JsonProperty(FIELD_COMMIT_TIME) long commitTime,
            @JsonProperty(FIELD_COMPACT_AVG_DURATION) long compactDuration,
            @JsonProperty(FIELD_COMPACTION_MAX_DURATION) long compactMaxDuration,
            @JsonProperty(FIELD_COMPACTION_MIN_DURATION) long compactMinDuration,
            @JsonProperty(FIELD_BUCKETS) @Nullable String buckets,
            @JsonProperty(FIELD_COMPACTION_TYPE) @Nullable String compactType,
            @JsonProperty(FIELD_IDENTIFIER) long identifier,
            @JsonProperty(FIELD_COMMIT_USER) String commitUser) {
        this.version = version;
        this.snapshotId = snapshotId;
        this.commitTime = commitTime;
        this.compactDuration = compactDuration;
        this.compactMaxDuration = compactMaxDuration;
        this.compactMinDuration = compactMinDuration;
        this.buckets = buckets;
        this.compactType = compactType;
        this.identifier = identifier;
        this.commitUser = commitUser;
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        // there is no version field for paimon <= 0.2
        return version == null ? TABLE_STORE_02_VERSION : version;
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    public long snapshotId() {
        return snapshotId;
    }

    @JsonGetter(FIELD_COMMIT_TIME)
    public long commitTime() {
        return commitTime;
    }

    @JsonGetter(FIELD_COMPACT_AVG_DURATION)
    public long compactDuration() {
        return compactDuration;
    }

    @JsonGetter(FIELD_COMPACTION_MAX_DURATION)
    public long compactMaxDuration() {
        return compactMaxDuration;
    }

    @JsonGetter(FIELD_COMPACTION_MIN_DURATION)
    public long compactMinDuration() {
        return compactMinDuration;
    }

    @JsonGetter(FIELD_BUCKETS)
    public String buckets() {
        return buckets == null ? "{}" : buckets;
    }

    @JsonGetter(FIELD_COMPACTION_TYPE)
    public String compactType() {
        return compactType;
    }

    @JsonGetter(FIELD_IDENTIFIER)
    public long identifier() {
        return identifier;
    }

    @JsonGetter(FIELD_COMMIT_USER)
    public String commitUser() {
        return commitUser;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version,
                snapshotId,
                commitTime,
                compactDuration,
                compactMaxDuration,
                compactMinDuration,
                buckets,
                compactType,
                identifier,
                commitUser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactMetric that = (CompactMetric) o;
        return Objects.equals(version, that.version)
                && snapshotId == that.snapshotId
                && commitTime == that.commitTime
                && Objects.equals(compactDuration, that.compactDuration)
                && Objects.equals(compactMaxDuration, that.compactMaxDuration)
                && Objects.equals(compactMinDuration, that.compactMinDuration)
                && Objects.equals(buckets, that.buckets)
                && Objects.equals(compactType, that.compactType)
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(commitUser, that.commitUser);
    }

    public static CompactMetric fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, CompactMetric.class);
    }
}
