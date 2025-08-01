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

package org.apache.paimon.tag;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

/** Snapshot with tagCreateTime and tagTimeRetained. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tag extends Snapshot {

    private static final String FIELD_TAG_CREATE_TIME = "tagCreateTime";
    private static final String FIELD_TAG_TIME_RETAINED = "tagTimeRetained";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_TAG_CREATE_TIME)
    @Nullable
    private final LocalDateTime tagCreateTime;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_TAG_TIME_RETAINED)
    @Nullable
    private final Duration tagTimeRetained;

    @JsonCreator
    public Tag(
            @JsonProperty(FIELD_VERSION) @Nullable Integer version,
            @JsonProperty(FIELD_ID) long id,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST) String baseManifestList,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST_SIZE) @Nullable Long baseManifestListSize,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST) String deltaManifestList,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST_SIZE) @Nullable Long deltaManifestListSize,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST) @Nullable String changelogManifestList,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST_SIZE) @Nullable
                    Long changelogManifestListSize,
            @JsonProperty(FIELD_INDEX_MANIFEST) @Nullable String indexManifest,
            @JsonProperty(FIELD_COMMIT_USER) String commitUser,
            @JsonProperty(FIELD_COMMIT_IDENTIFIER) long commitIdentifier,
            @JsonProperty(FIELD_COMMIT_KIND) CommitKind commitKind,
            @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
            @JsonProperty(FIELD_LOG_OFFSETS) Map<Integer, Long> logOffsets,
            @JsonProperty(FIELD_TOTAL_RECORD_COUNT) @Nullable Long totalRecordCount,
            @JsonProperty(FIELD_DELTA_RECORD_COUNT) @Nullable Long deltaRecordCount,
            @JsonProperty(FIELD_CHANGELOG_RECORD_COUNT) @Nullable Long changelogRecordCount,
            @JsonProperty(FIELD_WATERMARK) @Nullable Long watermark,
            @JsonProperty(FIELD_STATISTICS) @Nullable String statistics,
            @JsonProperty(FIELD_PROPERTIES) Map<String, String> properties,
            @JsonProperty(FIELD_NEXT_ROW_ID) @Nullable Long nextRowId,
            @JsonProperty(FIELD_TAG_CREATE_TIME) @Nullable LocalDateTime tagCreateTime,
            @JsonProperty(FIELD_TAG_TIME_RETAINED) @Nullable Duration tagTimeRetained) {
        super(
                version,
                id,
                schemaId,
                baseManifestList,
                baseManifestListSize,
                deltaManifestList,
                deltaManifestListSize,
                changelogManifestList,
                changelogManifestListSize,
                indexManifest,
                commitUser,
                commitIdentifier,
                commitKind,
                timeMillis,
                logOffsets,
                totalRecordCount,
                deltaRecordCount,
                changelogRecordCount,
                watermark,
                statistics,
                properties,
                nextRowId);
        this.tagCreateTime = tagCreateTime;
        this.tagTimeRetained = tagTimeRetained;
    }

    @JsonGetter(FIELD_TAG_CREATE_TIME)
    public @Nullable LocalDateTime getTagCreateTime() {
        return tagCreateTime;
    }

    @JsonGetter(FIELD_TAG_TIME_RETAINED)
    public @Nullable Duration getTagTimeRetained() {
        return tagTimeRetained;
    }

    @Override
    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Tag fromSnapshotAndTagTtl(
            Snapshot snapshot, Duration tagTimeRetained, LocalDateTime tagCreateTime) {
        return new Tag(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.baseManifestList(),
                snapshot.baseManifestListSize(),
                snapshot.deltaManifestList(),
                snapshot.deltaManifestListSize(),
                snapshot.changelogManifestList(),
                snapshot.changelogManifestListSize(),
                snapshot.indexManifest(),
                snapshot.commitUser(),
                snapshot.commitIdentifier(),
                snapshot.commitKind(),
                snapshot.timeMillis(),
                snapshot.logOffsets(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.changelogRecordCount(),
                snapshot.watermark(),
                snapshot.statistics(),
                snapshot.properties(),
                snapshot.nextRowId(),
                tagCreateTime,
                tagTimeRetained);
    }

    public Snapshot trimToSnapshot() {
        return new Snapshot(
                version,
                id,
                schemaId,
                baseManifestList,
                baseManifestListSize,
                deltaManifestList,
                deltaManifestListSize,
                changelogManifestList,
                changelogManifestListSize,
                indexManifest,
                commitUser,
                commitIdentifier,
                commitKind,
                timeMillis,
                logOffsets,
                totalRecordCount,
                deltaRecordCount,
                changelogRecordCount,
                watermark,
                statistics,
                properties,
                nextRowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tagCreateTime, tagTimeRetained);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Tag that = (Tag) o;
        return Objects.equals(tagCreateTime, that.tagCreateTime)
                && Objects.equals(tagTimeRetained, that.tagTimeRetained);
    }

    // =================== Utils for reading =========================

    public static Tag fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Tag.class);
    }

    public static Tag fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Tag tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        try {
            return fromJson(fileIO.readFileUtf8(path));
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
