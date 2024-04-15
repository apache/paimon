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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Snapshot with tagCreateTime and tagTimeRetained. */
public class Tag extends Snapshot {

    public static final Comparator<Tag> TAG_COMPARATOR = new TagComparator();

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
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST) String deltaManifestList,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST) @Nullable String changelogManifestList,
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
            @JsonProperty(FIELD_TAG_CREATE_TIME) @Nullable LocalDateTime tagCreateTime,
            @JsonProperty(FIELD_TAG_TIME_RETAINED) @Nullable Duration tagTimeRetained) {
        super(
                version,
                id,
                schemaId,
                baseManifestList,
                deltaManifestList,
                changelogManifestList,
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
                statistics);
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

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Tag fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Tag.class);
    }

    public static Tag fromPath(FileIO fileIO, Path path) {
        try {
            String json = fileIO.readFileUtf8(path);
            return Tag.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Fails to read tag from path " + path, e);
        }
    }

    public static Optional<Tag> safelyFromTagPath(FileIO fileIO, Path path) throws IOException {
        try {
            String json = fileIO.readFileUtf8(path);
            return Optional.of(Tag.fromJson(json));
        } catch (FileNotFoundException e) {
            return Optional.empty();
        }
    }

    public static Tag fromSnapshotAndTagTtl(
            Snapshot snapshot, Duration tagTimeRetained, LocalDateTime tagCreateTime) {
        return new Tag(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.baseManifestList(),
                snapshot.deltaManifestList(),
                snapshot.changelogManifestList(),
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
                tagCreateTime,
                tagTimeRetained);
    }

    public Snapshot toSnapshot() {
        return new Snapshot(
                version,
                id,
                schemaId,
                baseManifestList,
                deltaManifestList,
                changelogManifestList,
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
                statistics);
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

    private static class TagComparator implements Comparator<Tag> {
        @Override
        public int compare(Tag tag1, Tag tag2) {
            int comparisonResult = 0;

            // Compare id
            comparisonResult = Long.compare(tag1.id, tag2.id);
            if (comparisonResult != 0) {
                return comparisonResult;
            }

            // Compare tagCreateTime
            if (tag1.tagCreateTime != null && tag2.tagCreateTime != null) {
                comparisonResult = tag1.tagCreateTime.compareTo(tag2.tagCreateTime);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare tagTimeRetained
            if (tag1.tagTimeRetained != null && tag2.tagTimeRetained != null) {
                comparisonResult = tag1.tagTimeRetained.compareTo(tag2.tagTimeRetained);
            }

            // Compare version
            if (tag1.version != null && tag2.version != null) {
                comparisonResult = Integer.compare(tag1.version, tag2.version);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare schemaId
            comparisonResult = Long.compare(tag1.schemaId, tag2.schemaId);
            if (comparisonResult != 0) {
                return comparisonResult;
            }

            // Compare baseManifestList
            if (tag1.baseManifestList != null && tag2.baseManifestList != null) {
                comparisonResult = tag1.baseManifestList.compareTo(tag2.baseManifestList);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare deltaManifestList
            if (tag1.deltaManifestList != null && tag2.deltaManifestList != null) {
                comparisonResult = tag1.deltaManifestList.compareTo(tag2.deltaManifestList);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare changelogManifestList
            if (tag1.changelogManifestList != null && tag2.changelogManifestList != null) {
                comparisonResult = tag1.changelogManifestList.compareTo(tag2.changelogManifestList);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare indexManifest
            if (tag1.indexManifest != null && tag2.indexManifest != null) {
                comparisonResult = tag1.indexManifest.compareTo(tag2.indexManifest);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare commitUser
            if (tag1.commitUser != null && tag2.commitUser != null) {
                comparisonResult = tag1.commitUser.compareTo(tag2.commitUser);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare commitIdentifier
            comparisonResult = Long.compare(tag1.commitIdentifier, tag2.commitIdentifier);
            if (comparisonResult != 0) {
                return comparisonResult;
            }

            // Compare commitKind
            if (tag1.commitKind != null && tag2.commitKind != null) {
                comparisonResult = tag1.commitKind.compareTo(tag2.commitKind);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare timeMillis
            comparisonResult = Long.compare(tag1.timeMillis, tag2.timeMillis);
            if (comparisonResult != 0) {
                return comparisonResult;
            }

            // Compare logOffsets
            if (tag1.logOffsets != null && tag2.logOffsets != null) {
                comparisonResult = Integer.compare(tag1.logOffsets.size(), tag2.logOffsets.size());
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare totalRecordCount
            if (tag1.totalRecordCount != null && tag2.totalRecordCount != null) {
                comparisonResult = Long.compare(tag1.totalRecordCount, tag2.totalRecordCount);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare deltaRecordCount
            if (tag1.deltaRecordCount != null && tag2.deltaRecordCount != null) {
                comparisonResult = Long.compare(tag1.deltaRecordCount, tag2.deltaRecordCount);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare changelogRecordCount
            if (tag1.changelogRecordCount != null && tag2.changelogRecordCount != null) {
                comparisonResult =
                        Long.compare(tag1.changelogRecordCount, tag2.changelogRecordCount);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare watermark
            if (tag1.watermark != null && tag2.watermark != null) {
                comparisonResult = Long.compare(tag1.watermark, tag2.watermark);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            // Compare statistics
            if (tag1.statistics != null && tag2.statistics != null) {
                comparisonResult = tag1.statistics.compareTo(tag2.statistics);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            return comparisonResult;
        }
    }
}
