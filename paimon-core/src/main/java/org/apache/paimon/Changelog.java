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

package org.apache.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The metadata of changelog. It generates from the snapshot file during expiration. So that the
 * changelog of the table can outlive the snapshot's lifecycle. A table's changelog can come from
 * two source:
 * <li>The changelog file. Eg: from the changelog-producer = 'input'
 * <li>The delta files in the APPEND commits when the changelog-producer = 'none'
 */
public class Changelog {

    private static final int CURRENT_VERSION = 1;

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_ID = "id";
    private static final String FIELD_SCHEMA_ID = "schemaId";
    private static final String FIELD_DELTA_MANIFEST_LIST = "deltaManifestList";
    private static final String FIELD_CHANGELOG_MANIFEST_LIST = "changelogManifestList";
    private static final String FIELD_COMMIT_KIND = "commitKind";
    private static final String FIELD_TIME_MILLIS = "timeMillis";
    private static final String FIELD_RECORD_COUNT = "recordCount";
    private static final String FIELD_WATERMARK = "watermark";

    @JsonProperty(FIELD_VERSION)
    private final int version;

    @JsonProperty(FIELD_ID)
    private final long id;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final long schemaId;

    @JsonProperty(FIELD_DELTA_MANIFEST_LIST)
    private final String deltaManifestList;

    @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST)
    @Nullable
    private final String changelogManifestList;

    @JsonProperty(FIELD_TIME_MILLIS)
    private final long timeMillis;

    @JsonProperty(FIELD_RECORD_COUNT)
    @Nullable
    private final Long recordCount;

    @JsonProperty(FIELD_WATERMARK)
    @Nullable
    private final Long watermark;

    @JsonProperty(FIELD_COMMIT_KIND)
    private Snapshot.CommitKind commitKind;

    public Changelog(
            long id,
            long schemaId,
            @Nullable String deltaManifestList,
            @Nullable String changelogManifestList,
            Snapshot.CommitKind commitKind,
            long timeMillis,
            Long recordCount,
            @Nullable Long watermark) {
        this(
                CURRENT_VERSION,
                id,
                schemaId,
                deltaManifestList,
                changelogManifestList,
                commitKind,
                timeMillis,
                recordCount,
                watermark);
    }

    @JsonCreator
    public Changelog(
            @JsonProperty(FIELD_VERSION) @Nullable Integer version,
            @JsonProperty(FIELD_ID) long id,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST) @Nullable String deltaManifestList,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST) @Nullable String changelogManifestList,
            @JsonProperty(FIELD_COMMIT_KIND) Snapshot.CommitKind commitKind,
            @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
            @JsonProperty(FIELD_RECORD_COUNT) @Nullable Long recordCount,
            @JsonProperty(FIELD_WATERMARK) @Nullable Long watermark) {
        this.version = version;
        this.id = id;
        this.schemaId = schemaId;
        this.deltaManifestList = deltaManifestList;
        this.changelogManifestList = changelogManifestList;
        this.recordCount = recordCount;
        this.commitKind = commitKind;
        this.timeMillis = timeMillis;
        this.watermark = watermark;
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        return version;
    }

    @JsonGetter(FIELD_ID)
    public long id() {
        return id;
    }

    @JsonGetter(FIELD_CHANGELOG_MANIFEST_LIST)
    @Nullable
    public String changelogManifestList() {
        return changelogManifestList;
    }

    @JsonGetter(FIELD_DELTA_MANIFEST_LIST)
    public String deltaManifestList() {
        return deltaManifestList;
    }

    @JsonGetter(FIELD_SCHEMA_ID)
    public long schemaId() {
        return schemaId;
    }

    @JsonGetter(FIELD_COMMIT_KIND)
    public Snapshot.CommitKind commitKind() {
        return commitKind;
    }

    @JsonGetter(FIELD_TIME_MILLIS)
    public long timeMillis() {
        return timeMillis;
    }

    @JsonGetter(FIELD_RECORD_COUNT)
    public Long recordCount() {
        return recordCount;
    }

    @JsonGetter(FIELD_WATERMARK)
    @Nullable
    public Long watermark() {
        return watermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Changelog)) {
            return false;
        }
        Changelog changelog = (Changelog) o;
        return version == changelog.version
                && id == changelog.id
                && schemaId == changelog.schemaId
                && timeMillis == changelog.timeMillis
                && Objects.equals(deltaManifestList, changelog.deltaManifestList)
                && Objects.equals(changelogManifestList, changelog.changelogManifestList)
                && Objects.equals(recordCount, changelog.recordCount)
                && Objects.equals(watermark, changelog.watermark)
                && commitKind == changelog.commitKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version,
                id,
                schemaId,
                deltaManifestList,
                changelogManifestList,
                timeMillis,
                recordCount,
                watermark,
                commitKind);
    }

    /**
     * Return a {@link ManifestFileMeta} for each delta manifest in this changelog.
     *
     * @param manifestList a {@link ManifestList} instance used for reading files at snapshot.
     * @return a list of ManifestFileMeta.
     */
    public List<ManifestFileMeta> deltaManifests(ManifestList manifestList) {
        return deltaManifestList == null
                ? Collections.emptyList()
                : manifestList.read(deltaManifestList);
    }

    /**
     * Return a {@link ManifestFileMeta} for each changelog manifest in this changelog.
     *
     * @param manifestList a {@link ManifestList} instance used for reading files at snapshot.
     * @return a list of ManifestFileMeta.
     */
    public List<ManifestFileMeta> changelogManifests(ManifestList manifestList) {
        return changelogManifestList == null
                ? Collections.emptyList()
                : manifestList.read(changelogManifestList);
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Changelog fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Changelog.class);
    }

    public static Changelog fromPath(FileIO fileIO, Path path) {
        try {
            String json = fileIO.readFileUtf8(path);
            return Changelog.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Fails to read changelog from path " + path, e);
        }
    }
}
