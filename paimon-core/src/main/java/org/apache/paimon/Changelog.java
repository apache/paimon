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
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * The metadata of changelog. It generates from the snapshot file during expiration. So that the
 * changelog of the table can outlive the snapshot's lifecycle. A table's changelog can come from
 * one source:
 * <li>The changelog file. Eg: from the changelog-producer = 'input'
 */
public class Changelog extends Snapshot {

    public Changelog(Snapshot snapshot) {
        this(
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
                snapshot.statistics());
    }

    @JsonCreator
    public Changelog(
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
            @JsonProperty(FIELD_STATISTICS) @Nullable String statistics) {
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
