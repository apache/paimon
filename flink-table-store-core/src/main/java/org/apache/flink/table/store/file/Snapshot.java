/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** This file is the entrance to all data committed at some specific time point. */
public class Snapshot {

    public static final long FIRST_SNAPSHOT_ID = 1;

    private static final String FIELD_ID = "id";
    private static final String FIELD_SCHEMA_ID = "schemaId";
    private static final String FIELD_BASE_MANIFEST_LIST = "baseManifestList";
    private static final String FIELD_DELTA_MANIFEST_LIST = "deltaManifestList";
    private static final String FIELD_COMMIT_USER = "commitUser";
    private static final String FIELD_COMMIT_IDENTIFIER = "commitIdentifier";
    private static final String FIELD_COMMIT_KIND = "commitKind";
    private static final String FIELD_TIME_MILLIS = "timeMillis";
    private static final String FIELD_LOG_OFFSETS = "logOffsets";

    @JsonProperty(FIELD_ID)
    private final long id;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final long schemaId;

    // a manifest list recording all changes from the previous snapshots
    @JsonProperty(FIELD_BASE_MANIFEST_LIST)
    private final String baseManifestList;

    // a manifest list recording all new changes occurred in this snapshot
    // for faster expire and streaming reads
    @JsonProperty(FIELD_DELTA_MANIFEST_LIST)
    private final String deltaManifestList;

    @JsonProperty(FIELD_COMMIT_USER)
    private final String commitUser;

    // for deduplication
    @JsonProperty(FIELD_COMMIT_IDENTIFIER)
    private final String commitIdentifier;

    @JsonProperty(FIELD_COMMIT_KIND)
    private final CommitKind commitKind;

    @JsonProperty(FIELD_TIME_MILLIS)
    private final long timeMillis;

    @JsonProperty(FIELD_LOG_OFFSETS)
    private final Map<Integer, Long> logOffsets;

    @JsonCreator
    public Snapshot(
            @JsonProperty(FIELD_ID) long id,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST) String baseManifestList,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST) String deltaManifestList,
            @JsonProperty(FIELD_COMMIT_USER) String commitUser,
            @JsonProperty(FIELD_COMMIT_IDENTIFIER) String commitIdentifier,
            @JsonProperty(FIELD_COMMIT_KIND) CommitKind commitKind,
            @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
            @JsonProperty(FIELD_LOG_OFFSETS) Map<Integer, Long> logOffsets) {
        this.id = id;
        this.schemaId = schemaId;
        this.baseManifestList = baseManifestList;
        this.deltaManifestList = deltaManifestList;
        this.commitUser = commitUser;
        this.commitIdentifier = commitIdentifier;
        this.commitKind = commitKind;
        this.timeMillis = timeMillis;
        this.logOffsets = logOffsets;
    }

    @JsonGetter(FIELD_ID)
    public long id() {
        return id;
    }

    @JsonGetter(FIELD_SCHEMA_ID)
    public long schemaId() {
        return schemaId;
    }

    @JsonGetter(FIELD_BASE_MANIFEST_LIST)
    public String baseManifestList() {
        return baseManifestList;
    }

    @JsonGetter(FIELD_DELTA_MANIFEST_LIST)
    public String deltaManifestList() {
        return deltaManifestList;
    }

    @JsonGetter(FIELD_COMMIT_USER)
    public String commitUser() {
        return commitUser;
    }

    @JsonGetter(FIELD_COMMIT_IDENTIFIER)
    public String commitIdentifier() {
        return commitIdentifier;
    }

    @JsonGetter(FIELD_COMMIT_KIND)
    public CommitKind commitKind() {
        return commitKind;
    }

    @JsonGetter(FIELD_TIME_MILLIS)
    public long timeMillis() {
        return timeMillis;
    }

    @JsonGetter(FIELD_LOG_OFFSETS)
    public Map<Integer, Long> getLogOffsets() {
        return logOffsets;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public List<ManifestFileMeta> readAllManifests(ManifestList manifestList) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(manifestList.read(baseManifestList));
        result.addAll(manifestList.read(deltaManifestList));
        return result;
    }

    public static Snapshot fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Snapshot.class);
    }

    public static Snapshot fromPath(Path path) {
        try {
            String json = FileUtils.readFileUtf8(path);
            return Snapshot.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Fails to read snapshot from path " + path, e);
        }
    }

    /** Type of changes in this snapshot. */
    public enum CommitKind {

        /** Changes flushed from the mem table. */
        APPEND,

        /** Changes by compacting existing data files. */
        COMPACT,

        /** Changes that clear up the whole partition and then add new records. */
        OVERWRITE
    }
}
