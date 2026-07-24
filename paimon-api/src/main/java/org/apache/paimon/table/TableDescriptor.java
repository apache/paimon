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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.schema.TableSchema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * A portable, versioned description of a Paimon table, sufficient for a non-Java runtime (e.g. a
 * paimon-rust / paimon-cpp reader embedded in a query engine) to rebuild a table for scan/read
 * <b>without</b> a catalog lookup.
 *
 * <p>It carries only what a reader needs: the table {@link #path}, the full {@link TableSchema}
 * (identical to an on-disk {@code schema/schema-N} file), and a few optional hints. It deliberately
 * excludes JVM-only state (e.g. {@code FileIO}, {@code CatalogEnvironment}) so the JSON wire form
 * is language-neutral. Serialize/deserialize it through {@code JsonSerdeUtil} (which registers the
 * custom {@code TableSchema} serde) — not a bare Jackson {@code ObjectMapper}.
 *
 * <p>The descriptor's own {@link #version} is distinct from the nested {@link TableSchema}'s
 * schema-format version.
 *
 * <p>This is a passive data holder: it performs no validation on construction or parse. Validation
 * lives at the producer ({@code TableDescriptorSerializer}, which rejects unsupported table shapes)
 * and at the reader (which fails fast on an unknown version).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Experimental
public class TableDescriptor {

    /**
     * Current descriptor contract version. A reader that does not recognize the version must fail
     * fast rather than guess.
     */
    public static final int CURRENT_VERSION = 1;

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_PATH = "path";
    private static final String FIELD_TABLE_SCHEMA = "tableSchema";
    private static final String FIELD_DATABASE = "database";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";

    @JsonProperty(FIELD_VERSION)
    private final int version;

    @JsonProperty(FIELD_PATH)
    private final String path;

    @JsonProperty(FIELD_TABLE_SCHEMA)
    private final TableSchema tableSchema;

    @JsonProperty(FIELD_DATABASE)
    @Nullable
    private final String database;

    @JsonProperty(FIELD_NAME)
    @Nullable
    private final String name;

    /** The table branch. Omitted (null) for the main branch. */
    @JsonProperty(FIELD_BRANCH)
    @Nullable
    private final String branch;

    /**
     * The snapshot resolved by the planner when time travel was requested ({@code scan.snapshot-id}
     * / {@code scan.tag-name} / {@code scan.timestamp-millis} / {@code scan.version} / ...).
     * Omitted (null) for an ordinary scan, in which case a reader that plans from this descriptor
     * reads the latest snapshot. Carrying the resolved snapshot id (rather than the raw selectors)
     * keeps the contract engine- and version-neutral: the reader just pins this snapshot.
     */
    @JsonProperty(FIELD_SNAPSHOT_ID)
    @Nullable
    private final Long snapshotId;

    @JsonCreator
    public TableDescriptor(
            @JsonProperty(FIELD_VERSION) int version,
            @JsonProperty(FIELD_PATH) String path,
            @JsonProperty(FIELD_TABLE_SCHEMA) TableSchema tableSchema,
            @JsonProperty(FIELD_DATABASE) @Nullable String database,
            @JsonProperty(FIELD_NAME) @Nullable String name,
            @JsonProperty(FIELD_BRANCH) @Nullable String branch,
            @JsonProperty(FIELD_SNAPSHOT_ID) @Nullable Long snapshotId) {
        this.version = version;
        this.path = path;
        this.tableSchema = tableSchema;
        this.database = database;
        this.name = name;
        this.branch = branch;
        this.snapshotId = snapshotId;
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        return version;
    }

    @JsonGetter(FIELD_PATH)
    public String path() {
        return path;
    }

    @JsonGetter(FIELD_TABLE_SCHEMA)
    public TableSchema tableSchema() {
        return tableSchema;
    }

    @JsonGetter(FIELD_DATABASE)
    @Nullable
    public String database() {
        return database;
    }

    @JsonGetter(FIELD_NAME)
    @Nullable
    public String name() {
        return name;
    }

    @JsonGetter(FIELD_BRANCH)
    @Nullable
    public String branch() {
        return branch;
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    @Nullable
    public Long snapshotId() {
        return snapshotId;
    }
}
