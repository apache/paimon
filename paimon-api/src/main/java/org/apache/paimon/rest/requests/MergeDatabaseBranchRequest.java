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

package org.apache.paimon.rest.requests;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.rest.DatabaseReference;
import org.apache.paimon.rest.MergeMode;
import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.rest.TableMergeMode;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Request for merging a branch or immutable tag into a database branch. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class MergeDatabaseBranchRequest implements RESTRequest {

    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_DEFAULT_MERGE_MODE = "defaultMergeMode";
    private static final String FIELD_TABLE_MERGE_MODES = "tableMergeModes";

    private final DatabaseReference source;
    @Nullable private final MergeMode defaultMergeMode;
    @Nullable private final List<TableMergeMode> tableMergeModes;

    public MergeDatabaseBranchRequest(DatabaseReference source) {
        this(source, null, null);
    }

    @JsonCreator
    @ConstructorProperties({FIELD_SOURCE, FIELD_DEFAULT_MERGE_MODE, FIELD_TABLE_MERGE_MODES})
    public MergeDatabaseBranchRequest(
            @JsonProperty(FIELD_SOURCE) DatabaseReference source,
            @Nullable @JsonProperty(FIELD_DEFAULT_MERGE_MODE) MergeMode defaultMergeMode,
            @Nullable @JsonProperty(FIELD_TABLE_MERGE_MODES) List<TableMergeMode> tableMergeModes) {
        this.source = source;
        this.defaultMergeMode = defaultMergeMode;
        this.tableMergeModes =
                tableMergeModes == null
                        ? null
                        : Collections.unmodifiableList(new ArrayList<>(tableMergeModes));
    }

    @JsonGetter(FIELD_SOURCE)
    public DatabaseReference getSource() {
        return source;
    }

    /** Null uses the server default, {@link MergeMode#NORMAL}. */
    @Nullable
    @JsonGetter(FIELD_DEFAULT_MERGE_MODE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public MergeMode getDefaultMergeMode() {
        return defaultMergeMode;
    }

    /** Per-table modes override the default; null or empty supplies no overrides. */
    @Nullable
    @JsonGetter(FIELD_TABLE_MERGE_MODES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<TableMergeMode> getTableMergeModes() {
        return tableMergeModes;
    }
}
