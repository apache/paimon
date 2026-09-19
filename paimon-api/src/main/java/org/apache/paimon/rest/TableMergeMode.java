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

package org.apache.paimon.rest;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;

/** Overrides the default merge mode for one table name within the database being merged. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMergeMode {

    private static final String FIELD_TABLE = "table";
    private static final String FIELD_MERGE_MODE = "mergeMode";

    private final String table;
    private final MergeMode mergeMode;

    @JsonCreator
    @ConstructorProperties({FIELD_TABLE, FIELD_MERGE_MODE})
    public TableMergeMode(
            @JsonProperty(FIELD_TABLE) String table,
            @JsonProperty(FIELD_MERGE_MODE) MergeMode mergeMode) {
        this.table = table;
        this.mergeMode = mergeMode;
    }

    @JsonGetter(FIELD_TABLE)
    public String getTable() {
        return table;
    }

    @JsonGetter(FIELD_MERGE_MODE)
    public MergeMode getMergeMode() {
        return mergeMode;
    }
}
