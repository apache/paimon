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

package org.apache.paimon.management;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.List;

/** Included or excluded columns attached to a column permission. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnSelection {

    private static final String FIELD_COLUMN_NAMES = "columnNames";
    private static final String FIELD_EXCLUDED_COLUMN_NAMES = "excludedColumnNames";

    @Nullable
    @JsonProperty(FIELD_COLUMN_NAMES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> columnNames;

    @Nullable
    @JsonProperty(FIELD_EXCLUDED_COLUMN_NAMES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> excludedColumnNames;

    @JsonCreator
    @ConstructorProperties({FIELD_COLUMN_NAMES, FIELD_EXCLUDED_COLUMN_NAMES})
    public ColumnSelection(
            @Nullable @JsonProperty(FIELD_COLUMN_NAMES) List<String> columnNames,
            @Nullable @JsonProperty(FIELD_EXCLUDED_COLUMN_NAMES) List<String> excludedColumnNames) {
        this.columnNames = columnNames;
        this.excludedColumnNames = excludedColumnNames;
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN_NAMES)
    public List<String> getColumnNames() {
        return columnNames;
    }

    @Nullable
    @JsonGetter(FIELD_EXCLUDED_COLUMN_NAMES)
    public List<String> getExcludedColumnNames() {
        return excludedColumnNames;
    }
}
