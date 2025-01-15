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

package org.apache.paimon.view;

import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/** Schema for view. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewSchema {
    private static final String FIELD_FIELDS = "rowType";
    private static final String FIELD_OPTIONS = "options";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_QUERY = "query";

    @JsonProperty(FIELD_QUERY)
    private final String query;

    @Nullable
    @JsonProperty(FIELD_COMMENT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String comment;

    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    @JsonProperty(FIELD_FIELDS)
    private final RowType rowType;

    @JsonCreator
    public ViewSchema(
            @JsonProperty(FIELD_FIELDS) RowType rowType,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_QUERY) String query) {
        this.options = options;
        this.comment = comment;
        this.query = query;
        this.rowType = rowType;
    }

    public ViewSchema(
            String query, @Nullable String comment, Map<String, String> options, RowType rowType) {
        this.query = query;
        this.comment = comment;
        this.options = options;
        this.rowType = rowType;
    }

    @JsonGetter(FIELD_FIELDS)
    public RowType rowType() {
        return rowType;
    }

    @JsonGetter(FIELD_QUERY)
    public String query() {
        return query;
    }

    @Nullable
    @JsonGetter(FIELD_COMMENT)
    public String comment() {
        return comment;
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ViewSchema that = (ViewSchema) o;
        return Objects.equals(query, that.query)
                && Objects.equals(comment, that.comment)
                && Objects.equals(options, that.options)
                && Objects.equals(rowType, that.rowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, comment, options, rowType);
    }
}
