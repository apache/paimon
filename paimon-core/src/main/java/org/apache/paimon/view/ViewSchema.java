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

import org.apache.paimon.types.DataField;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Schema for view. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewSchema {

    private static final String FIELD_FIELDS = "fields";
    private static final String FIELD_QUERY = "query";
    private static final String FIELD_DIALECTS = "dialects";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_FIELDS)
    private final List<DataField> fields;

    @JsonProperty(FIELD_QUERY)
    private final String query;

    @JsonProperty(FIELD_DIALECTS)
    private final Map<String, String> dialects;

    @Nullable
    @JsonProperty(FIELD_COMMENT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String comment;

    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    @JsonCreator
    public ViewSchema(
            @JsonProperty(FIELD_FIELDS) List<DataField> fields,
            @JsonProperty(FIELD_QUERY) String query,
            @JsonProperty(FIELD_DIALECTS) Map<String, String> dialects,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        this.fields = fields;
        this.query = query;
        this.dialects = dialects;
        this.options = options;
        this.comment = comment;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<DataField> fields() {
        return fields;
    }

    @JsonGetter(FIELD_QUERY)
    public String query() {
        return query;
    }

    @JsonGetter(FIELD_DIALECTS)
    public Map<String, String> dialects() {
        return dialects;
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
        return Objects.equals(fields, that.fields)
                && Objects.equals(query, that.query)
                && Objects.equals(dialects, that.dialects)
                && Objects.equals(comment, that.comment)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, query, dialects, comment, options);
    }
}
