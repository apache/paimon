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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Wrap the {@link Schema} class to support RESTCatalog. Define a class as: 1. This class in rest
 * catalog is easy to maintain. 2. It's easy to manage rest API fields.
 */
public class TableSchema {
    private static final String FIELD_FILED_NAME = "fields";
    private static final String FIELD_PARTITION_KEYS_NAME = "partitionKeys";
    private static final String FIELD_PRIMARY_KEYS_NAME = "primaryKeys";
    private static final String FIELD_OPTIONS_NAME = "options";
    private static final String FIELD_COMMENT_NAME = "comment";

    @JsonProperty(FIELD_FILED_NAME)
    private final List<DataField> fields;

    @JsonProperty(FIELD_PARTITION_KEYS_NAME)
    private final List<String> partitionKeys;

    @JsonProperty(FIELD_PRIMARY_KEYS_NAME)
    private final List<String> primaryKeys;

    @JsonProperty(FIELD_OPTIONS_NAME)
    private final Map<String, String> options;

    @JsonProperty(FIELD_COMMENT_NAME)
    private final String comment;

    @JsonCreator
    public TableSchema(
            @JsonProperty(FIELD_FILED_NAME) List<DataField> fields,
            @JsonProperty(FIELD_PARTITION_KEYS_NAME) List<String> partitionKeys,
            @JsonProperty(FIELD_PRIMARY_KEYS_NAME) List<String> primaryKeys,
            @JsonProperty(FIELD_OPTIONS_NAME) Map<String, String> options,
            @JsonProperty(FIELD_COMMENT_NAME) String comment) {
        this.fields = fields;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = options;
        this.comment = comment;
    }

    public TableSchema(Schema schema) {
        this.fields = schema.fields();
        this.partitionKeys = schema.partitionKeys();
        this.primaryKeys = schema.primaryKeys();
        this.options = schema.options();
        this.comment = schema.comment();
    }

    @JsonGetter(FIELD_FILED_NAME)
    public List<DataField> getFields() {
        return fields;
    }

    @JsonGetter(FIELD_PARTITION_KEYS_NAME)
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @JsonGetter(FIELD_PRIMARY_KEYS_NAME)
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @JsonGetter(FIELD_OPTIONS_NAME)
    public Map<String, String> getOptions() {
        return options;
    }

    @JsonGetter(FIELD_COMMENT_NAME)
    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            TableSchema that = (TableSchema) o;
            return Objects.equals(fields, that.fields)
                    && Objects.equals(partitionKeys, that.partitionKeys)
                    && Objects.equals(primaryKeys, that.primaryKeys)
                    && Objects.equals(options, that.options)
                    && Objects.equals(comment, that.comment);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, partitionKeys, primaryKeys, options, comment);
    }
}
