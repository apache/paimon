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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Schema in Iceberg's metadata.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#schemas">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergSchema {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_SCHEMA_ID = "schema-id";
    private static final String FIELD_FIELDS = "fields";

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final int schemaId;

    @JsonProperty(FIELD_FIELDS)
    private final List<IcebergDataField> fields;

    public static IcebergSchema create(TableSchema tableSchema) {
        int bias;
        if (new CoreOptions(tableSchema.options()).formatType().equals("parquet")) {
            // data files start with trimmed primary keys + sequence number + value kind
            // also ParquetSchemaUtil.addFallbackIds starts enumerating id from 1 instead of 0
            bias = tableSchema.trimmedPrimaryKeys().size() + 3;
        } else {
            bias = 0;
        }

        return new IcebergSchema(
                (int) tableSchema.id(),
                tableSchema.fields().stream()
                        .map(f -> new IcebergDataField(f, bias))
                        .collect(Collectors.toList()));
    }

    public IcebergSchema(int schemaId, List<IcebergDataField> fields) {
        this("struct", schemaId, fields);
    }

    @JsonCreator
    public IcebergSchema(
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_SCHEMA_ID) int schemaId,
            @JsonProperty(FIELD_FIELDS) List<IcebergDataField> fields) {
        this.type = type;
        this.schemaId = schemaId;
        this.fields = fields;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_SCHEMA_ID)
    public int schemaId() {
        return schemaId;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<IcebergDataField> fields() {
        return fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, schemaId, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSchema)) {
            return false;
        }

        IcebergSchema that = (IcebergSchema) o;
        return Objects.equals(type, that.type)
                && schemaId == that.schemaId
                && Objects.equals(fields, that.fields);
    }
}
