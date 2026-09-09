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

package org.apache.paimon.schema;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for schema version compatibility between old and new Paimon versions. */
public class SchemaVersionCompatibilityTest {

    // ==================== Reading old-format schemas ====================

    @Test
    public void testReadSchemaWithoutComment() {
        // Old schema format without "comment" field
        String oldSchemaJson =
                "{\n"
                        + "  \"fields\" : [{\n"
                        + "    \"id\" : 0,\n"
                        + "    \"name\" : \"f0\",\n"
                        + "    \"type\" : \"INT\"\n"
                        + "  }, {\n"
                        + "    \"id\" : 1,\n"
                        + "    \"name\" : \"f1\",\n"
                        + "    \"type\" : \"STRING\"\n"
                        + "  }],\n"
                        + "  \"partitionKeys\" : [],\n"
                        + "  \"primaryKeys\" : [\"f0\"],\n"
                        + "  \"options\" : {\"bucket\" : \"4\"}\n"
                        + "}";

        Schema schema = JsonSerdeUtil.fromJson(oldSchemaJson, Schema.class);

        assertThat(schema.comment()).isNull();
        assertThat(schema.primaryKeys()).containsExactly("f0");
        assertThat(schema.options()).containsEntry("bucket", "4");
        assertThat(schema.fields()).hasSize(2);
        assertThat(schema.rowType().getFieldCount()).isEqualTo(2);
    }

    @Test
    public void testReadSchemaWithComment() {
        // New schema format with "comment" field
        String newSchemaJson =
                "{\n"
                        + "  \"fields\" : [{\n"
                        + "    \"id\" : 0,\n"
                        + "    \"name\" : \"id\",\n"
                        + "    \"type\" : \"BIGINT\"\n"
                        + "  }],\n"
                        + "  \"partitionKeys\" : [],\n"
                        + "  \"primaryKeys\" : [\"id\"],\n"
                        + "  \"options\" : {},\n"
                        + "  \"comment\" : \"test table comment\"\n"
                        + "}";

        Schema schema = JsonSerdeUtil.fromJson(newSchemaJson, Schema.class);

        assertThat(schema.comment()).isEqualTo("test table comment");
        assertThat(schema.primaryKeys()).containsExactly("id");
        assertThat(schema.fields()).hasSize(1);
    }

    @Test
    public void testReadSchemaWithUnknownFields() {
        // New Paimon reads old schema with unknown future fields - should be ignored
        String schemaJson =
                "{\n"
                        + "  \"fields\" : [{\n"
                        + "    \"id\" : 0,\n"
                        + "    \"name\" : \"col1\",\n"
                        + "    \"type\" : \"INT\"\n"
                        + "  }],\n"
                        + "  \"partitionKeys\" : [],\n"
                        + "  \"primaryKeys\" : [],\n"
                        + "  \"options\" : {},\n"
                        + "  \"futureSchemaField\" : \"someValue\",\n"
                        + "  \"anotherFutureField\" : 42\n"
                        + "}";

        Schema schema = JsonSerdeUtil.fromJson(schemaJson, Schema.class);

        assertThat(schema.fields()).hasSize(1);
        assertThat(schema.fieldNames()).containsExactly("col1");
        // Unknown fields should be silently ignored
    }

    @Test
    public void testSchemaWithPartitionKeys() {
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "dt", DataTypes.STRING()),
                                new DataField(1, "val", DataTypes.INT())),
                        Collections.singletonList("dt"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "partitioned table");

        String json = JsonSerdeUtil.toJson(schema);
        Schema parsed = JsonSerdeUtil.fromJson(json, Schema.class);

        assertThat(parsed.partitionKeys()).containsExactly("dt");
        assertThat(parsed.comment()).isEqualTo("partitioned table");
        assertThat(parsed.fields()).hasSize(2);
    }

    // ==================== Write-read round-trip tests ====================

    @Test
    public void testSchemaJsonRoundTrip() {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "10");
        options.put("file.format", "parquet");

        Schema original =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING()),
                                new DataField(2, "score", DataTypes.DOUBLE())),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        options,
                        "test table");

        String json = JsonSerdeUtil.toJson(original);
        Schema parsed = JsonSerdeUtil.fromJson(json, Schema.class);

        assertThat(parsed.fields()).hasSize(3);
        assertThat(parsed.fieldNames()).containsExactly("id", "name", "score");
        assertThat(parsed.primaryKeys()).containsExactly("id");
        assertThat(parsed.partitionKeys()).isEmpty();
        assertThat(parsed.options()).containsEntry("bucket", "10");
        assertThat(parsed.options()).containsEntry("file.format", "parquet");
        assertThat(parsed.comment()).isEqualTo("test table");
    }

    @Test
    public void testSchemaWithNullCommentJsonRoundTrip() {
        Schema schema =
                new Schema(
                        Collections.singletonList(
                                new DataField(0, "col", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null);

        String json = JsonSerdeUtil.toJson(schema);

        // Null comment should not appear in JSON
        assertThat(json).doesNotContain("\"comment\"");

        Schema parsed = JsonSerdeUtil.fromJson(json, Schema.class);
        assertThat(parsed.comment()).isNull();
        assertThat(parsed.fields()).hasSize(1);
    }

    // ==================== Forward compatibility (old reads new) ====================

    @Test
    public void testNewSchemaJsonCompatibleWithOldReader() {
        // Old reader should be able to parse new schema JSON (unknown fields ignored)
        Schema schema =
                new Schema(
                        Collections.singletonList(
                                new DataField(0, "col", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "a comment");

        String json = JsonSerdeUtil.toJson(schema);

        // The JSON should be valid and parseable
        Schema parsed = JsonSerdeUtil.fromJson(json, Schema.class);
        assertThat(parsed.comment()).isEqualTo("a comment");
        assertThat(parsed.fields()).hasSize(1);
        assertThat(parsed.fieldNames()).containsExactly("col");
    }

    @Test
    public void testSchemaEquality() {
        Schema schema1 =
                new Schema(
                        Collections.singletonList(
                                new DataField(0, "a", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "comment");

        Schema schema2 =
                new Schema(
                        Collections.singletonList(
                                new DataField(0, "a", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "comment");

        assertThat(schema1).isEqualTo(schema2);
        assertThat(schema1.hashCode()).isEqualTo(schema2.hashCode());
    }
}