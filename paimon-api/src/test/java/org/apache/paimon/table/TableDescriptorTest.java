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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableDescriptor} JSON serialization. */
public class TableDescriptorTest {

    private static TableSchema tableSchema() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", new IntType()));
        Schema schema =
                new Schema(
                        fields,
                        Collections.singletonList("f0"),
                        Arrays.asList("f0", "f1"),
                        Collections.singletonMap("bucket", "1"),
                        "comment");
        return TableSchema.create(5, schema);
    }

    @Test
    public void testJsonRoundTrip() {
        TableDescriptor descriptor =
                new TableDescriptor(
                        TableDescriptor.CURRENT_VERSION,
                        "file:///tmp/paimon/mydb.db/t",
                        tableSchema(),
                        "mydb",
                        "t",
                        null,
                        null);

        String json = JsonSerdeUtil.toJson(descriptor);
        TableDescriptor parsed = JsonSerdeUtil.fromJson(json, TableDescriptor.class);

        assertThat(parsed.version()).isEqualTo(1);
        assertThat(parsed.path()).isEqualTo("file:///tmp/paimon/mydb.db/t");
        assertThat(parsed.database()).isEqualTo("mydb");
        assertThat(parsed.name()).isEqualTo("t");
        assertThat(parsed.branch()).isNull();
        assertThat(parsed.tableSchema().id()).isEqualTo(5);
        assertThat(parsed.tableSchema().primaryKeys()).containsExactly("f0", "f1");
        assertThat(parsed.tableSchema().partitionKeys()).containsExactly("f0");
        assertThat(parsed.tableSchema().options()).containsEntry("bucket", "1");
    }

    @Test
    public void testBranchRoundTrip() {
        TableDescriptor descriptor =
                new TableDescriptor(
                        TableDescriptor.CURRENT_VERSION,
                        "file:///tmp/paimon/mydb.db/t",
                        tableSchema(),
                        "mydb",
                        "t",
                        "b1",
                        null);

        String json = JsonSerdeUtil.toJson(descriptor);
        assertThat(json).contains("\"branch\"");
        TableDescriptor parsed = JsonSerdeUtil.fromJson(json, TableDescriptor.class);
        assertThat(parsed.branch()).isEqualTo("b1");
    }

    @Test
    public void testSnapshotIdRoundTrip() {
        TableDescriptor descriptor =
                new TableDescriptor(
                        TableDescriptor.CURRENT_VERSION,
                        "file:///tmp/paimon/mydb.db/t",
                        tableSchema(),
                        "mydb",
                        "t",
                        null,
                        7L);

        String json = JsonSerdeUtil.toJson(descriptor);
        assertThat(json).contains("\"snapshotId\"");
        TableDescriptor parsed = JsonSerdeUtil.fromJson(json, TableDescriptor.class);
        assertThat(parsed.snapshotId()).isEqualTo(7L);
    }

    @Test
    public void testNestedTableSchemaUsesPaimonSerde() {
        // The nested tableSchema must round-trip through Paimon's custom serde
        // (SchemaSerializer), preserving id/highestFieldId losslessly.
        TableDescriptor descriptor =
                new TableDescriptor(
                        TableDescriptor.CURRENT_VERSION,
                        "file:///tmp/t",
                        tableSchema(),
                        null,
                        null,
                        null,
                        null);

        String json = JsonSerdeUtil.toJson(descriptor);
        assertThat(json).contains("\"tableSchema\"").contains("\"highestFieldId\"");

        TableDescriptor parsed = JsonSerdeUtil.fromJson(json, TableDescriptor.class);
        assertThat(parsed.tableSchema().highestFieldId())
                .isEqualTo(descriptor.tableSchema().highestFieldId());
        assertThat(parsed.database()).isNull();
    }

    @Test
    public void testUnknownFieldsIgnored() {
        // Forward compatibility: unknown top-level fields must not fail parsing.
        String json =
                "{\"version\":1,\"path\":\"file:///tmp/t\",\"futureField\":\"x\","
                        + "\"tableSchema\":"
                        + JsonSerdeUtil.toJson(tableSchema())
                        + "}";
        TableDescriptor parsed = JsonSerdeUtil.fromJson(json, TableDescriptor.class);
        assertThat(parsed.version()).isEqualTo(1);
        assertThat(parsed.path()).isEqualTo("file:///tmp/t");
    }
}
