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

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the canonical cross-language {@link TableDescriptor} JSON. The checked-in golden {@code
 * table/table-descriptor-v1.json} is the exact wire form the paimon-rust reader is tested against,
 * so a drift on either side is caught here and in the Rust golden test.
 */
public class TableDescriptorGoldenTest {

    private static final String GOLDEN = "/table/table-descriptor-v1.json";

    /** A fully deterministic descriptor (fixed schema id / highestFieldId / timeMillis). */
    static TableDescriptor canonicalDescriptor() {
        TableSchema tableSchema =
                new TableSchema(
                        3,
                        1L,
                        Arrays.asList(
                                new DataField(0, "id", new IntType(false)),
                                new DataField(1, "name", new VarCharType(true, 100)),
                                new DataField(2, "tags", new ArrayType(true, new IntType(true)))),
                        2,
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        Collections.singletonMap("bucket", "1"),
                        "canonical",
                        1700000000000L);
        return new TableDescriptor(
                TableDescriptor.CURRENT_VERSION,
                "file:///tmp/warehouse/mydb.db/t",
                tableSchema,
                "mydb",
                "t",
                null,
                null);
    }

    private static String readGolden() {
        try (InputStream in = TableDescriptorGoldenTest.class.getResourceAsStream(GOLDEN)) {
            assertThat(in).as("golden resource %s must exist", GOLDEN).isNotNull();
            try (Scanner scanner = new Scanner(in, "UTF-8").useDelimiter("\\A")) {
                return scanner.hasNext() ? scanner.next() : "";
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void producesCanonicalGolden() {
        String json = TableDescriptorSerializer.toJson(canonicalDescriptor());
        assertThat(json.trim()).isEqualTo(readGolden().trim());
    }

    @Test
    public void parsesCanonicalGolden() {
        TableDescriptor descriptor = JsonSerdeUtil.fromJson(readGolden(), TableDescriptor.class);
        assertThat(descriptor.version()).isEqualTo(1);
        assertThat(descriptor.path()).isEqualTo("file:///tmp/warehouse/mydb.db/t");
        assertThat(descriptor.database()).isEqualTo("mydb");
        assertThat(descriptor.name()).isEqualTo("t");
        assertThat(descriptor.tableSchema().id()).isEqualTo(1L);
        assertThat(descriptor.tableSchema().highestFieldId()).isEqualTo(2);
        assertThat(descriptor.tableSchema().primaryKeys()).containsExactly("id");
        assertThat(descriptor.tableSchema().fields()).hasSize(3);
        assertThat(descriptor.tableSchema().fields().get(2).name()).isEqualTo("tags");
    }
}
