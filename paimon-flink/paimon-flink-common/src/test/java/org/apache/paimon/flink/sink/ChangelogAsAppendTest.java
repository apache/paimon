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

package org.apache.paimon.flink.sink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Payload-log semantics, including before images, deletes, duplicates and object reuse. */
class ChangelogAsAppendTest {
    private final RowType type =
            RowType.of(
                    new org.apache.flink.table.types.logical.LogicalType[] {
                        new IntType(),
                        RowType.of(new IntType()),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new BigIntType()
                    },
                    new String[] {"played", "nested", "kind", "emitted_ms"});

    @Test
    void retainsEveryPayloadAndCopiesReusedRows() {
        ChangelogAsAppend converter = new ChangelogAsAppend(type, "kind", "emitted_ms");
        GenericRowData nested = GenericRowData.of(42);
        GenericRowData input = GenericRowData.of(0, nested, null, null);
        List<RowData> output = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (RowKind kind : RowKind.values()) {
            input.setRowKind(kind);
            RowData row = converter.map(input);
            output.add(row);
            assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
            assertThat(row.getString(2).toString()).isEqualTo(kind.name());
            assertThat(input.getRowKind()).isEqualTo(kind);
            assertThat(input.isNullAt(2)).isTrue();
            assertThat(row.getLong(3)).isBetween(start, System.currentTimeMillis());
        }
        // A duplicate payload must remain a separate positive record.
        output.add(converter.map(input));
        input.setField(0, 1000);
        nested.setField(0, 99);
        assertThat(output).hasSize(5);
        for (RowData row : output) {
            assertThat(row.getInt(0)).isZero();
            assertThat(row.getRow(1, 1).getInt(0)).isEqualTo(42);
        }
    }

    @Test
    void retainsNullPayloadFields() {
        RowData row =
                new ChangelogAsAppend(type, "kind", "emitted_ms")
                        .map(GenericRowData.of(null, null, null, null));
        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.isNullAt(1)).isTrue();
    }

    @Test
    void requiresExplicitMetadataFieldsWithCorrectTypes() {
        assertThatThrownBy(() -> new ChangelogAsAppend(type, null, "emitted_ms"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChangelogAsAppend(type, "played", "emitted_ms"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChangelogAsAppend(type, "kind", "nested"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonNullableOrTruncatedMetadata() {
        for (org.apache.flink.table.types.logical.LogicalType[] fields :
                new org.apache.flink.table.types.logical.LogicalType[][] {
                    {new VarCharType(false, VarCharType.MAX_LENGTH), new BigIntType()},
                    {new VarCharType(VarCharType.MAX_LENGTH), new BigIntType(false)},
                    {new VarCharType(12), new BigIntType()}
                }) {
            RowType metadata = RowType.of(fields, new String[] {"kind", "emitted_ms"});
            assertThatThrownBy(() -> new ChangelogAsAppend(metadata, "kind", "emitted_ms"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nullable; kind field must fit UPDATE_BEFORE");
        }
    }

    @Test
    void overwritesMetadataWithoutChangingInput() {
        GenericRowData input = GenericRowData.of(0, null, null, 1L);
        input.setRowKind(RowKind.DELETE);
        long started = System.currentTimeMillis();
        RowData output = new ChangelogAsAppend(type, "kind", "emitted_ms").map(input);
        assertThat(output.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(output.getString(2).toString()).isEqualTo("DELETE");
        assertThat(output.getLong(3)).isBetween(started, System.currentTimeMillis());
        assertThat(input.isNullAt(2)).isTrue();
        assertThat(input.getLong(3)).isEqualTo(1L);
        assertThat(input.getRowKind()).isEqualTo(RowKind.DELETE);
    }
}
