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

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the Flink projection and conversion around shared generated-column evaluation. */
class GeneratedColumnMapFunctionTest {

    @ParameterizedTest
    @EnumSource(RowKind.class)
    void testOmittedColumnsDefaultsAndSuppliedNull(RowKind kind) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataField(0, "name", DataTypes.STRING()).newDefaultValue("fallback"),
                        new DataField(1, "number", DataTypes.INT())
                                .newGenerationExpression("CAST(text AS INT)"),
                        new DataField(2, "text", DataTypes.STRING()),
                        new DataField(3, "label", DataTypes.STRING())
                                .newGenerationExpression("UPPER(name)"),
                        new DataField(4, "dt", DataTypes.DATE())
                                .newGenerationExpression("CAST(event_day AS DATE)"),
                        new DataField(5, "event_day", DataTypes.STRING()));
        GeneratedColumnMapFunction mapper =
                new GeneratedColumnMapFunction(rowType, new int[] {4, 3});
        GenericRowData input =
                GenericRowData.of(
                        null,
                        null,
                        StringData.fromString("invalid integer"),
                        null,
                        null,
                        StringData.fromString("2026-11-02"));
        input.setRowKind(kind);

        RowData first = mapper.map(input);
        assertThat(first.getRowKind()).isEqualTo(kind);
        assertThat(first.isNullAt(1)).isTrue();
        assertThat(first.getString(3).toString()).isEqualTo("FALLBACK");
        assertThat(first.getInt(4)).isEqualTo((int) LocalDate.of(2026, 11, 2).toEpochDay());
        assertThat(input.isNullAt(3)).isTrue();
        assertThat(input.isNullAt(4)).isTrue();

        input.setField(0, StringData.fromString("other"));
        input.setField(1, 42);
        input.setField(5, StringData.fromString("2026-11-03"));
        RowData second = mapper.map(input);
        assertThat(second.getRowKind()).isEqualTo(kind);
        assertThat(second.getInt(1)).isEqualTo(42);
        assertThat(second.getString(3).toString()).isEqualTo("OTHER");
        assertThat(second.getInt(4)).isEqualTo((int) LocalDate.of(2026, 11, 3).toEpochDay());
        assertThat(first.getString(3).toString()).isEqualTo("FALLBACK");
        assertThat(first.getInt(4)).isEqualTo((int) LocalDate.of(2026, 11, 2).toEpochDay());
    }

    @Test
    void testSuppliedBlobAndNestedRowKeepFlinkRepresentation() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataField(0, "name", DataTypes.STRING()),
                        new DataField(1, "content", DataTypes.BLOB()),
                        new DataField(
                                2,
                                "nested",
                                DataTypes.ROW(new DataField(10, "content", DataTypes.BLOB()))),
                        new DataField(3, "label", DataTypes.STRING())
                                .newGenerationExpression("UPPER(name)"));
        GeneratedColumnMapFunction mapper = new GeneratedColumnMapFunction(rowType, new int[] {3});
        byte[] inline = new byte[] {1, 2, 3};
        byte[] descriptor = new BlobDescriptor("file:///not-opened", 1, 2).serialize();
        GenericRowData input =
                GenericRowData.of(
                        StringData.fromString("hello"),
                        inline,
                        GenericRowData.of((Object) descriptor),
                        null);

        RowData result = mapper.map(input);

        assertThat(result.getString(3).toString()).isEqualTo("HELLO");
        assertThat(result.getBinary(1)).isEqualTo(inline);
        assertThat(result.getRow(2, 1).getBinary(0)).isEqualTo(descriptor);
    }
}
