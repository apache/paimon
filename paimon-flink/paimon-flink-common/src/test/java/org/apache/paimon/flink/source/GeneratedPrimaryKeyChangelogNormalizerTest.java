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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.Projection;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedPrimaryKeyChangelogNormalizerTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {new IntType(false), new VarCharType()},
                    new String[] {"id", "value"});

    @Test
    void testUpdatesDeletesAndObjectReuse() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                harness()) {
            harness.open();
            GenericRowData row = GenericRowData.of(1, StringData.fromString("first"));
            harness.processElement(new StreamRecord<>(row));
            row.setField(1, StringData.fromString("second"));
            row.setRowKind(RowKind.UPDATE_AFTER);
            harness.processElement(new StreamRecord<>(row));
            row.setField(1, StringData.fromString("delete payload is ignored"));
            row.setRowKind(RowKind.DELETE);
            harness.processElement(new StreamRecord<>(row));
            row.setField(0, 2);
            harness.processElement(new StreamRecord<>(row));
            row.setField(0, 1);
            row.setField(1, StringData.fromString("third"));
            row.setRowKind(RowKind.UPDATE_AFTER);
            harness.processElement(new StreamRecord<>(row));

            assertThat(output(harness))
                    .containsExactly(
                            Row.of(1, "first"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "first"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "second"),
                            Row.ofKind(RowKind.DELETE, 1, "second"),
                            Row.of(1, "third"));
            assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
        }
    }

    @Test
    void testReusableSourceKeepsKeysIsolated() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                harness()) {
            harness.open();
            GenericRowData row = GenericRowData.of(1, StringData.fromString("first"));
            harness.processElement(new StreamRecord<>(row));
            row.setField(0, 2);
            row.setField(1, StringData.fromString("second"));
            harness.processElement(new StreamRecord<>(row));
            row.setField(0, 1);
            row.setField(1, StringData.fromString("updated"));
            row.setRowKind(RowKind.UPDATE_AFTER);
            harness.processElement(new StreamRecord<>(row));

            assertThat(output(harness))
                    .containsExactly(
                            Row.of(1, "first"),
                            Row.of(2, "second"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "first"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "updated"));
        }
    }

    @Test
    void testCheckpointRestoresBeforeImage() throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                harness()) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(GenericRowData.of(1, StringData.fromString("before"))));
            snapshot = harness.snapshot(1, 1);
        }
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                harness()) {
            harness.initializeState(snapshot);
            harness.open();
            GenericRowData update = GenericRowData.of(1, StringData.fromString("after"));
            update.setRowKind(RowKind.UPDATE_AFTER);
            harness.processElement(new StreamRecord<>(update));

            assertThat(output(harness))
                    .containsExactly(
                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "before"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "after"));
        }
    }

    @Test
    void testRejectBeforeImageInput() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                harness()) {
            harness.open();
            GenericRowData before = GenericRowData.of(1, StringData.fromString("before"));
            before.setRowKind(RowKind.UPDATE_BEFORE);
            assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(before)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Expected an upsert changelog");
        }
    }

    @Test
    void testProjectionOverFullRowsSupportsNestedAndReorderedFields() {
        RowType sourceType =
                RowType.of(new IntType(), RowType.of(new VarCharType(), new IntType()));
        NestedProjectedRowData projected =
                NestedProjectedRowData.from(sourceType, new int[][] {{1, 1}, {0}, {1, 0}});
        GenericRowData row =
                GenericRowData.of(7, GenericRowData.of(StringData.fromString("nested"), 42));
        projected.replaceRow(row);
        assertThat(projected.getInt(0)).isEqualTo(42);
        assertThat(projected.getInt(1)).isEqualTo(7);
        assertThat(projected.getString(2).toString()).isEqualTo("nested");
        row.setField(1, null);
        projected.replaceRow(row);
        assertThat(projected.isNullAt(0)).isTrue();
        assertThat(projected.isNullAt(2)).isTrue();
        assertThat(projected.getInt(1)).isEqualTo(7);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new GeneratedPrimaryKeyChangelogNormalizer(ROW_TYPE),
                new GeneratedPrimaryKeyChangelogNormalizer.PrimaryKeySelector(
                        ROW_TYPE, new int[] {0}),
                InternalTypeInfo.of(Projection.of(new int[] {0}).project(ROW_TYPE)));
    }

    private List<Row> output(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        return harness.extractOutputValues().stream()
                .map(
                        row ->
                                Row.ofKind(
                                        row.getRowKind(),
                                        row.getInt(0),
                                        row.getString(1).toString()))
                .collect(Collectors.toList());
    }
}
