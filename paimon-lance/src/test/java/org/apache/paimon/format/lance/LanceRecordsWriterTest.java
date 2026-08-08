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

package org.apache.paimon.format.lance;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.lance.jni.LanceWriter;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LanceRecordsWriter}. */
class LanceRecordsWriterTest {

    @Test
    void testArrowBundlePreservesRowBundleRowOrder() throws Exception {
        RowType rowType = RowType.builder().field("value", DataTypes.INT()).build();
        ArrowFormatWriter arrowWriter = new ArrowFormatWriter(rowType, 1024, true);
        BufferAllocator writerAllocator = arrowWriter.getAllocator();
        CapturingLanceWriter nativeWriter = new CapturingLanceWriter();
        LanceRecordsWriter writer = new LanceRecordsWriter(() -> 0L, arrowWriter, nativeWriter);

        writer.addElement(GenericRow.of(1));
        try (BufferAllocator sourceAllocator =
                        arrowWriter
                                .getAllocator()
                                .newChildAllocator("lance-bundle-test", 0, Long.MAX_VALUE);
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            setInt((IntVector) root.getVector("value"), 2);
            root.setRowCount(1);
            writer.writeBundle(new ArrowBundleRecords(root, rowType, true));
        }
        writer.addElement(GenericRow.of(3));
        writer.close();

        assertThat(nativeWriter.snapshots).hasSize(3);
        assertThat(nativeWriter.snapshots.get(0).values.get(0)).containsExactly(1);
        assertThat(nativeWriter.snapshots.get(1).values.get(0)).containsExactly(2);
        assertThat(nativeWriter.snapshots.get(2).values.get(0)).containsExactly(3);
        assertThat(nativeWriter.initializedAllocator).isSameAs(writerAllocator);
    }

    @Test
    void testReorderedArrowBundleFallsBackToRows() throws Exception {
        RowType writerType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).build();
        RowType sourceType =
                RowType.builder().field("b", DataTypes.INT()).field("a", DataTypes.INT()).build();
        ArrowFormatWriter arrowWriter = new ArrowFormatWriter(writerType, 1024, true);
        CapturingLanceWriter nativeWriter = new CapturingLanceWriter();
        LanceRecordsWriter writer = new LanceRecordsWriter(() -> 0L, arrowWriter, nativeWriter);

        try (BufferAllocator sourceAllocator =
                        arrowWriter
                                .getAllocator()
                                .newChildAllocator("lance-schema-test", 0, Long.MAX_VALUE);
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(sourceType, sourceAllocator)) {
            setInt((IntVector) root.getVector("b"), 20);
            setInt((IntVector) root.getVector("a"), 10);
            root.setRowCount(1);
            writer.writeBundle(new ArrowBundleRecords(root, writerType, true));
        }
        writer.close();

        assertThat(nativeWriter.snapshots).hasSize(1);
        Snapshot snapshot = nativeWriter.snapshots.get(0);
        assertThat(snapshot.fieldNames).containsExactly("a", "b");
        assertThat(snapshot.values.get(0)).containsExactly(10);
        assertThat(snapshot.values.get(1)).containsExactly(20);
    }

    @Test
    void testDifferentAllocatorRootFallsBackToRows() throws Exception {
        RowType rowType = RowType.builder().field("value", DataTypes.INT()).build();
        ArrowFormatWriter arrowWriter = new ArrowFormatWriter(rowType, 1024, true);
        CapturingLanceWriter nativeWriter = new CapturingLanceWriter();
        LanceRecordsWriter writer = new LanceRecordsWriter(() -> 0L, arrowWriter, nativeWriter);

        try (BufferAllocator sourceAllocator = new org.apache.arrow.memory.RootAllocator();
                VectorSchemaRoot root =
                        ArrowUtils.createVectorSchemaRoot(rowType, sourceAllocator)) {
            nativeWriter.disallowedRoot = root;
            setInt((IntVector) root.getVector("value"), 10);
            root.setRowCount(1);

            writer.writeBundle(new ArrowBundleRecords(root, rowType, true));
        }
        writer.close();

        assertThat(nativeWriter.disallowedRootWrites).isZero();
        assertThat(nativeWriter.snapshots).hasSize(1);
        assertThat(nativeWriter.snapshots.get(0).values.get(0)).containsExactly(10);
    }

    private static void setInt(IntVector vector, int value) {
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
    }

    private static class CapturingLanceWriter extends LanceWriter {

        private final List<Snapshot> snapshots = new ArrayList<>();
        private BufferAllocator initializedAllocator;
        private VectorSchemaRoot disallowedRoot;
        private int disallowedRootWrites;

        private CapturingLanceWriter() {
            super("unused", Collections.emptyMap());
        }

        @Override
        public void ensureInitialized(BufferAllocator bufferAllocator) {
            initializedAllocator = bufferAllocator;
        }

        @Override
        public void writeVsr(VectorSchemaRoot root) {
            if (root == disallowedRoot) {
                disallowedRootWrites++;
            }
            List<String> fieldNames =
                    root.getSchema().getFields().stream()
                            .map(field -> field.getName())
                            .collect(Collectors.toList());
            List<List<Integer>> values = new ArrayList<>();
            for (int column = 0; column < root.getFieldVectors().size(); column++) {
                IntVector vector = (IntVector) root.getVector(column);
                List<Integer> columnValues = new ArrayList<>();
                for (int row = 0; row < root.getRowCount(); row++) {
                    columnValues.add(vector.get(row));
                }
                values.add(columnValues);
            }
            snapshots.add(new Snapshot(fieldNames, values));
        }

        @Override
        public void close() throws IOException {}

        @Override
        public String path() {
            return "unused";
        }
    }

    private static class Snapshot {

        private final List<String> fieldNames;
        private final List<List<Integer>> values;

        private Snapshot(List<String> fieldNames, List<List<Integer>> values) {
            this.fieldNames = fieldNames;
            this.values = values;
        }
    }
}
