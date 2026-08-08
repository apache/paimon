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

package org.apache.paimon.format.shredding;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InferShreddingWritePlanWriter}. */
class InferShreddingWritePlanWriterTest {

    private static final RowType ROW_TYPE =
            RowType.builder().field("value", DataTypes.INT()).build();

    @Test
    void testMixedRowsAndBundlesPreserveOrderAndInferenceBoundary() throws Exception {
        TestingWriterFactory writerFactory = new TestingWriterFactory();
        TestingWritePlanFactory writePlanFactory = new TestingWritePlanFactory(3);
        InferShreddingWritePlanWriter writer =
                new InferShreddingWritePlanWriter(
                        writerFactory, writePlanFactory, new NoOpPositionOutputStream(), "none");

        writer.addElement(GenericRow.of(1));
        writer.writeBundle(bundle(GenericRow.of(2), GenericRow.of(3), GenericRow.of(4)));
        writer.addElement(GenericRow.of(5));
        writer.writeBundle(bundle(GenericRow.of(6), GenericRow.of(7)));
        writer.close();

        assertThat(writePlanFactory.sampleValues).containsExactly(1, 2, 3, 4);
        assertThat(writerFactory.writer.values).containsExactly(101, 102, 103, 104, 105, 106, 107);
    }

    @Test
    void testCloseFinalizesBufferedRowsAndBundle() throws Exception {
        TestingWriterFactory writerFactory = new TestingWriterFactory();
        TestingWritePlanFactory writePlanFactory = new TestingWritePlanFactory(10);
        InferShreddingWritePlanWriter writer =
                new InferShreddingWritePlanWriter(
                        writerFactory, writePlanFactory, new NoOpPositionOutputStream(), "none");

        writer.addElement(GenericRow.of(1));
        writer.writeBundle(bundle(GenericRow.of(2), GenericRow.of(3)));
        writer.close();

        assertThat(writePlanFactory.sampleValues).containsExactly(1, 2, 3);
        assertThat(writerFactory.writer.values).containsExactly(101, 102, 103);
    }

    private static BundleRecords bundle(InternalRow... rows) {
        List<InternalRow> records = Arrays.asList(rows);
        return new BundleRecords() {
            @Override
            public Iterator<InternalRow> iterator() {
                return records.iterator();
            }

            @Override
            public long rowCount() {
                return records.size();
            }
        };
    }

    private static class TestingWriterFactory implements SupportsShreddingWritePlan {

        private final TestingFormatWriter writer = new TestingFormatWriter();

        @Override
        public FormatWriter createWithShreddingWritePlan(
                PositionOutputStream out, String compression, ShreddingWritePlan writePlan) {
            return writer;
        }
    }

    private static class TestingFormatWriter implements FormatWriter {

        private final List<Integer> values = new ArrayList<>();

        @Override
        public void addElement(InternalRow element) {
            values.add(element.getInt(0));
        }

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {}
    }

    private static class TestingWritePlanFactory implements ShreddingWritePlanFactory {

        private final int inferBufferRowCount;
        private final List<Integer> sampleValues = new ArrayList<>();

        private TestingWritePlanFactory(int inferBufferRowCount) {
            this.inferBufferRowCount = inferBufferRowCount;
        }

        @Override
        public RowType logicalRowType() {
            return ROW_TYPE;
        }

        @Override
        public boolean shouldCreateWritePlan() {
            return true;
        }

        @Override
        public boolean shouldInferWritePlan() {
            return true;
        }

        @Override
        public int inferBufferRowCount() {
            return inferBufferRowCount;
        }

        @Override
        public ShreddingWritePlan createWritePlan(List<InternalRow> sampleRows) {
            for (InternalRow row : sampleRows) {
                sampleValues.add(row.getInt(0));
            }
            return new TestingWritePlan();
        }
    }

    private static class TestingWritePlan implements ShreddingWritePlan {

        @Override
        public RowType logicalRowType() {
            return ROW_TYPE;
        }

        @Override
        public RowType physicalRowType() {
            return ROW_TYPE;
        }

        @Override
        public InternalRow toPhysicalRow(InternalRow row) {
            return GenericRow.of(row.getInt(0) + 100);
        }
    }

    private static class NoOpPositionOutputStream extends PositionOutputStream {

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public void write(int b) {}

        @Override
        public void write(byte[] b) {}

        @Override
        public void write(byte[] b, int off, int len) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
