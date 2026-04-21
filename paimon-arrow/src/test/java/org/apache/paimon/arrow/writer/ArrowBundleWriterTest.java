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

package org.apache.paimon.arrow.writer;

import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowBundleWriter}. */
public class ArrowBundleWriterTest {

    @Test
    public void testAddBatchWithoutDeletionVector() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.BIGINT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 1024, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector intVector = new HeapIntVector(4);
        HeapLongVector longVector = new HeapLongVector(4);
        for (int i = 0; i < 4; i++) {
            intVector.setInt(i, (i + 1) * 10);
            longVector.setLong(i, (i + 1) * 100L);
        }

        VectorizedColumnBatch batch =
                new VectorizedColumnBatch(new ColumnVector[] {intVector, longVector});
        batch.setNumRows(4);

        writer.add(batch, null);
        writer.close();

        assertThat(nativeWriter.snapshots).hasSize(1);
        CapturingNativeWriter.Snapshot snapshot = nativeWriter.snapshots.get(0);
        assertThat(snapshot.rowCount).isEqualTo(4);
        assertThat(snapshot.intValues).containsExactly(10, 20, 30, 40);
        assertThat(snapshot.longValues).containsExactly(100L, 200L, 300L, 400L);
    }

    @Test
    public void testAddBatchWithDeletionVector() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.BIGINT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 1024, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector intVector = new HeapIntVector(4);
        HeapLongVector longVector = new HeapLongVector(4);
        for (int i = 0; i < 4; i++) {
            intVector.setInt(i, (i + 1) * 10);
            longVector.setLong(i, (i + 1) * 100L);
        }

        VectorizedColumnBatch batch =
                new VectorizedColumnBatch(new ColumnVector[] {intVector, longVector});
        batch.setNumRows(4);

        // pick rows 0 and 2 (skip 1 and 3)
        writer.add(batch, new int[] {0, 2});
        writer.close();

        assertThat(nativeWriter.snapshots).hasSize(1);
        CapturingNativeWriter.Snapshot snapshot = nativeWriter.snapshots.get(0);
        assertThat(snapshot.rowCount).isEqualTo(2);
        assertThat(snapshot.intValues).containsExactly(10, 30);
        assertThat(snapshot.longValues).containsExactly(100L, 300L);
    }

    @Test
    public void testAddBatchWithAllDeleted() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 1024, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector intVector = new HeapIntVector(3);
        for (int i = 0; i < 3; i++) {
            intVector.setInt(i, i);
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {intVector});
        batch.setNumRows(3);

        // all deleted -> empty picked array
        writer.add(batch, new int[] {});
        writer.close();

        assertThat(nativeWriter.snapshots).isEmpty();
    }

    @Test
    public void testAddBatchWithNoneDeleted() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 1024, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector intVector = new HeapIntVector(3);
        for (int i = 0; i < 3; i++) {
            intVector.setInt(i, (i + 1) * 5);
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {intVector});
        batch.setNumRows(3);

        // none deleted -> pick all rows
        writer.add(batch, new int[] {0, 1, 2});
        writer.close();

        assertThat(nativeWriter.snapshots).hasSize(1);
        CapturingNativeWriter.Snapshot snapshot = nativeWriter.snapshots.get(0);
        assertThat(snapshot.rowCount).isEqualTo(3);
        assertThat(snapshot.intValues).containsExactly(5, 10, 15);
    }

    @Test
    public void testAddBatchLargerThanWriterBatchSize() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        // batchSize = 3, but input has 7 rows
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector intVector = new HeapIntVector(7);
        for (int i = 0; i < 7; i++) {
            intVector.setInt(i, i + 1);
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {intVector});
        batch.setNumRows(7);

        writer.add(batch, null);
        writer.close();

        // should be split into 3 batches: 3 + 3 + 1
        assertThat(nativeWriter.snapshots).hasSize(3);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(3);
        assertThat(nativeWriter.snapshots.get(0).intValues).containsExactly(1, 2, 3);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(3);
        assertThat(nativeWriter.snapshots.get(1).intValues).containsExactly(4, 5, 6);
        assertThat(nativeWriter.snapshots.get(2).rowCount).isEqualTo(1);
        assertThat(nativeWriter.snapshots.get(2).intValues).containsExactly(7);
    }

    @Test
    public void testAddBatchLargerThanWriterBatchSizeWithDeletion() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        // batchSize = 2, input has 6 rows, pick rows 0,2,4 -> values [1,3,5]
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 2, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector intVector = new HeapIntVector(6);
        for (int i = 0; i < 6; i++) {
            intVector.setInt(i, i + 1);
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {intVector});
        batch.setNumRows(6);

        writer.add(batch, new int[] {0, 2, 4});
        writer.close();

        // 3 remaining rows, batchSize=2 -> 2 batches: 2 + 1
        assertThat(nativeWriter.snapshots).hasSize(2);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(0).intValues).containsExactly(1, 3);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(1);
        assertThat(nativeWriter.snapshots.get(1).intValues).containsExactly(5);
    }

    @Test
    public void testMultipleSmallBatchesFillOneWriterBatch() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 4, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        // batch 1: [1, 2]
        HeapIntVector intVector1 = new HeapIntVector(2);
        intVector1.setInt(0, 1);
        intVector1.setInt(1, 2);
        VectorizedColumnBatch batch1 = new VectorizedColumnBatch(new ColumnVector[] {intVector1});
        batch1.setNumRows(2);
        writer.add(batch1, null);

        // batch 2: [3, 4] -> flushes [1,2] first, then writes [3,4]
        HeapIntVector intVector2 = new HeapIntVector(2);
        intVector2.setInt(0, 3);
        intVector2.setInt(1, 4);
        VectorizedColumnBatch batch2 = new VectorizedColumnBatch(new ColumnVector[] {intVector2});
        batch2.setNumRows(2);
        writer.add(batch2, null);

        writer.close();

        assertThat(nativeWriter.snapshots).hasSize(2);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(0).intValues).containsExactly(1, 2);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(1).intValues).containsExactly(3, 4);
    }

    @Test
    public void testMultipleSmallBatchesSpanWriterBatches() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        // writerBatchSize=3, add three batches of 2
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector iv1 = new HeapIntVector(2);
        iv1.setInt(0, 1);
        iv1.setInt(1, 2);
        VectorizedColumnBatch b1 = new VectorizedColumnBatch(new ColumnVector[] {iv1});
        b1.setNumRows(2);
        writer.add(b1, null);

        HeapIntVector iv2 = new HeapIntVector(2);
        iv2.setInt(0, 3);
        iv2.setInt(1, 4);
        VectorizedColumnBatch b2 = new VectorizedColumnBatch(new ColumnVector[] {iv2});
        b2.setNumRows(2);
        writer.add(b2, null);

        HeapIntVector iv3 = new HeapIntVector(2);
        iv3.setInt(0, 5);
        iv3.setInt(1, 6);
        VectorizedColumnBatch b3 = new VectorizedColumnBatch(new ColumnVector[] {iv3});
        b3.setNumRows(2);
        writer.add(b3, null);

        writer.close();

        // each add() flushes previous, so 3 batches of 2
        assertThat(nativeWriter.snapshots).hasSize(3);
        assertThat(nativeWriter.snapshots.get(0).intValues).containsExactly(1, 2);
        assertThat(nativeWriter.snapshots.get(1).intValues).containsExactly(3, 4);
        assertThat(nativeWriter.snapshots.get(2).intValues).containsExactly(5, 6);
    }

    @Test
    public void testMultipleSmallBatchesWithDeletion() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        // batch 1: [10, 20, 30, 40], pick index 0,2 -> remaining [10, 30]
        HeapIntVector iv1 = new HeapIntVector(4);
        iv1.setInt(0, 10);
        iv1.setInt(1, 20);
        iv1.setInt(2, 30);
        iv1.setInt(3, 40);
        VectorizedColumnBatch b1 = new VectorizedColumnBatch(new ColumnVector[] {iv1});
        b1.setNumRows(4);
        writer.add(b1, new int[] {0, 2});

        // batch 2: [50, 60, 70, 80], pick index 0,2 -> remaining [50, 70]
        HeapIntVector iv2 = new HeapIntVector(4);
        iv2.setInt(0, 50);
        iv2.setInt(1, 60);
        iv2.setInt(2, 70);
        iv2.setInt(3, 80);
        VectorizedColumnBatch b2 = new VectorizedColumnBatch(new ColumnVector[] {iv2});
        b2.setNumRows(4);
        writer.add(b2, new int[] {0, 2});

        writer.close();

        // each add() flushes previous
        assertThat(nativeWriter.snapshots).hasSize(2);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(0).intValues).containsExactly(10, 30);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(1).intValues).containsExactly(50, 70);
    }

    @Test
    public void testMultipleBatchesWithTwoColumns() throws IOException {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.BIGINT());
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        HeapIntVector iv1 = new HeapIntVector(2);
        HeapLongVector lv1 = new HeapLongVector(2);
        iv1.setInt(0, 1);
        iv1.setInt(1, 2);
        lv1.setLong(0, 100L);
        lv1.setLong(1, 200L);
        VectorizedColumnBatch b1 = new VectorizedColumnBatch(new ColumnVector[] {iv1, lv1});
        b1.setNumRows(2);
        writer.add(b1, null);

        HeapIntVector iv2 = new HeapIntVector(2);
        HeapLongVector lv2 = new HeapLongVector(2);
        iv2.setInt(0, 3);
        iv2.setInt(1, 4);
        lv2.setLong(0, 300L);
        lv2.setLong(1, 400L);
        VectorizedColumnBatch b2 = new VectorizedColumnBatch(new ColumnVector[] {iv2, lv2});
        b2.setNumRows(2);
        writer.add(b2, null);

        writer.close();

        // each add() flushes previous
        assertThat(nativeWriter.snapshots).hasSize(2);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(0).intValues).containsExactly(1, 2);
        assertThat(nativeWriter.snapshots.get(0).longValues).containsExactly(100L, 200L);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(1).intValues).containsExactly(3, 4);
        assertThat(nativeWriter.snapshots.get(1).longValues).containsExactly(300L, 400L);
    }

    @Test
    public void testMultipleBatchesWithArrayType() throws IOException {
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.INT()));
        // writerBatchSize=3, add two batches of 2
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        // batch 1: [[1,2], [3]] -> 2 rows
        HeapIntVector elements1 = new HeapIntVector(3);
        elements1.setInt(0, 1);
        elements1.setInt(1, 2);
        elements1.setInt(2, 3);
        HeapArrayVector arrayVec1 = new HeapArrayVector(2, elements1);
        arrayVec1.putOffsetLength(0, 0, 2);
        arrayVec1.putOffsetLength(1, 2, 1);
        VectorizedColumnBatch b1 = new VectorizedColumnBatch(new ColumnVector[] {arrayVec1});
        b1.setNumRows(2);
        writer.add(b1, null);

        // batch 2: [[4,5], [6]] -> 2 rows
        HeapIntVector elements2 = new HeapIntVector(3);
        elements2.setInt(0, 4);
        elements2.setInt(1, 5);
        elements2.setInt(2, 6);
        HeapArrayVector arrayVec2 = new HeapArrayVector(2, elements2);
        arrayVec2.putOffsetLength(0, 0, 2);
        arrayVec2.putOffsetLength(1, 2, 1);
        VectorizedColumnBatch b2 = new VectorizedColumnBatch(new ColumnVector[] {arrayVec2});
        b2.setNumRows(2);
        writer.add(b2, null);

        writer.close();

        // each add() flushes previous
        assertThat(nativeWriter.snapshots).hasSize(2);
        CapturingNativeWriter.Snapshot s0 = nativeWriter.snapshots.get(0);
        assertThat(s0.rowCount).isEqualTo(2);
        assertThat(s0.objectColumns.get(0)).containsExactly(Arrays.asList(1, 2), Arrays.asList(3));

        CapturingNativeWriter.Snapshot s1 = nativeWriter.snapshots.get(1);
        assertThat(s1.rowCount).isEqualTo(2);
        assertThat(s1.objectColumns.get(0)).containsExactly(Arrays.asList(4, 5), Arrays.asList(6));
    }

    @Test
    public void testMultipleBatchesWithMapType() throws IOException {
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.INT()));
        // writerBatchSize=3, add two batches of 2
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        // batch 1: {1:10, 2:20}, {3:30} -> 2 rows
        HeapIntVector keys1 = new HeapIntVector(3);
        HeapIntVector vals1 = new HeapIntVector(3);
        keys1.setInt(0, 1);
        keys1.setInt(1, 2);
        keys1.setInt(2, 3);
        vals1.setInt(0, 10);
        vals1.setInt(1, 20);
        vals1.setInt(2, 30);
        HeapMapVector mapVec1 = new HeapMapVector(2, keys1, vals1);
        mapVec1.putOffsetLength(0, 0, 2);
        mapVec1.putOffsetLength(1, 2, 1);
        VectorizedColumnBatch b1 = new VectorizedColumnBatch(new ColumnVector[] {mapVec1});
        b1.setNumRows(2);
        writer.add(b1, null);

        // batch 2: {4:40}, {5:50, 6:60} -> 2 rows
        HeapIntVector keys2 = new HeapIntVector(3);
        HeapIntVector vals2 = new HeapIntVector(3);
        keys2.setInt(0, 4);
        keys2.setInt(1, 5);
        keys2.setInt(2, 6);
        vals2.setInt(0, 40);
        vals2.setInt(1, 50);
        vals2.setInt(2, 60);
        HeapMapVector mapVec2 = new HeapMapVector(2, keys2, vals2);
        mapVec2.putOffsetLength(0, 0, 1);
        mapVec2.putOffsetLength(1, 1, 2);
        VectorizedColumnBatch b2 = new VectorizedColumnBatch(new ColumnVector[] {mapVec2});
        b2.setNumRows(2);
        writer.add(b2, null);

        writer.close();

        // each add() flushes previous
        assertThat(nativeWriter.snapshots).hasSize(2);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(2);
    }

    @Test
    public void testMultipleBatchesWithRowType() throws IOException {
        RowType innerRowType = RowType.of(DataTypes.INT(), DataTypes.INT());
        RowType rowType = RowType.of(innerRowType);
        // writerBatchSize=3, add two batches of 2
        ArrowFormatCWriter cWriter = new ArrowFormatCWriter(rowType, 3, true);
        ArrowFormatWriter formatWriter = cWriter.formatWriter();
        VectorSchemaRoot vsr = formatWriter.getVectorSchemaRoot();

        CapturingNativeWriter nativeWriter = new CapturingNativeWriter(vsr, cWriter);
        PositionOutputStream outputStream = new NoOpPositionOutputStream();

        ArrowBundleWriter writer = new ArrowBundleWriter(outputStream, cWriter, nativeWriter);

        // batch 1: (1,10), (2,20)
        HeapIntVector f1a = new HeapIntVector(2);
        HeapIntVector f1b = new HeapIntVector(2);
        f1a.setInt(0, 1);
        f1a.setInt(1, 2);
        f1b.setInt(0, 10);
        f1b.setInt(1, 20);
        HeapRowVector rowVec1 = new HeapRowVector(2, f1a, f1b);
        VectorizedColumnBatch b1 = new VectorizedColumnBatch(new ColumnVector[] {rowVec1});
        b1.setNumRows(2);
        writer.add(b1, null);

        // batch 2: (3,30), (4,40)
        HeapIntVector f2a = new HeapIntVector(2);
        HeapIntVector f2b = new HeapIntVector(2);
        f2a.setInt(0, 3);
        f2a.setInt(1, 4);
        f2b.setInt(0, 30);
        f2b.setInt(1, 40);
        HeapRowVector rowVec2 = new HeapRowVector(2, f2a, f2b);
        VectorizedColumnBatch b2 = new VectorizedColumnBatch(new ColumnVector[] {rowVec2});
        b2.setNumRows(2);
        writer.add(b2, null);

        writer.close();

        // each add() flushes previous
        assertThat(nativeWriter.snapshots).hasSize(2);
        assertThat(nativeWriter.snapshots.get(0).rowCount).isEqualTo(2);
        assertThat(nativeWriter.snapshots.get(1).rowCount).isEqualTo(2);
    }

    private static class CapturingNativeWriter extends NativeWriter {
        private final VectorSchemaRoot vsr;
        private final ArrowFormatCWriter cWriter;
        final List<Snapshot> snapshots = new ArrayList<>();

        CapturingNativeWriter(VectorSchemaRoot vsr, ArrowFormatCWriter cWriter) {
            this.vsr = vsr;
            this.cWriter = cWriter;
        }

        @Override
        public long nativeMemoryUsed() {
            return 0;
        }

        @Override
        public void writeIpcBytes(long arrayAddress, long schemaAddress) {
            int rowCount = vsr.getRowCount();
            int[] intValues = null;
            long[] longValues = null;
            List<List<Object>> objectColumns = new ArrayList<>();

            for (int col = 0; col < vsr.getFieldVectors().size(); col++) {
                org.apache.arrow.vector.FieldVector fv = vsr.getVector(col);
                if (fv instanceof IntVector) {
                    intValues = new int[rowCount];
                    for (int i = 0; i < rowCount; i++) {
                        intValues[i] = ((IntVector) fv).get(i);
                    }
                } else if (fv instanceof BigIntVector) {
                    longValues = new long[rowCount];
                    for (int i = 0; i < rowCount; i++) {
                        longValues[i] = ((BigIntVector) fv).get(i);
                    }
                }
                List<Object> colObjects = new ArrayList<>();
                for (int i = 0; i < rowCount; i++) {
                    colObjects.add(fv.getObject(i));
                }
                objectColumns.add(colObjects);
            }
            snapshots.add(new Snapshot(rowCount, intValues, longValues, objectColumns));
            cWriter.release();
        }

        @Override
        public void close() {}

        static class Snapshot {
            final int rowCount;
            final int[] intValues;
            final long[] longValues;
            final List<List<Object>> objectColumns;

            Snapshot(
                    int rowCount,
                    int[] intValues,
                    long[] longValues,
                    List<List<Object>> objectColumns) {
                this.rowCount = rowCount;
                this.intValues = intValues;
                this.longValues = longValues;
                this.objectColumns = objectColumns;
            }
        }
    }

    private static class NoOpPositionOutputStream extends PositionOutputStream {
        private long pos = 0;

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            pos++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            pos += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            pos += len;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
