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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.NextIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/** Tests for {@link org.apache.paimon.table.sink.TableRecordSink}. */
public class TableRecordSinkTest {

    private static final Random RANDOM = new Random();

    @TempDir private Path dir;

    @Test
    public void testFunctionRandom() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .partitionKeys("f2")
                        .build();
        IOManager ioManager = new IOManagerImpl(dir.toString());

        TableSchema tableSchema = schema.toTableSchema(0);
        RowKeyExtractor rowKeyExtractor =
                new RowKeyExtractor(tableSchema) {
                    @Override
                    public int bucket() {
                        return 0;
                    }
                };

        TableRecordSink.BatchBucketSorter tableRecordSink =
                new TableRecordSink.BatchBucketSorter(ioManager, rowKeyExtractor, tableSchema);

        List<BinaryRow> datas = data3(Math.abs(RANDOM.nextInt(100)) + 100);
        int i = 1;
        for (BinaryRow data : datas) {
            rowKeyExtractor.setRecord(data);
            tableRecordSink.write(
                    rowKeyExtractor.partition(), rowKeyExtractor.bucket(), rowKeyExtractor.record);
            if (i++ / 10 == 0) {
                tableRecordSink.flushMemory();
            }
        }

        TreeSet<BinaryRow> treeSet =
                new TreeSet<>(
                        Comparator.comparingLong((BinaryRow o) -> o.getLong(2))
                                .thenComparingLong(o -> o.getLong(0)));
        treeSet.addAll(datas);
        NextIterator<SinkRecord> iterator = tableRecordSink.sortedIterator();
        SinkRecord sinkRecord;
        Iterator<BinaryRow> treeIterator = treeSet.iterator();
        while ((sinkRecord = iterator.nextOrNull()) != null) {
            InternalRow row = sinkRecord.row();
            long f0 = row.getLong(0);
            BinaryString f1 = row.getString(1);
            long f2 = row.getLong(2);

            BinaryRow treeRow = treeIterator.next();
            long t0 = treeRow.getLong(0);
            BinaryString t1 = treeRow.getString(1);
            long t2 = treeRow.getLong(2);

            Assertions.assertThat(f0).isEqualTo(t0);
            Assertions.assertThat(f1).isEqualTo(t1);
            Assertions.assertThat(f2).isEqualTo(t2);
        }
    }

    @Test
    public void testRowKindWithFlush() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .partitionKeys("f0")
                        .build();
        IOManager ioManager = new IOManagerImpl(dir.toString());

        TableSchema tableSchema = schema.toTableSchema(0);
        RowKeyExtractor rowKeyExtractor =
                new RowKeyExtractor(tableSchema) {
                    @Override
                    public int bucket() {
                        return 0;
                    }
                };

        TableRecordSink.BatchBucketSorter sorter =
                new TableRecordSink.BatchBucketSorter(ioManager, rowKeyExtractor, tableSchema);

        TableRecordSink.BatchBucketSorter sorter2 =
                new TableRecordSink.BatchBucketSorter(ioManager, rowKeyExtractor, tableSchema);

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.ofKind(RowKind.DELETE, 0, 0));
        rows.add(GenericRow.of(1, 0));
        rows.add(GenericRow.ofKind(RowKind.DELETE, 1, 1));
        rows.add(GenericRow.of(0, 1));
        rows.add(GenericRow.of(0, 1));
        rows.add(GenericRow.of(1, 0));
        rows.add(GenericRow.of(1, 0));
        rows.add(GenericRow.of(0, 1));
        rows.add(GenericRow.ofKind(RowKind.DELETE, 0, 1));
        rows.add(GenericRow.of(1, 1));
        rows.add(GenericRow.of(1, 1));
        rows.add(GenericRow.ofKind(RowKind.DELETE, 1, 0));
        rows.add(GenericRow.of(0, 0));
        rows.add(GenericRow.of(1, 1));

        for (InternalRow row : rows) {
            rowKeyExtractor.setRecord(row);
            sorter.write(
                    rowKeyExtractor.partition(), rowKeyExtractor.bucket(), rowKeyExtractor.record);
            sorter.flushMemory();
            sorter2.write(
                    rowKeyExtractor.partition(), rowKeyExtractor.bucket(), rowKeyExtractor.record);
        }

        NextIterator<SinkRecord> i1 = sorter.sortedIterator();
        NextIterator<SinkRecord> i2 = sorter2.sortedIterator();

        SinkRecord sinkRecord;
        while ((sinkRecord = i1.nextOrNull()) != null) {
            SinkRecord sinkRecord2 = i2.nextOrNull();
            Assertions.assertThat(sinkRecord.row().getRowKind())
                    .isEqualTo(sinkRecord2.row().getRowKind());
        }
    }

    @Test
    public void testFunctionRandomWithBucket() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .column("f3", DataTypes.BIGINT())
                        .partitionKeys("f2")
                        .build();
        IOManager ioManager = new IOManagerImpl(dir.toString());

        TableSchema tableSchema = schema.toTableSchema(0);
        RowKeyExtractor rowKeyExtractor =
                new RowKeyExtractor(schema.toTableSchema(0)) {
                    @Override
                    public int bucket() {
                        return (int) record.getLong(3);
                    }
                };

        TableRecordSink.BatchBucketSorter tableRecordSink =
                new TableRecordSink.BatchBucketSorter(ioManager, rowKeyExtractor, tableSchema);

        List<BinaryRow> datas = data4(Math.abs(RANDOM.nextInt(100)) + 100);
        int i = 1;
        for (BinaryRow data : datas) {
            rowKeyExtractor.setRecord(data);
            tableRecordSink.write(
                    rowKeyExtractor.partition(), rowKeyExtractor.bucket(), rowKeyExtractor.record);
            if (i++ / 10 == 0) {
                tableRecordSink.flushMemory();
            }
        }

        TreeSet<BinaryRow> treeSet =
                new TreeSet<>(
                        Comparator.comparingLong((BinaryRow o) -> o.getLong(2))
                                .thenComparingLong(o -> o.getLong(3))
                                .thenComparingLong(o -> o.getLong(0)));
        treeSet.addAll(datas);
        NextIterator<SinkRecord> iterator = tableRecordSink.sortedIterator();
        SinkRecord sinkRecord;
        Iterator<BinaryRow> treeIterator = treeSet.iterator();
        while ((sinkRecord = iterator.nextOrNull()) != null) {
            InternalRow row = sinkRecord.row();
            long f0 = row.getLong(0);
            BinaryString f1 = row.getString(1);
            long f2 = row.getLong(2);

            BinaryRow treeRow = treeIterator.next();
            long t0 = treeRow.getLong(0);
            BinaryString t1 = treeRow.getString(1);
            long t2 = treeRow.getLong(2);

            Assertions.assertThat(f0).isEqualTo(t0);
            Assertions.assertThat(f1).isEqualTo(t1);
            Assertions.assertThat(f2).isEqualTo(t2);
        }
    }

    private List<BinaryRow> data3(int size) {
        List<BinaryRow> datas = new ArrayList<>();
        BinaryRow binaryRow = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        byte[] bytes = new byte[1024];

        long l = 0;
        for (int i = 0; i < size; i++) {
            writer.writeLong(0, l++);
            RANDOM.nextBytes(bytes);
            writer.writeString(1, BinaryString.fromBytes(bytes));
            // use int to bound the value
            writer.writeLong(2, RANDOM.nextInt(10));
            writer.complete();
            datas.add(binaryRow.copy());
        }

        return datas;
    }

    private List<BinaryRow> data4(int size) {
        List<BinaryRow> datas = new ArrayList<>();
        BinaryRow binaryRow = new BinaryRow(4);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        byte[] bytes = new byte[1024];

        long l = 0;
        for (int i = 0; i < size; i++) {
            writer.writeLong(0, l++);
            RANDOM.nextBytes(bytes);
            writer.writeString(1, BinaryString.fromBytes(bytes));
            // use int to bound the value
            writer.writeLong(2, RANDOM.nextInt(10));
            writer.writeLong(3, RANDOM.nextInt(10));
            writer.complete();
            datas.add(binaryRow.copy());
        }

        return datas;
    }
}
