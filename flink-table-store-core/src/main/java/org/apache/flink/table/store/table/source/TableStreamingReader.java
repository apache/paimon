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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateFilter;
import org.apache.flink.table.store.reader.RecordReaderIterator;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.source.snapshot.ContinuousDataFileSnapshotEnumerator;
import org.apache.flink.table.store.table.source.snapshot.SnapshotEnumerator;
import org.apache.flink.table.store.utils.TypeUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;
import org.apache.flink.shaded.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.transformFieldMapping;

/** A streaming reader to read table. */
public class TableStreamingReader {

    private final FileStoreTable table;
    private final int[] projection;
    @Nullable private final Predicate predicate;
    @Nullable private final PredicateFilter recordFilter;
    private final SnapshotEnumerator enumerator;

    public TableStreamingReader(
            FileStoreTable table, int[] projection, @Nullable Predicate predicate) {
        this.table = table;
        this.projection = projection;
        this.predicate = predicate;

        if (predicate != null) {
            List<String> fieldNames = table.schema().fieldNames();
            List<String> primaryKeys = table.schema().primaryKeys();

            // for pk table: only filter by pk, the stream is upsert instead of changelog
            // for non-pk table: filter all
            IntUnaryOperator operator =
                    i -> {
                        int index = Ints.indexOf(projection, i);
                        boolean safeFilter =
                                primaryKeys.isEmpty() || primaryKeys.contains(fieldNames.get(i));
                        return safeFilter ? index : -1;
                    };

            int[] fieldIdxToProjectionIdx =
                    IntStream.range(0, table.schema().fields().size()).map(operator).toArray();

            this.recordFilter =
                    new PredicateFilter(
                            TypeUtils.project(table.schema().logicalRowType(), projection),
                            transformFieldMapping(predicate, fieldIdxToProjectionIdx).orElse(null));
        } else {
            recordFilter = null;
        }

        DataTableScan scan = table.newScan();
        if (predicate != null) {
            scan.withFilter(predicate);
        }
        enumerator = ContinuousDataFileSnapshotEnumerator.createWithSnapshotStarting(table, scan);
    }

    @Nullable
    public Iterator<InternalRow> nextBatch() throws Exception {
        DataTableScan.DataFilePlan plan = enumerator.enumerate();
        return plan == null ? null : read(plan);
    }

    private Iterator<InternalRow> read(DataTableScan.DataFilePlan plan) throws IOException {
        InnerTableRead read = table.newRead().withProjection(projection);
        if (predicate != null) {
            read.withFilter(predicate);
        }

        List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (DataSplit split : plan.splits) {
            readers.add(() -> read.createReader(split));
        }
        Iterator<InternalRow> iterator =
                new RecordReaderIterator<>(ConcatRecordReader.create(readers));
        if (recordFilter != null) {
            return Iterators.filter(iterator, recordFilter::test);
        }
        return iterator;
    }
}
