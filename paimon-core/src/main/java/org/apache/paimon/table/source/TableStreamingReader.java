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

package org.apache.paimon.table.source;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateFilter;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataTableScan.DataFilePlan;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;
import org.apache.flink.shaded.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A streaming reader to read table. */
public class TableStreamingReader {

    private final ReadBuilder readBuilder;
    @Nullable private final PredicateFilter recordFilter;
    private final StreamDataTableScan scan;

    public TableStreamingReader(Table table, int[] projection, @Nullable Predicate predicate) {
        this.readBuilder = table.newReadBuilder().withProjection(projection).withFilter(predicate);

        if (predicate != null) {
            List<String> fieldNames = table.rowType().getFieldNames();
            List<String> primaryKeys = table.primaryKeys();

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
                    IntStream.range(0, table.rowType().getFieldCount()).map(operator).toArray();

            this.recordFilter =
                    new PredicateFilter(
                            TypeUtils.project(table.rowType(), projection),
                            transformFieldMapping(predicate, fieldIdxToProjectionIdx).orElse(null));
        } else {
            recordFilter = null;
        }

        scan = (StreamDataTableScan) readBuilder.newStreamScan();
        scan.withSnapshotStarting();
    }

    public Iterator<InternalRow> nextBatch() throws Exception {
        try {
            return read(scan.plan());
        } catch (EndOfScanException e) {
            throw new IllegalArgumentException(
                    "TableStreamingReader does not support finished enumerator.", e);
        }
    }

    private Iterator<InternalRow> read(DataFilePlan plan) throws IOException {
        TableRead read = readBuilder.newRead();

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
