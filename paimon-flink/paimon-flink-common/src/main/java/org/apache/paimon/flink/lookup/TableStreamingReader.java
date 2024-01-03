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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.io.SplitsParallelReadUtil;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateFilter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_BOOTSTRAP_PARALLELISM;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A streaming reader to read table. */
public class TableStreamingReader {

    private final Table table;
    private final int[] projection;
    private final ReadBuilder readBuilder;
    @Nullable private final PredicateFilter recordFilter;
    private final StreamTableScan scan;

    private final boolean sequenceFieldEnabled;

    public TableStreamingReader(Table table, int[] projection, @Nullable Predicate predicate) {
        this.table = table;
        this.projection = projection;
        CoreOptions options = CoreOptions.fromMap(table.options());
        if (options.startupMode() != CoreOptions.StartupMode.COMPACTED_FULL) {
            table =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.SCAN_MODE.key(),
                                    CoreOptions.StartupMode.LATEST_FULL.toString()));
        }
        this.sequenceFieldEnabled =
                table.primaryKeys().size() > 0 && options.sequenceField().isPresent();

        this.readBuilder = table.newReadBuilder().withProjection(projection).withFilter(predicate);
        scan = readBuilder.newStreamScan();

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
    }

    public RecordReader<InternalRow> nextBatch(boolean useParallelism) throws Exception {
        try {
            return read(scan.plan(), useParallelism);
        } catch (EndOfScanException e) {
            throw new IllegalArgumentException(
                    "TableStreamingReader does not support finished enumerator.", e);
        }
    }

    private RecordReader<InternalRow> read(TableScan.Plan plan, boolean useParallelism)
            throws IOException {
        CoreOptions options = CoreOptions.fromMap(table.options());
        FunctionWithException<Split, RecordReader<InternalRow>, IOException> readerSupplier =
                sequenceFieldEnabled ? createKvReaderSupplier() : createReaderSupplier();

        RowType rowType = TypeUtils.project(table.rowType(), projection);
        // append sequence number at the last column.
        rowType = PrimaryKeyLookupTable.appendSequenceNumber(rowType);

        RecordReader<InternalRow> reader;
        if (useParallelism) {
            reader =
                    SplitsParallelReadUtil.parallelExecute(
                            rowType,
                            readerSupplier,
                            plan.splits(),
                            options.pageSize(),
                            new Options(table.options()).get(LOOKUP_BOOTSTRAP_PARALLELISM));
        } else {
            List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
            for (Split split : plan.splits()) {
                readers.add(() -> readerSupplier.apply(split));
            }
            reader = ConcatRecordReader.create(readers);
        }

        if (recordFilter != null) {
            reader = reader.filter(recordFilter::test);
        }
        return reader;
    }

    private FunctionWithException<Split, RecordReader<InternalRow>, IOException>
            createKvReaderSupplier() {
        KeyValueTableRead read = (KeyValueTableRead) readBuilder.newRead();
        return split -> {
            JoinedRow reused = new JoinedRow();
            return read.kvReader(split)
                    .transform(
                            kv -> {
                                reused.replace(kv.value(), GenericRow.of(kv.sequenceNumber()));
                                reused.setRowKind(kv.valueKind());
                                return reused;
                            });
        };
    }

    private FunctionWithException<Split, RecordReader<InternalRow>, IOException>
            createReaderSupplier() {
        TableRead read = readBuilder.newRead();

        return split -> {
            JoinedRow reused = new JoinedRow();
            return read.createReader(split)
                    .transform(
                            row -> {
                                reused.replace(row, GenericRow.of(-1L));
                                reused.setRowKind(row.getRowKind());
                                return reused;
                            });
        };
    }
}
