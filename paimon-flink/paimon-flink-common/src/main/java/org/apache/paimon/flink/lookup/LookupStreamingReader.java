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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.SplitsParallelReadUtil;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_BOOTSTRAP_PARALLELISM;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A streaming reader to load data into {@link LookupTable}. */
public class LookupStreamingReader {

    private final Table table;
    private final int[] projection;
    private final ReadBuilder readBuilder;
    @Nullable private final Predicate projectedPredicate;
    private final StreamTableScan scan;

    private static final List<ConfigOption<?>> TIME_TRAVEL_OPTIONS =
            Arrays.asList(
                    CoreOptions.SCAN_TIMESTAMP_MILLIS,
                    CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                    CoreOptions.SCAN_SNAPSHOT_ID,
                    CoreOptions.SCAN_TAG_NAME,
                    CoreOptions.SCAN_VERSION);

    public LookupStreamingReader(
            Table table,
            int[] projection,
            @Nullable Predicate predicate,
            Set<Integer> requireCachedBucketIds) {
        this.table = unsetTimeTravelOptions(table);
        this.projection = projection;
        this.readBuilder =
                this.table
                        .newReadBuilder()
                        .withProjection(projection)
                        .withFilter(predicate)
                        .withBucketFilter(
                                requireCachedBucketIds == null
                                        ? null
                                        : requireCachedBucketIds::contains);
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

            this.projectedPredicate =
                    transformFieldMapping(predicate, fieldIdxToProjectionIdx).orElse(null);
        } else {
            this.projectedPredicate = null;
        }
    }

    private Table unsetTimeTravelOptions(Table origin) {
        FileStoreTable fileStoreTable = (FileStoreTable) origin;
        Map<String, String> newOptions = new HashMap<>(fileStoreTable.options());
        TIME_TRAVEL_OPTIONS.stream().map(ConfigOption::key).forEach(newOptions::remove);

        CoreOptions.StartupMode startupMode = CoreOptions.fromMap(newOptions).startupMode();
        if (startupMode != CoreOptions.StartupMode.COMPACTED_FULL) {
            startupMode = CoreOptions.StartupMode.LATEST_FULL;
        }
        newOptions.put(CoreOptions.SCAN_MODE.key(), startupMode.toString());

        TableSchema newSchema = fileStoreTable.schema().copy(newOptions);
        return fileStoreTable.copy(newSchema);
    }

    public RecordReader<InternalRow> nextBatch(boolean useParallelism) throws Exception {
        List<Split> splits = scan.plan().splits();
        CoreOptions options = CoreOptions.fromMap(table.options());
        FunctionWithIOException<Split, RecordReader<InternalRow>> readerSupplier =
                split -> readBuilder.newRead().createReader(split);

        RowType readType = TypeUtils.project(table.rowType(), projection);

        RecordReader<InternalRow> reader;
        if (useParallelism) {
            reader =
                    SplitsParallelReadUtil.parallelExecute(
                            readType,
                            readerSupplier,
                            splits,
                            options.pageSize(),
                            new Options(table.options()).get(LOOKUP_BOOTSTRAP_PARALLELISM));
        } else {
            List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
            for (Split split : splits) {
                readers.add(() -> readerSupplier.apply(split));
            }
            reader = ConcatRecordReader.create(readers);
        }

        if (projectedPredicate != null) {
            reader = reader.filter(projectedPredicate::test);
        }
        return reader;
    }

    @Nullable
    public Long nextSnapshotId() {
        return scan.checkpoint();
    }
}
