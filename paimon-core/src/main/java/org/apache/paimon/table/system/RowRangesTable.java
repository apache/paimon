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

package org.apache.paimon.table.system;

import org.apache.paimon.Snapshot;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lists the smallest independently rewritable row-ID ranges of a Data Evolution snapshot. */
public class RowRangesTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;
    public static final String ROW_RANGES = "row_ranges";

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("partition", DataTypes.STRING().notNull())
                    .field("start_row_id", DataTypes.BIGINT().notNull())
                    .field("end_row_id", DataTypes.BIGINT().notNull())
                    .field("file_count", DataTypes.BIGINT().notNull())
                    .field("file_size_in_bytes", DataTypes.BIGINT().notNull())
                    .field("snapshot_id", DataTypes.BIGINT().notNull())
                    .build();

    private final FileStoreTable table;

    public RowRangesTable(FileStoreTable table) {
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "System table 'row_ranges' requires a Data Evolution table.");
        this.table = table;
    }

    @Override
    public String name() {
        return table.name() + SYSTEM_TABLE_SPLITTER + ROW_RANGES;
    }

    @Override
    public RowType rowType() {
        return ROW_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList("partition", "start_row_id");
    }

    @Override
    public FileIO fileIO() {
        return table.fileIO();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new RowRangesTable(table.copy(dynamicOptions));
    }

    @Override
    public InnerTableScan newScan() {
        return new ReadOnceTableScan() {
            @Nullable private Predicate partitionFilter;

            @Override
            public InnerTableScan withFilter(Predicate predicate) {
                List<Predicate> filters =
                        PredicateBuilder.pickTransformFieldMapping(
                                PredicateBuilder.splitAnd(predicate),
                                ROW_TYPE.getFieldNames(),
                                Collections.singletonList("partition"));
                partitionFilter = filters.isEmpty() ? null : PredicateBuilder.and(filters);
                return this;
            }

            @Override
            protected Plan innerPlan() {
                Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
                if (snapshot == null) {
                    return Collections::emptyList;
                }
                List<BinaryRow> partitions =
                        table.partitionKeys().isEmpty()
                                ? Collections.singletonList(BinaryRow.EMPTY_ROW)
                                : table.newSnapshotReader().withSnapshot(snapshot).partitions();
                CastExecutor<InternalRow, BinaryString> cast = partitionCast();
                List<Split> splits =
                        partitions.stream()
                                .filter(
                                        p ->
                                                partitionFilter == null
                                                        || partitionFilter.test(
                                                                GenericRow.of(cast.cast(p))))
                                .map(p -> new RowRangesSplit(snapshot, p))
                                .collect(Collectors.toList());
                return () -> splits;
            }
        };
    }

    private static class RowRangesSplit extends SingletonSplit {
        private static final long serialVersionUID = 1L;

        private final Snapshot snapshot;
        private final BinaryRow partition;

        private RowRangesSplit(Snapshot snapshot, BinaryRow partition) {
            this.snapshot = snapshot;
            this.partition = partition;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    @SuppressWarnings("unchecked")
    private CastExecutor<InternalRow, BinaryString> partitionCast() {
        return (CastExecutor<InternalRow, BinaryString>)
                CastExecutors.resolveToString(table.schema().logicalPartitionType());
    }

    @Override
    public InnerTableRead newRead() {
        return new InnerTableRead() {
            @Nullable private Predicate filter;
            @Nullable private RowType readType;

            @Override
            public InnerTableRead withFilter(Predicate predicate) {
                this.filter = predicate;
                return this;
            }

            @Override
            public InnerTableRead withReadType(RowType readType) {
                this.readType = readType;
                return this;
            }

            @Override
            public InnerTableRead withIOManager(IOManager ioManager) {
                return this;
            }

            @Override
            public RecordReader<InternalRow> createReader(Split split) {
                RowRangesSplit ranges = (RowRangesSplit) split;
                List<ManifestEntry> files =
                        table.store()
                                .newScan()
                                .withSnapshot(ranges.snapshot)
                                .withPartitionFilter(Collections.singletonList(ranges.partition))
                                .dropStats()
                                .plan()
                                .files();
                RangeHelper<ManifestEntry> helper =
                        new RangeHelper<>(entry -> entry.file().nonNullRowIdRange());
                BinaryString partition = partitionCast().cast(ranges.partition);
                Iterator<InternalRow> rows =
                        helper.mergeOverlappingRanges(files).stream()
                                .map(group -> rangeRow(partition, ranges.snapshot.id(), group))
                                .iterator();
                RecordReader<InternalRow> reader = new IteratorRecordReader<>(rows);
                if (filter != null) {
                    reader = reader.filter(filter::test);
                }
                return readType == null
                        ? reader
                        : reader.transform(ProjectedRow.from(readType, ROW_TYPE)::replaceRow);
            }
        };
    }

    private static InternalRow rangeRow(
            BinaryString partition, long snapshotId, List<ManifestEntry> group) {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        long bytes = 0;
        for (ManifestEntry entry : group) {
            DataFileMeta file = entry.file();
            Range range = file.nonNullRowIdRange();
            start = Math.min(start, range.from);
            end = Math.max(end, range.to);
            bytes += file.fileSize();
        }
        return GenericRow.of(partition, start, end, (long) group.size(), bytes, snapshotId);
    }
}
