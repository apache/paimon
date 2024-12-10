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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketSpec;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing summary of the specific table. */
public class SummaryTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String SUMMARY = "summary";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            // table summary.
                            new DataField(0, "table_name", SerializationUtils.newStringType(false)),
                            new DataField(1, "table_path", SerializationUtils.newStringType(false)),
                            new DataField(2, "table_type", SerializationUtils.newStringType(false)),
                            new DataField(
                                    3, "primary_keys", SerializationUtils.newStringType(true)),
                            new DataField(
                                    4, "partition_keys", SerializationUtils.newStringType(true)),
                            new DataField(5, "comment", SerializationUtils.newStringType(true)),
                            new DataField(6, "schema_id", new BigIntType(false)),
                            new DataField(
                                    7, "bucket_mode", SerializationUtils.newStringType(false)),
                            new DataField(8, "bucket_num", new IntType(false)),
                            new DataField(
                                    9, "snapshot_range", SerializationUtils.newStringType(true)),
                            new DataField(
                                    10,
                                    "latest_commit_kind",
                                    SerializationUtils.newStringType(true)),
                            new DataField(11, "tag_nums", new IntType(false)),
                            new DataField(12, "options", SerializationUtils.newStringType(true)),

                            // partition summary.
                            new DataField(13, "partition_nums", new IntType(false)),
                            new DataField(
                                    14,
                                    "max_record_partition",
                                    SerializationUtils.newStringType(true)),
                            new DataField(
                                    15,
                                    "max_filenums_partition",
                                    SerializationUtils.newStringType(true)),
                            new DataField(
                                    16,
                                    "max_filesize_partition",
                                    SerializationUtils.newStringType(true)),

                            // datafile summary.
                            new DataField(17, "file_nums", new BigIntType(false)),
                            new DataField(18, "file_size", new BigIntType(false)),
                            new DataField(19, "estimate_record_count", new BigIntType(false)),
                            new DataField(20, "last_commit_time", new TimestampType(true, 3))));

    private final FileStoreTable storeTable;

    public SummaryTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public InnerTableScan newScan() {
        return new SummaryScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new SummaryRead(storeTable);
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + SUMMARY;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("table_name");
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SummaryTable(storeTable.copy(dynamicOptions));
    }

    private static class SummaryScan extends ReadOnceTableScan {

        @Override
        protected Plan innerPlan() {
            return () -> Collections.singletonList(new SummarySplit());
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // no need to filter.
            return this;
        }
    }

    private static class SummarySplit extends SingletonSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            return obj != null && getClass() == obj.getClass();
        }
    }

    private static class SummaryRead implements InnerTableRead {

        private RowType readType;

        private final FileStoreTable storeTable;

        private SummaryRead(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            Preconditions.checkArgument(
                    split instanceof SummarySplit, "Unsupported split: " + split.getClass());

            Iterator<InternalRow> iterator =
                    Collections.singletonList(toRow(storeTable)).iterator();

            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(readType, PartitionsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }

        public InternalRow toRow(FileStoreTable storeTable) {
            GenericRow result = new GenericRow(21);
            setTableSummary(result);
            setPartitionSummary(result);
            setDataFilesSummary(result);

            Snapshot latestSnapshot = storeTable.snapshotManager().latestSnapshot();
            result.setField(
                    20,
                    latestSnapshot != null
                            ? Timestamp.fromLocalDateTime(
                                    LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(latestSnapshot.timeMillis()),
                                            ZoneId.systemDefault()))
                            : null);

            return result;
        }

        private void setTableSummary(GenericRow result) {
            List<String> pk = storeTable.primaryKeys();
            BucketSpec bucketSpec = storeTable.bucketSpec();
            Snapshot latestSnapshot = storeTable.snapshotManager().latestSnapshot();

            result.setField(0, BinaryString.fromString(storeTable.fullName()));
            result.setField(1, BinaryString.fromString(storeTable.location().toString()));
            result.setField(
                    2,
                    BinaryString.fromString(pk.isEmpty() ? "Append table" : "Primary key table"));

            result.setField(3, toJson(pk));
            result.setField(4, toJson(storeTable.partitionKeys()));
            result.setField(5, BinaryString.fromString(storeTable.comment().orElse("")));
            result.setField(6, storeTable.schema().id());
            result.setField(7, BinaryString.fromString(bucketSpec.getBucketMode().toString()));
            result.setField(8, bucketSpec.getNumBuckets());
            result.setField(
                    9,
                    BinaryString.fromString(
                            String.format(
                                    "[%s,%s]",
                                    storeTable.snapshotManager().earliestSnapshotId(),
                                    latestSnapshot != null ? latestSnapshot.id() : null)));
            result.setField(
                    10,
                    latestSnapshot != null
                            ? BinaryString.fromString(latestSnapshot.commitKind().name())
                            : null);
            result.setField(11, storeTable.tagManager().allTagNames().size());
            result.setField(12, toJson(storeTable.options()));
        }

        private void setPartitionSummary(GenericRow result) {
            RowDataToObjectArrayConverter converter =
                    new RowDataToObjectArrayConverter(storeTable.schema().logicalPartitionType());

            List<PartitionEntry> partitionEntryList = storeTable.newScan().listPartitionEntries();
            result.setField(13, partitionEntryList.size());
            if (storeTable.partitionKeys().isEmpty()) {
                result.setField(14, null);
                result.setField(15, null);
                result.setField(16, null);
                return;
            }

            Pair<BinaryRow, Long> maxRecord = Pair.of(null, 0L);
            Pair<BinaryRow, Long> maxFileNums = Pair.of(null, 0L);
            Pair<BinaryRow, Long> maxFilesize = Pair.of(null, 0L);

            for (PartitionEntry partitionEntry : partitionEntryList) {
                if (partitionEntry.recordCount() > maxRecord.getRight()) {
                    maxRecord.setRight(partitionEntry.recordCount());
                    maxRecord.setLeft(partitionEntry.partition());
                }
                if (partitionEntry.fileCount() > maxFileNums.getRight()) {
                    maxFileNums.setRight(partitionEntry.fileCount());
                    maxFileNums.setLeft(partitionEntry.partition());
                }
                if (partitionEntry.fileSizeInBytes() > maxFilesize.getRight()) {
                    maxFilesize.setRight(partitionEntry.fileSizeInBytes());
                    maxFilesize.setLeft(partitionEntry.partition());
                }
            }

            String partitionInfoFormat = "{\"partition\":\"%s\",\"%s\": %s}";

            result.setField(
                    14,
                    BinaryString.fromString(
                            String.format(
                                    partitionInfoFormat,
                                    Arrays.toString(converter.convert(maxRecord.getLeft())),
                                    "record_count",
                                    maxRecord.getRight())));
            result.setField(
                    15,
                    BinaryString.fromString(
                            String.format(
                                    partitionInfoFormat,
                                    Arrays.toString(converter.convert(maxFileNums.getLeft())),
                                    "file_nums",
                                    maxFileNums.getRight())));
            result.setField(
                    16,
                    BinaryString.fromString(
                            String.format(
                                    partitionInfoFormat,
                                    Arrays.toString(converter.convert(maxFilesize.getLeft())),
                                    "file_size",
                                    maxFilesize.getRight())));
        }

        private void setDataFilesSummary(GenericRow result) {
            // If the latest snapshot type is COMPACT, this will be exact, otherwise it is slightly
            // larger than the actual value, because these data are not merged.
            long estimateRecordCount = 0L;
            long fileNums = 0L;
            long fileSizeInBytes = 0L;
            for (DataSplit dataSplit : storeTable.newSnapshotReader().read().dataSplits()) {
                estimateRecordCount += dataSplit.rowCount();
                fileNums += dataSplit.dataFiles().size();
                for (DataFileMeta dataFile : dataSplit.dataFiles()) {
                    fileSizeInBytes += dataFile.fileSize();
                }
            }
            result.setField(17, fileNums);
            result.setField(18, fileSizeInBytes);
            result.setField(19, estimateRecordCount);
        }

        private BinaryString toJson(Object obj) {
            return BinaryString.fromString(JsonSerdeUtil.toFlatJson(obj));
        }
    }
}
