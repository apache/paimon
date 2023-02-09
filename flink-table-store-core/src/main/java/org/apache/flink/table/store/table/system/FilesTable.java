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

package org.apache.flink.table.store.table.system;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.DataFilePathFactory;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.stats.FieldStatsConverters;
import org.apache.flink.table.store.file.utils.IteratorRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.SerializationUtils;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.table.source.snapshot.StaticDataFileSnapshotEnumerator;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.ProjectedRow;
import org.apache.flink.table.store.utils.RowDataToObjectArrayConverter;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.flink.table.store.file.catalog.Catalog.SYSTEM_TABLE_SPLITTER;
import static org.apache.flink.table.store.utils.Preconditions.checkArgument;

/** A {@link Table} for showing files of a snapshot in specific table. */
public class FilesTable implements Table {

    private static final long serialVersionUID = 1L;

    public static final String FILES = "files";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "snapshot_id", new BigIntType(false)),
                            new DataField(1, "partition", SerializationUtils.newStringType(true)),
                            new DataField(2, "bucket", new IntType(false)),
                            new DataField(3, "file_path", SerializationUtils.newStringType(false)),
                            new DataField(
                                    4, "file_format", SerializationUtils.newStringType(false)),
                            new DataField(5, "schema_id", new BigIntType(false)),
                            new DataField(6, "level", new IntType(false)),
                            new DataField(7, "record_count", new BigIntType(false)),
                            new DataField(8, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(9, "min_key", SerializationUtils.newStringType(true)),
                            new DataField(10, "max_key", SerializationUtils.newStringType(true)),
                            new DataField(
                                    11,
                                    "null_value_counts",
                                    SerializationUtils.newStringType(false)),
                            new DataField(
                                    12, "min_value_stats", SerializationUtils.newStringType(false)),
                            new DataField(
                                    13,
                                    "max_value_stats",
                                    SerializationUtils.newStringType(false))));

    private final FileStoreTable storeTable;

    public FilesTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + FILES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public Path location() {
        return storeTable.location();
    }

    @Override
    public TableScan newScan() {
        return new FilesScan(storeTable);
    }

    @Override
    public TableRead newRead() {
        return new FilesRead(new SchemaManager(storeTable.fileIO(), storeTable.location()));
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new FilesTable(storeTable.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return storeTable.fileIO();
    }

    private static class FilesScan implements TableScan {

        private final FileStoreTable storeTable;

        private FilesScan(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }

        @Override
        public TableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan plan() {
            return () -> Collections.singletonList(new FilesSplit(storeTable));
        }
    }

    private static class FilesSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final FileStoreTable storeTable;

        private FilesSplit(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }

        @Override
        public long rowCount() {
            DataTableScan.DataFilePlan plan = dataFilePlan();
            if (plan == null) {
                return 0;
            }
            return plan.splits.stream().mapToLong(s -> s.files().size()).sum();
        }

        @Nullable
        private DataTableScan.DataFilePlan dataFilePlan() {
            return StaticDataFileSnapshotEnumerator.create(storeTable, storeTable.newScan())
                    .enumerate();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilesSplit that = (FilesSplit) o;
            return Objects.equals(storeTable, that.storeTable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storeTable);
        }
    }

    private static class FilesRead implements TableRead {
        private final SchemaManager schemaManager;

        private int[][] projection;

        private FilesRead(SchemaManager schemaManager) {
            this.schemaManager = schemaManager;
        }

        @Override
        public TableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public TableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof FilesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            FilesSplit filesSplit = (FilesSplit) split;
            DataTableScan.DataFilePlan dataFilePlan = filesSplit.dataFilePlan();
            if (dataFilePlan == null) {
                return new IteratorRecordReader<>(Collections.emptyIterator());
            }

            List<Iterator<InternalRow>> iteratorList = new ArrayList<>();
            // dataFilePlan.snapshotId indicates there's no files in the table, use the newest
            // schema id directly
            FieldStatsConverters fieldStatsConverters =
                    new FieldStatsConverters(
                            sid -> schemaManager.schema(sid).fields(),
                            dataFilePlan.snapshotId == null
                                    ? filesSplit.storeTable.schema().id()
                                    : filesSplit
                                            .storeTable
                                            .snapshotManager()
                                            .snapshot(dataFilePlan.snapshotId)
                                            .schemaId());
            for (DataSplit dataSplit : dataFilePlan.splits) {
                iteratorList.add(
                        Iterators.transform(
                                dataSplit.files().iterator(),
                                v ->
                                        toRow(
                                                dataSplit,
                                                dataFilePlan.snapshotId,
                                                v,
                                                filesSplit.storeTable,
                                                fieldStatsConverters)));
            }
            Iterator<InternalRow> rows = Iterators.concat(iteratorList.iterator());
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(
                DataSplit dataSplit,
                Long snapshotId,
                DataFileMeta dataFileMeta,
                FileStoreTable storeTable,
                FieldStatsConverters fieldStatsConverters) {
            TableSchema tableSchema =
                    schemaManager.schema(
                            storeTable.snapshotManager().snapshot(snapshotId).schemaId());
            RowDataToObjectArrayConverter partitionConverter =
                    new RowDataToObjectArrayConverter(tableSchema.logicalPartitionType());

            TableSchema dataSchema = schemaManager.schema(dataFileMeta.schemaId());
            RowType keysType = dataSchema.logicalTrimmedPrimaryKeysType();
            RowDataToObjectArrayConverter keyConverter =
                    keysType.getFieldCount() > 0
                            ? new RowDataToObjectArrayConverter(
                                    dataSchema.logicalTrimmedPrimaryKeysType())
                            : new RowDataToObjectArrayConverter(dataSchema.logicalRowType());

            // Create field stats array serializer with schema evolution
            FieldStatsArraySerializer fieldStatsArraySerializer =
                    fieldStatsConverters.getOrCreate(dataSchema.id());

            // Get schema field stats for different table
            BinaryTableStats schemaFieldStats = storeTable.getSchemaFieldStats(dataFileMeta);

            // Create value stats
            List<String> fieldNames = tableSchema.fieldNames();
            FieldStats[] fieldStatsArray =
                    schemaFieldStats.fields(fieldStatsArraySerializer, dataFileMeta.rowCount());
            checkArgument(fieldNames.size() == fieldStatsArray.length);
            Map<String, Long> nullValueCounts = new TreeMap<>();
            Map<String, Object> lowerValueBounds = new TreeMap<>();
            Map<String, Object> upperValueBounds = new TreeMap<>();
            for (int i = 0; i < fieldStatsArray.length; i++) {
                String fieldName = fieldNames.get(i);
                FieldStats fieldStats = fieldStatsArray[i];
                nullValueCounts.put(fieldName, fieldStats.nullCount());
                lowerValueBounds.put(fieldName, fieldStats.minValue());
                upperValueBounds.put(fieldName, fieldStats.maxValue());
            }

            return GenericRow.of(
                    snapshotId,
                    dataSplit.partition() == null
                            ? null
                            : BinaryString.fromString(
                                    Arrays.toString(
                                            partitionConverter.convert(dataSplit.partition()))),
                    dataSplit.bucket(),
                    BinaryString.fromString(dataFileMeta.fileName()),
                    BinaryString.fromString(
                            DataFilePathFactory.formatIdentifier(dataFileMeta.fileName())),
                    dataFileMeta.schemaId(),
                    dataFileMeta.level(),
                    dataFileMeta.rowCount(),
                    dataFileMeta.fileSize(),
                    dataFileMeta.minKey().getFieldCount() <= 0
                            ? null
                            : BinaryString.fromString(
                                    Arrays.toString(keyConverter.convert(dataFileMeta.minKey()))),
                    dataFileMeta.minKey().getFieldCount() <= 0
                            ? null
                            : BinaryString.fromString(
                                    Arrays.toString(keyConverter.convert(dataFileMeta.maxKey()))),
                    BinaryString.fromString(nullValueCounts.toString()),
                    BinaryString.fromString(lowerValueBounds.toString()),
                    BinaryString.fromString(upperValueBounds.toString()));
        }
    }
}
