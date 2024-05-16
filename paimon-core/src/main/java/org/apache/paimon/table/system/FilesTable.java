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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.LazyGenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.stats.FieldStatsConverters;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing files of a snapshot in specific table. */
public class FilesTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String FILES = "files";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "bucket", new IntType(false)),
                            new DataField(2, "file_path", SerializationUtils.newStringType(false)),
                            new DataField(
                                    3, "file_format", SerializationUtils.newStringType(false)),
                            new DataField(4, "schema_id", new BigIntType(false)),
                            new DataField(5, "level", new IntType(false)),
                            new DataField(6, "record_count", new BigIntType(false)),
                            new DataField(7, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(8, "min_key", SerializationUtils.newStringType(true)),
                            new DataField(9, "max_key", SerializationUtils.newStringType(true)),
                            new DataField(
                                    10,
                                    "null_value_counts",
                                    SerializationUtils.newStringType(false)),
                            new DataField(
                                    11, "min_value_stats", SerializationUtils.newStringType(false)),
                            new DataField(
                                    12, "max_value_stats", SerializationUtils.newStringType(false)),
                            new DataField(13, "min_sequence_number", new BigIntType(true)),
                            new DataField(14, "max_sequence_number", new BigIntType(true)),
                            new DataField(15, "creation_time", DataTypes.TIMESTAMP_MILLIS())));

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
    public List<String> primaryKeys() {
        return Collections.singletonList("file_path");
    }

    @Override
    public InnerTableScan newScan() {
        return new FilesScan(storeTable);
    }

    @Override
    public InnerTableRead newRead() {
        return new FilesRead(
                new SchemaManager(storeTable.fileIO(), storeTable.location()), storeTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new FilesTable(storeTable.copy(dynamicOptions));
    }

    private static class FilesScan extends ReadOnceTableScan {

        private final FileStoreTable storeTable;

        @Nullable private LeafPredicate partitionPredicate;
        @Nullable private LeafPredicate bucketPredicate;
        @Nullable private LeafPredicate levelPredicate;

        private FilesScan(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }

        @Override
        public InnerTableScan withFilter(Predicate pushdown) {
            if (pushdown == null) {
                return this;
            }

            Map<String, LeafPredicate> leafPredicates =
                    pushdown.visit(LeafPredicateExtractor.INSTANCE);
            this.partitionPredicate = leafPredicates.get("partition");
            this.bucketPredicate = leafPredicates.get("bucket");
            this.levelPredicate = leafPredicates.get("level");
            return this;
        }

        @Override
        public Plan innerPlan() {
            // plan here, just set the result of plan to split
            TableScan.Plan plan = tablePlan();
            return () -> Collections.singletonList(new FilesSplit(plan.splits()));
        }

        private TableScan.Plan tablePlan() {
            InnerTableScan scan = storeTable.newScan();
            if (partitionPredicate != null) {
                if (partitionPredicate.function() instanceof Equal) {
                    String partitionStr = partitionPredicate.literals().get(0).toString();
                    if (partitionStr.startsWith("[")) {
                        partitionStr = partitionStr.substring(1);
                    }
                    if (partitionStr.endsWith("]")) {
                        partitionStr = partitionStr.substring(0, partitionStr.length() - 1);
                    }
                    String[] partFields = partitionStr.split(", ");
                    LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
                    List<String> partitionKeys = storeTable.partitionKeys();
                    for (int i = 0; i < partitionKeys.size(); i++) {
                        partSpec.put(partitionKeys.get(i), partFields[i]);
                    }
                    scan.withPartitionFilter(partSpec);
                }
                // TODO support range?
            }
            if (bucketPredicate != null) {
                scan.withBucketFilter(
                        bucket -> {
                            // bucket index: 1
                            return bucketPredicate.test(GenericRow.of(null, bucket));
                        });
            }
            if (levelPredicate != null) {
                scan.withLevelFilter(
                        level -> {
                            // level index: 5
                            return levelPredicate.test(
                                    GenericRow.of(null, null, null, null, null, level));
                        });
            }
            return scan.plan();
        }
    }

    private static class FilesSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final List<Split> splits;

        private FilesSplit(List<Split> splits) {
            this.splits = splits;
        }

        @Override
        public long rowCount() {
            return splits.stream()
                    .map(s -> (DataSplit) s)
                    .mapToLong(s -> s.dataFiles().size())
                    .sum();
        }

        public List<Split> splits() {
            return splits;
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
            return Objects.equals(splits, that.splits);
        }

        @Override
        public int hashCode() {
            return Objects.hash(splits);
        }
    }

    private static class FilesRead implements InnerTableRead {

        private final SchemaManager schemaManager;

        private final FileStoreTable storeTable;

        private int[][] projection;

        private FilesRead(SchemaManager schemaManager, FileStoreTable fileStoreTable) {
            this.schemaManager = schemaManager;
            this.storeTable = fileStoreTable;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof FilesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            FilesSplit filesSplit = (FilesSplit) split;
            if (filesSplit.splits().isEmpty()) {
                return new IteratorRecordReader<>(Collections.emptyIterator());
            }

            List<Iterator<InternalRow>> iteratorList = new ArrayList<>();
            // dataFilePlan.snapshotId indicates there's no files in the table, use the newest
            // schema id directly
            FieldStatsConverters fieldStatsConverters =
                    new FieldStatsConverters(
                            sid -> schemaManager.schema(sid).fields(), storeTable.schema().id());

            RowDataToObjectArrayConverter partitionConverter =
                    new RowDataToObjectArrayConverter(storeTable.schema().logicalPartitionType());

            Function<Long, RowDataToObjectArrayConverter> keyConverters =
                    new Function<Long, RowDataToObjectArrayConverter>() {
                        final Map<Long, RowDataToObjectArrayConverter> keyConverterMap =
                                new HashMap<>();

                        @Override
                        public RowDataToObjectArrayConverter apply(Long schemaId) {
                            return keyConverterMap.computeIfAbsent(
                                    schemaId,
                                    k -> {
                                        TableSchema dataSchema = schemaManager.schema(schemaId);
                                        RowType keysType =
                                                dataSchema.logicalTrimmedPrimaryKeysType();
                                        return keysType.getFieldCount() > 0
                                                ? new RowDataToObjectArrayConverter(
                                                        dataSchema.logicalTrimmedPrimaryKeysType())
                                                : new RowDataToObjectArrayConverter(
                                                        dataSchema.logicalRowType());
                                    });
                        }
                    };
            for (Split dataSplit : filesSplit.splits()) {
                iteratorList.add(
                        Iterators.transform(
                                ((DataSplit) dataSplit).dataFiles().iterator(),
                                file ->
                                        toRow(
                                                (DataSplit) dataSplit,
                                                partitionConverter,
                                                keyConverters,
                                                file,
                                                storeTable.getSchemaFieldStats(file),
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

        private LazyGenericRow toRow(
                DataSplit dataSplit,
                RowDataToObjectArrayConverter partitionConverter,
                Function<Long, RowDataToObjectArrayConverter> keyConverters,
                DataFileMeta dataFileMeta,
                BinaryTableStats tableStats,
                FieldStatsConverters fieldStatsConverters) {
            StatsLazyGetter statsGetter =
                    new StatsLazyGetter(tableStats, dataFileMeta, fieldStatsConverters);
            @SuppressWarnings("unchecked")
            Supplier<Object>[] fields =
                    new Supplier[] {
                        () ->
                                dataSplit.partition() == null
                                        ? null
                                        : BinaryString.fromString(
                                                Arrays.toString(
                                                        partitionConverter.convert(
                                                                dataSplit.partition()))),
                        dataSplit::bucket,
                        () -> BinaryString.fromString(dataFileMeta.fileName()),
                        () ->
                                BinaryString.fromString(
                                        DataFilePathFactory.formatIdentifier(
                                                dataFileMeta.fileName())),
                        dataFileMeta::schemaId,
                        dataFileMeta::level,
                        dataFileMeta::rowCount,
                        dataFileMeta::fileSize,
                        () ->
                                dataFileMeta.minKey().getFieldCount() <= 0
                                        ? null
                                        : BinaryString.fromString(
                                                Arrays.toString(
                                                        keyConverters
                                                                .apply(dataFileMeta.schemaId())
                                                                .convert(dataFileMeta.minKey()))),
                        () ->
                                dataFileMeta.maxKey().getFieldCount() <= 0
                                        ? null
                                        : BinaryString.fromString(
                                                Arrays.toString(
                                                        keyConverters
                                                                .apply(dataFileMeta.schemaId())
                                                                .convert(dataFileMeta.maxKey()))),
                        () -> BinaryString.fromString(statsGetter.nullValueCounts().toString()),
                        () -> BinaryString.fromString(statsGetter.lowerValueBounds().toString()),
                        () -> BinaryString.fromString(statsGetter.upperValueBounds().toString()),
                        dataFileMeta::minSequenceNumber,
                        dataFileMeta::maxSequenceNumber,
                        dataFileMeta::creationTime
                    };

            return new LazyGenericRow(fields);
        }
    }

    private static class StatsLazyGetter {

        private final BinaryTableStats tableStats;
        private final DataFileMeta file;
        private final FieldStatsConverters fieldStatsConverters;

        private Map<String, Long> lazyNullValueCounts;
        private Map<String, Object> lazyLowerValueBounds;
        private Map<String, Object> lazyUpperValueBounds;

        private StatsLazyGetter(
                BinaryTableStats tableStats,
                DataFileMeta file,
                FieldStatsConverters fieldStatsConverters) {
            this.tableStats = tableStats;
            this.file = file;
            this.fieldStatsConverters = fieldStatsConverters;
        }

        private void initialize() {
            FieldStatsArraySerializer serializer =
                    fieldStatsConverters.getOrCreate(file.schemaId());
            // Create value stats
            InternalRow min = serializer.evolution(tableStats.minValues());
            InternalRow max = serializer.evolution(tableStats.maxValues());
            InternalArray nullCounts =
                    serializer.evolution(tableStats.nullCounts(), file.rowCount());
            lazyNullValueCounts = new TreeMap<>();
            lazyLowerValueBounds = new TreeMap<>();
            lazyUpperValueBounds = new TreeMap<>();
            for (int i = 0; i < min.getFieldCount(); i++) {
                DataField field = fieldStatsConverters.tableDataFields().get(i);
                String name = field.name();
                DataType type = field.type();
                lazyNullValueCounts.put(
                        name, nullCounts.isNullAt(i) ? null : nullCounts.getLong(i));
                lazyLowerValueBounds.put(name, InternalRowUtils.get(min, i, type));
                lazyUpperValueBounds.put(name, InternalRowUtils.get(max, i, type));
            }
        }

        private Map<String, Long> nullValueCounts() {
            if (lazyNullValueCounts == null) {
                initialize();
            }
            return lazyNullValueCounts;
        }

        private Map<String, Object> lowerValueBounds() {
            if (lazyLowerValueBounds == null) {
                initialize();
            }
            return lazyLowerValueBounds;
        }

        private Map<String, Object> upperValueBounds() {
            if (lazyUpperValueBounds == null) {
                initialize();
            }
            return lazyUpperValueBounds;
        }
    }
}
