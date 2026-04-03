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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.LazyGenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafBinaryFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/**
 * A {@link Table} for showing key ranges and file path information for each data file, supporting
 * diagnosis of data distribution and Global Index (PIP-41) coverage.
 */
public class FileKeyRangesTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String FILE_KEY_RANGES = "file_key_ranges";

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
                            new DataField(10, "first_row_id", new BigIntType(true))));

    private final FileStoreTable storeTable;

    public FileKeyRangesTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + FILE_KEY_RANGES;
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
    public FileIO fileIO() {
        return storeTable.fileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new FileKeyRangesScan(storeTable);
    }

    @Override
    public InnerTableRead newRead() {
        return new FileKeyRangesRead(storeTable.schemaManager(), storeTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new FileKeyRangesTable(storeTable.copy(dynamicOptions));
    }

    private static class FileKeyRangesScan extends ReadOnceTableScan {

        @Nullable private LeafPredicate partitionPredicate;
        @Nullable private LeafPredicate bucketPredicate;
        @Nullable private LeafPredicate levelPredicate;

        private final FileStoreTable fileStoreTable;

        public FileKeyRangesScan(FileStoreTable fileStoreTable) {
            this.fileStoreTable = fileStoreTable;
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
            SnapshotReader snapshotReader = fileStoreTable.newSnapshotReader();
            if (partitionPredicate != null) {
                List<String> partitionKeys = fileStoreTable.partitionKeys();
                RowType partitionType = fileStoreTable.schema().logicalPartitionType();
                if (partitionPredicate.function() instanceof Equal) {
                    LinkedHashMap<String, String> partSpec =
                            parsePartitionSpec(
                                    partitionPredicate.literals().get(0).toString(), partitionKeys);
                    if (partSpec == null) {
                        return Collections::emptyList;
                    }
                    snapshotReader.withPartitionFilter(partSpec);
                } else if (partitionPredicate.function() instanceof In) {
                    List<Predicate> orPredicates = new ArrayList<>();
                    PredicateBuilder partBuilder = new PredicateBuilder(partitionType);
                    for (Object literal : partitionPredicate.literals()) {
                        LinkedHashMap<String, String> partSpec =
                                parsePartitionSpec(literal.toString(), partitionKeys);
                        if (partSpec == null) {
                            continue;
                        }
                        List<Predicate> andPredicates = new ArrayList<>();
                        for (int i = 0; i < partitionKeys.size(); i++) {
                            Object value =
                                    TypeUtils.castFromString(
                                            partSpec.get(partitionKeys.get(i)),
                                            partitionType.getTypeAt(i));
                            andPredicates.add(partBuilder.equal(i, value));
                        }
                        orPredicates.add(PredicateBuilder.and(andPredicates));
                    }
                    if (!orPredicates.isEmpty()) {
                        snapshotReader.withPartitionFilter(PredicateBuilder.or(orPredicates));
                    }
                } else if (partitionPredicate.function() instanceof LeafBinaryFunction) {
                    LinkedHashMap<String, String> partSpec =
                            parsePartitionSpec(
                                    partitionPredicate.literals().get(0).toString(), partitionKeys);
                    if (partSpec != null) {
                        PredicateBuilder partBuilder = new PredicateBuilder(partitionType);
                        List<Predicate> predicates = new ArrayList<>();
                        for (int i = 0; i < partitionKeys.size(); i++) {
                            Object value =
                                    TypeUtils.castFromString(
                                            partSpec.get(partitionKeys.get(i)),
                                            partitionType.getTypeAt(i));
                            predicates.add(
                                    new LeafPredicate(
                                            partitionPredicate.function(),
                                            partitionType.getTypeAt(i),
                                            i,
                                            partitionKeys.get(i),
                                            Collections.singletonList(value)));
                        }
                        snapshotReader.withPartitionFilter(PredicateBuilder.and(predicates));
                    }
                }
            }

            return () ->
                    snapshotReader.partitions().stream()
                            .map(p -> new FilesTable.FilesSplit(p, bucketPredicate, levelPredicate))
                            .collect(Collectors.toList());
        }

        @Nullable
        private LinkedHashMap<String, String> parsePartitionSpec(
                String partitionStr, List<String> partitionKeys) {
            if (partitionStr.startsWith("{")) {
                partitionStr = partitionStr.substring(1);
            }
            if (partitionStr.endsWith("}")) {
                partitionStr = partitionStr.substring(0, partitionStr.length() - 1);
            }
            String[] partFields = partitionStr.split(", ");
            if (partitionKeys.size() != partFields.length) {
                return null;
            }
            LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
            for (int i = 0; i < partitionKeys.size(); i++) {
                partSpec.put(partitionKeys.get(i), partFields[i]);
            }
            return partSpec;
        }
    }

    private static class FileKeyRangesRead implements InnerTableRead {

        private final SchemaManager schemaManager;

        private final FileStoreTable storeTable;

        private RowType readType;

        private FileKeyRangesRead(SchemaManager schemaManager, FileStoreTable fileStoreTable) {
            this.schemaManager = schemaManager;
            this.storeTable = fileStoreTable;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof FilesTable.FilesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            FilesTable.FilesSplit filesSplit = (FilesTable.FilesSplit) split;
            List<Split> splits = filesSplit.splits(storeTable);
            if (splits.isEmpty()) {
                return new IteratorRecordReader<>(Collections.emptyIterator());
            }

            @SuppressWarnings("unchecked")
            CastExecutor<InternalRow, BinaryString> partitionCastExecutor =
                    (CastExecutor<InternalRow, BinaryString>)
                            CastExecutors.resolveToString(
                                    storeTable.schema().logicalPartitionType());

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

            List<Iterator<InternalRow>> iteratorList = new ArrayList<>();
            for (Split dataSplit : splits) {
                iteratorList.add(
                        Iterators.transform(
                                ((DataSplit) dataSplit).dataFiles().iterator(),
                                file ->
                                        toRow(
                                                (DataSplit) dataSplit,
                                                partitionCastExecutor,
                                                keyConverters,
                                                file)));
            }
            Iterator<InternalRow> rows = Iterators.concat(iteratorList.iterator());
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, FileKeyRangesTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private LazyGenericRow toRow(
                DataSplit dataSplit,
                CastExecutor<InternalRow, BinaryString> partitionCastExecutor,
                Function<Long, RowDataToObjectArrayConverter> keyConverters,
                DataFileMeta file) {
            @SuppressWarnings("unchecked")
            Supplier<Object>[] fields =
                    new Supplier[] {
                        () ->
                                dataSplit.partition() == null
                                        ? null
                                        : partitionCastExecutor.cast(dataSplit.partition()),
                        dataSplit::bucket,
                        () ->
                                BinaryString.fromString(
                                        file.externalPath()
                                                .orElse(
                                                        dataSplit.bucketPath()
                                                                + "/"
                                                                + file.fileName())),
                        () ->
                                BinaryString.fromString(
                                        DataFilePathFactory.formatIdentifier(file.fileName())),
                        file::schemaId,
                        file::level,
                        file::rowCount,
                        file::fileSize,
                        () ->
                                file.minKey().getFieldCount() <= 0
                                        ? null
                                        : BinaryString.fromString(
                                                Arrays.toString(
                                                        keyConverters
                                                                .apply(file.schemaId())
                                                                .convert(file.minKey()))),
                        () ->
                                file.maxKey().getFieldCount() <= 0
                                        ? null
                                        : BinaryString.fromString(
                                                Arrays.toString(
                                                        keyConverters
                                                                .apply(file.schemaId())
                                                                .convert(file.maxKey()))),
                        file::firstRowId
                    };

            return new LazyGenericRow(fields);
        }
    }
}
