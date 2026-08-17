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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexFormat.FileIndexMeta;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
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
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.PartitionPredicateHelper;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing file indexes of data files in a snapshot. */
public class FileIndexesTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String FILE_INDEXES = "file_indexes";

    private static final String EMBEDDED = "EMBEDDED";
    private static final String FILE = "FILE";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "bucket", new IntType(false)),
                            new DataField(2, "file_path", SerializationUtils.newStringType(false)),
                            new DataField(3, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(4, "record_count", new BigIntType(false)),
                            new DataField(5, "schema_id", new BigIntType(false)),
                            new DataField(
                                    6, "column_name", SerializationUtils.newStringType(false)),
                            new DataField(7, "index_type", SerializationUtils.newStringType(false)),
                            new DataField(
                                    8, "storage_type", SerializationUtils.newStringType(false)),
                            new DataField(
                                    9, "index_file_path", SerializationUtils.newStringType(true)),
                            new DataField(10, "index_size_in_bytes", new BigIntType(false)),
                            new DataField(
                                    11, "index_container_size_in_bytes", new BigIntType(false)),
                            new DataField(12, "is_empty", new BooleanType(false))));

    private final FileStoreTable storeTable;

    public FileIndexesTable(FileStoreTable storeTable) {
        this.storeTable = storeTable;
    }

    @Override
    public String name() {
        return storeTable.name() + SYSTEM_TABLE_SPLITTER + FILE_INDEXES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList("file_path", "column_name", "index_type");
    }

    @Override
    public FileIO fileIO() {
        return storeTable.fileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new FileIndexesScan(storeTable);
    }

    @Override
    public InnerTableRead newRead() {
        return new FileIndexesRead(storeTable);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new FileIndexesTable(storeTable.copy(dynamicOptions));
    }

    private static class FileIndexesScan extends ReadOnceTableScan {

        @Nullable private LeafPredicate partitionPredicate;
        @Nullable private LeafPredicate bucketPredicate;

        private final FileStoreTable storeTable;

        private FileIndexesScan(FileStoreTable storeTable) {
            this.storeTable = storeTable;
        }

        @Override
        public InnerTableScan withFilter(Predicate pushdown) {
            if (pushdown == null) {
                return this;
            }

            Map<String, LeafPredicate> leafPredicates =
                    pushdown.visit(LeafPredicateExtractor.INSTANCE);
            partitionPredicate = leafPredicates.get("partition");
            bucketPredicate = leafPredicates.get("bucket");
            return this;
        }

        @Override
        public Plan innerPlan() {
            SnapshotReader snapshotReader = storeTable.newSnapshotReader();
            boolean hasResults =
                    PartitionPredicateHelper.applyPartitionFilter(
                            snapshotReader,
                            partitionPredicate,
                            storeTable.partitionKeys(),
                            storeTable.schema().logicalPartitionType());
            if (!hasResults) {
                return Collections::emptyList;
            }

            return () ->
                    snapshotReader.partitions().stream()
                            .map(
                                    partition ->
                                            // Keep file inventory planning aligned with FilesTable.
                                            new FilesTable.FilesSplit(
                                                    partition, bucketPredicate, null))
                            .collect(Collectors.toList());
        }
    }

    private static class FileIndexesRead implements InnerTableRead {

        private static final Set<String> FILE_PUSHDOWN_FIELDS = Collections.singleton("file_path");

        private final FileStoreTable storeTable;
        private final DataFilePathFactories pathFactories;

        @Nullable private Predicate filePredicate;
        @Nullable private Predicate predicate;
        @Nullable private RowType readType;

        private FileIndexesRead(FileStoreTable storeTable) {
            this.storeTable = storeTable;
            this.pathFactories = new DataFilePathFactories(storeTable.store().pathFactory());
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            List<Predicate> remaining = new ArrayList<>(PredicateBuilder.splitAnd(predicate));
            List<Predicate> filePredicates =
                    remaining.stream()
                            .filter(p -> onlyContainsFields(p, FILE_PUSHDOWN_FIELDS))
                            .collect(Collectors.toList());
            remaining.removeAll(filePredicates);

            this.filePredicate =
                    filePredicates.isEmpty() ? null : PredicateBuilder.and(filePredicates);
            this.predicate = remaining.isEmpty() ? null : PredicateBuilder.and(remaining);
            return this;
        }

        private static boolean onlyContainsFields(Predicate predicate, Set<String> fields) {
            if (predicate instanceof CompoundPredicate) {
                return ((CompoundPredicate) predicate)
                        .children().stream().allMatch(p -> onlyContainsFields(p, fields));
            }
            return fields.containsAll(((LeafPredicate) predicate).fieldNames());
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

            List<Split> dataSplits = ((FilesTable.FilesSplit) split).splits(storeTable);
            if (dataSplits.isEmpty()) {
                return new IteratorRecordReader<>(Collections.emptyIterator());
            }

            @SuppressWarnings("unchecked")
            CastExecutor<InternalRow, BinaryString> partitionCastExecutor =
                    (CastExecutor<InternalRow, BinaryString>)
                            CastExecutors.resolveToString(
                                    storeTable.schema().logicalPartitionType());

            Iterator<InternalRow> iterator = splitRows(dataSplits, partitionCastExecutor);
            if (predicate != null) {
                iterator = Iterators.filter(iterator, predicate::test);
            }
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(readType, FileIndexesTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }

        private Iterator<InternalRow> splitRows(
                List<Split> dataSplits,
                CastExecutor<InternalRow, BinaryString> partitionCastExecutor) {
            Iterator<Iterator<InternalRow>> splitRows =
                    Iterators.transform(
                            dataSplits.iterator(),
                            split -> fileRows((DataSplit) split, partitionCastExecutor));
            return Iterators.concat(splitRows);
        }

        private Iterator<InternalRow> fileRows(
                DataSplit dataSplit,
                CastExecutor<InternalRow, BinaryString> partitionCastExecutor) {
            DataFilePathFactory dataFilePathFactory =
                    pathFactories.get(dataSplit.partition(), dataSplit.bucket());
            Iterator<Iterator<InternalRow>> fileRows =
                    Iterators.transform(
                            dataSplit.dataFiles().iterator(),
                            file -> {
                                BinaryString filePath = filePath(dataSplit, file);
                                if (filePredicate != null && !testFilePath(filePath)) {
                                    return Collections.emptyIterator();
                                }
                                return indexRows(
                                                dataSplit,
                                                file,
                                                filePath,
                                                dataFilePathFactory,
                                                partitionCastExecutor)
                                        .iterator();
                            });
            return Iterators.concat(fileRows);
        }

        private boolean testFilePath(BinaryString filePath) {
            GenericRow row = new GenericRow(TABLE_TYPE.getFieldCount());
            row.setField(2, filePath);
            return filePredicate.test(row);
        }

        private List<InternalRow> indexRows(
                DataSplit dataSplit,
                DataFileMeta file,
                BinaryString filePath,
                DataFilePathFactory dataFilePathFactory,
                CastExecutor<InternalRow, BinaryString> partitionCastExecutor) {
            byte[] embeddedIndex = file.embeddedIndex();
            if (embeddedIndex != null) {
                try (FileIndexFormat.Reader reader =
                        FileIndexFormat.createMetadataReader(
                                new ByteArraySeekableStream(embeddedIndex))) {
                    return toRows(
                            dataSplit,
                            file,
                            filePath,
                            partitionCastExecutor,
                            reader.indexMetas(),
                            EMBEDDED,
                            null,
                            embeddedIndex.length);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Failed to read file index metadata from " + filePath + ".", e);
                } catch (RuntimeException e) {
                    throw fileIndexReadException(filePath.toString(), e);
                }
            }

            List<String> indexFiles =
                    file.extraFiles().stream()
                            .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            if (indexFiles.isEmpty()) {
                return Collections.emptyList();
            }
            if (indexFiles.size() > 1) {
                throw new IllegalStateException(
                        "Found more than one file index for data file "
                                + file.fileName()
                                + ": "
                                + String.join(", ", indexFiles));
            }

            Path indexPath = dataFilePathFactory.toAlignedPath(indexFiles.get(0), file);
            try {
                long containerSize = storeTable.fileIO().getFileStatus(indexPath).getLen();
                try (FileIndexFormat.Reader reader =
                        FileIndexFormat.createMetadataReader(
                                storeTable.fileIO().newInputStream(indexPath))) {
                    return toRows(
                            dataSplit,
                            file,
                            filePath,
                            partitionCastExecutor,
                            reader.indexMetas(),
                            FILE,
                            BinaryString.fromString(indexPath.toString()),
                            containerSize);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to read file index metadata from " + indexPath + ".", e);
            } catch (RuntimeException e) {
                throw fileIndexReadException(indexPath.toString(), e);
            }
        }

        private static RuntimeException fileIndexReadException(
                String indexLocation, RuntimeException exception) {
            String message = "Failed to read file index metadata from " + indexLocation + ".";
            if (exception.getCause() instanceof IOException) {
                return new UncheckedIOException(message, (IOException) exception.getCause());
            }
            return new RuntimeException(message, exception);
        }

        private static List<InternalRow> toRows(
                DataSplit dataSplit,
                DataFileMeta file,
                BinaryString filePath,
                CastExecutor<InternalRow, BinaryString> partitionCastExecutor,
                List<FileIndexMeta> indexMetas,
                String storageType,
                @Nullable BinaryString indexFilePath,
                long containerSize) {
            BinaryString partition =
                    dataSplit.partition() == null
                            ? null
                            : partitionCastExecutor.cast(dataSplit.partition());
            List<InternalRow> rows = new ArrayList<>(indexMetas.size());
            for (FileIndexMeta indexMeta : indexMetas) {
                rows.add(
                        GenericRow.of(
                                partition,
                                dataSplit.bucket(),
                                filePath,
                                file.fileSize(),
                                file.rowCount(),
                                file.schemaId(),
                                BinaryString.fromString(indexMeta.columnName()),
                                BinaryString.fromString(indexMeta.indexType()),
                                BinaryString.fromString(storageType),
                                indexFilePath,
                                (long) indexMeta.sizeInBytes(),
                                containerSize,
                                indexMeta.empty()));
            }
            return rows;
        }

        private static BinaryString filePath(DataSplit dataSplit, DataFileMeta file) {
            return BinaryString.fromString(
                    file.externalPath().orElse(dataSplit.bucketPath() + "/" + file.fileName()));
        }
    }
}
