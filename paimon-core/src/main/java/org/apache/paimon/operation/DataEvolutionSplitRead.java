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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.ForceSingleBatchReader;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.FormatReaderMapping.Builder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowTracking;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A union {@link SplitRead} to read multiple inner files to merge columns, note that this class
 * does not support filtering push down and deletion vectors, as they can interfere with the process
 * of merging columns.
 */
public class DataEvolutionSplitRead implements SplitRead<InternalRow> {

    private final FileIO fileIO;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;
    private final Function<Long, TableSchema> schemaFetcher;

    protected RowType readRowType;

    public DataEvolutionSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory) {
        this.fileIO = fileIO;
        final Map<Long, TableSchema> cache = new HashMap<>();
        this.schemaFetcher =
                schemaId -> cache.computeIfAbsent(schemaId, key -> schemaManager.schema(schemaId));
        this.schema = schema;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.formatReaderMappings = new HashMap<>();
        this.readRowType = rowType;
    }

    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadType(RowType readRowType) {
        this.readRowType = readRowType;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withFilter(@Nullable Predicate predicate) {
        // TODO: Support File index push down (all conditions) and Predicate push down (only if no
        // column merge)
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        List<DataFileMeta> files = split.dataFiles();
        BinaryRow partition = split.partition();
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, split.bucket());
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        Builder formatBuilder =
                new Builder(
                        formatDiscover,
                        readRowType.getFields(),
                        schema -> rowTypeWithRowTracking(schema.logicalRowType(), true).getFields(),
                        null,
                        null,
                        null);

        List<List<DataFileMeta>> splitByRowId = DataEvolutionSplitGenerator.split(files);
        for (List<DataFileMeta> needMergeFiles : splitByRowId) {
            if (needMergeFiles.size() == 1 || readRowType.getFields().isEmpty()) {
                // No need to merge fields, just create a single file reader
                suppliers.add(
                        () ->
                                createFileReader(
                                        partition,
                                        dataFilePathFactory,
                                        needMergeFiles.get(0),
                                        formatBuilder));

            } else {
                suppliers.add(
                        () ->
                                createUnionReader(
                                        needMergeFiles,
                                        partition,
                                        dataFilePathFactory,
                                        formatBuilder));
            }
        }

        return ConcatRecordReader.create(suppliers);
    }

    private DataEvolutionFileReader createUnionReader(
            List<DataFileMeta> needMergeFiles,
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            Builder formatBuilder)
            throws IOException {
        List<FieldBunch> fieldsFiles =
                splitFieldBunches(
                        needMergeFiles,
                        file -> {
                            checkArgument(
                                    isBlobFile(file.fileName()),
                                    "Only blob file need to call this method.");
                            return schemaFetcher
                                    .apply(file.schemaId())
                                    .logicalRowType()
                                    .getField(file.writeCols().get(0))
                                    .id();
                        });

        long rowCount = fieldsFiles.get(0).rowCount();
        long firstRowId = fieldsFiles.get(0).files().get(0).firstRowId();

        for (FieldBunch bunch : fieldsFiles) {
            checkArgument(
                    bunch.rowCount() == rowCount,
                    "All files in a field merge split should have the same row count.");
            checkArgument(
                    bunch.files().get(0).firstRowId() == firstRowId,
                    "All files in a field merge split should have the same first row id and could not be null.");
        }

        // Init all we need to create a compound reader
        List<DataField> allReadFields = readRowType.getFields();
        RecordReader<InternalRow>[] fileRecordReaders = new RecordReader[fieldsFiles.size()];
        int[] readFieldIndex = allReadFields.stream().mapToInt(DataField::id).toArray();
        // which row the read field index belongs to
        int[] rowOffsets = new int[allReadFields.size()];
        // which field index in the reading row
        int[] fieldOffsets = new int[allReadFields.size()];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);

        for (int i = 0; i < fieldsFiles.size(); i++) {
            FieldBunch bunch = fieldsFiles.get(i);
            DataFileMeta firstFile = bunch.files().get(0);
            String formatIdentifier = DataFilePathFactory.formatIdentifier(firstFile.fileName());
            long schemaId = firstFile.schemaId();
            TableSchema dataSchema = schemaFetcher.apply(schemaId).project(firstFile.writeCols());
            int[] fieldIds =
                    SpecialFields.rowTypeWithRowTracking(dataSchema.logicalRowType()).getFields()
                            .stream()
                            .mapToInt(DataField::id)
                            .toArray();
            List<DataField> readFields = new ArrayList<>();
            for (int j = 0; j < readFieldIndex.length; j++) {
                for (int fieldId : fieldIds) {
                    // Check if the read field index matches the file field
                    // index
                    if (readFieldIndex[j] == fieldId) {
                        // If the row offset is not set, set it to the current
                        // file reader
                        if (rowOffsets[j] == -1) {
                            // "i" is the reader index, and "readFields.size()"
                            // is the offset the that row
                            rowOffsets[j] = i;
                            fieldOffsets[j] = readFields.size();
                            readFields.add(allReadFields.get(j));
                        }
                        break;
                    }
                }
            }

            if (readFields.isEmpty()) {
                fileRecordReaders[i] = null;
            } else {
                // create new FormatReaderMapping for read partial fields
                List<String> readFieldNames =
                        readFields.stream().map(DataField::name).collect(Collectors.toList());
                FormatReaderMapping formatReaderMapping =
                        formatReaderMappings.computeIfAbsent(
                                new FormatKey(schemaId, formatIdentifier, readFieldNames),
                                key ->
                                        formatBuilder.build(
                                                formatIdentifier,
                                                schema,
                                                dataSchema,
                                                readFields,
                                                false));
                fileRecordReaders[i] =
                        new ForceSingleBatchReader(
                                createFileReader(
                                        partition,
                                        bunch,
                                        dataFilePathFactory,
                                        formatReaderMapping));
            }
        }

        for (int i = 0; i < rowOffsets.length; i++) {
            if (rowOffsets[i] == -1) {
                checkArgument(
                        allReadFields.get(i).type().isNullable(),
                        format(
                                "Field %s is not null but can't find any file contains it.",
                                allReadFields.get(i)));
            }
        }

        return new DataEvolutionFileReader(rowOffsets, fieldOffsets, fileRecordReaders);
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            Builder formatBuilder)
            throws IOException {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        long schemaId = file.schemaId();
        FormatReaderMapping formatReaderMapping =
                formatReaderMappings.computeIfAbsent(
                        new FormatKey(file.schemaId(), formatIdentifier),
                        key ->
                                formatBuilder.build(
                                        formatIdentifier,
                                        schema,
                                        schemaId == schema.id()
                                                ? schema
                                                : schemaFetcher.apply(schemaId)));
        return createFileReader(partition, file, dataFilePathFactory, formatReaderMapping);
    }

    private RecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            FieldBunch bunch,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping)
            throws IOException {
        if (bunch.files().size() == 1) {
            return createFileReader(
                    partition, bunch.files().get(0), dataFilePathFactory, formatReaderMapping);
        }
        List<ReaderSupplier<InternalRow>> readerSuppliers = new ArrayList<>();
        for (DataFileMeta file : bunch.files()) {
            FormatReaderContext formatReaderContext =
                    new FormatReaderContext(
                            fileIO, dataFilePathFactory.toPath(file), file.fileSize(), null);
            readerSuppliers.add(
                    () ->
                            new DataFileRecordReader(
                                    schema.logicalRowType(),
                                    formatReaderMapping.getReaderFactory(),
                                    formatReaderContext,
                                    formatReaderMapping.getIndexMapping(),
                                    formatReaderMapping.getCastMapping(),
                                    PartitionUtils.create(
                                            formatReaderMapping.getPartitionPair(), partition),
                                    true,
                                    file.firstRowId(),
                                    file.maxSequenceNumber(),
                                    formatReaderMapping.getSystemFields()));
        }
        return ConcatRecordReader.create(readerSuppliers);
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping)
            throws IOException {
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(
                        fileIO, dataFilePathFactory.toPath(file), file.fileSize(), null);
        return new DataFileRecordReader(
                schema.logicalRowType(),
                formatReaderMapping.getReaderFactory(),
                formatReaderContext,
                formatReaderMapping.getIndexMapping(),
                formatReaderMapping.getCastMapping(),
                PartitionUtils.create(formatReaderMapping.getPartitionPair(), partition),
                true,
                file.firstRowId(),
                file.maxSequenceNumber(),
                formatReaderMapping.getSystemFields());
    }

    @VisibleForTesting
    public static List<FieldBunch> splitFieldBunches(
            List<DataFileMeta> needMergeFiles, Function<DataFileMeta, Integer> blobFileToFieldId) {
        List<FieldBunch> fieldsFiles = new ArrayList<>();
        Map<Integer, BlobBunch> blobBunchMap = new HashMap<>();
        long rowCount = -1;
        for (DataFileMeta file : needMergeFiles) {
            if (isBlobFile(file.fileName())) {
                int fieldId = blobFileToFieldId.apply(file);
                final long expectedRowCount = rowCount;
                blobBunchMap
                        .computeIfAbsent(fieldId, key -> new BlobBunch(expectedRowCount))
                        .add(file);
            } else {
                // Normal file, just add it to the current merge split
                fieldsFiles.add(new DataBunch(file));
                rowCount = file.rowCount();
            }
        }
        fieldsFiles.addAll(blobBunchMap.values());
        return fieldsFiles;
    }

    /** Files for partial field. */
    public interface FieldBunch {

        long rowCount();

        List<DataFileMeta> files();
    }

    private static class DataBunch implements FieldBunch {

        private final DataFileMeta dataFile;

        private DataBunch(DataFileMeta dataFile) {
            this.dataFile = dataFile;
        }

        @Override
        public long rowCount() {
            return dataFile.rowCount();
        }

        @Override
        public List<DataFileMeta> files() {
            return Collections.singletonList(dataFile);
        }
    }

    @VisibleForTesting
    static class BlobBunch implements FieldBunch {

        final List<DataFileMeta> files;
        final long expectedRowCount;

        long latestFistRowId = -1;
        long expectedNextFirstRowId = -1;
        long latestMaxSequenceNumber = -1;
        long rowCount;

        BlobBunch(long expectedRowCount) {
            this.files = new ArrayList<>();
            this.rowCount = 0;
            this.expectedRowCount = expectedRowCount;
        }

        void add(DataFileMeta file) {
            if (!isBlobFile(file.fileName())) {
                throw new IllegalArgumentException("Only blob file can be added to a blob bunch.");
            }

            if (file.firstRowId() == latestFistRowId) {
                if (file.maxSequenceNumber() >= latestMaxSequenceNumber) {
                    throw new IllegalArgumentException(
                            "Blob file with same first row id should have decreasing sequence number.");
                }
                return;
            }
            if (!files.isEmpty()) {
                long firstRowId = file.firstRowId();
                if (firstRowId < expectedNextFirstRowId) {
                    checkArgument(
                            file.maxSequenceNumber() < latestMaxSequenceNumber,
                            "Blob file with overlapping row id should have decreasing sequence number.");
                    return;
                } else if (firstRowId > expectedNextFirstRowId) {
                    throw new IllegalArgumentException(
                            "Blob file first row id should be continuous, expect "
                                    + expectedNextFirstRowId
                                    + " but got "
                                    + firstRowId);
                }
                checkArgument(
                        file.schemaId() == files.get(0).schemaId(),
                        "All files in a blob bunch should have the same schema id.");
                checkArgument(
                        file.writeCols().equals(files.get(0).writeCols()),
                        "All files in a blob bunch should have the same write columns.");
            }
            files.add(file);
            rowCount += file.rowCount();
            checkArgument(
                    rowCount <= expectedRowCount,
                    "Blob files row count exceed the expect " + expectedRowCount);
            this.latestMaxSequenceNumber = file.maxSequenceNumber();
            this.latestFistRowId = file.firstRowId();
            this.expectedNextFirstRowId = latestFistRowId + file.rowCount();
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public List<DataFileMeta> files() {
            return files;
        }
    }
}
