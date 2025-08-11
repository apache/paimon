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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowLineage;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A union {@link SplitRead} to read multiple inner files to merge columns, note that this class
 * does not support filtering push down and deletion vectors, as they can interfere with the process
 * of merging columns.
 */
public class DataEvolutionSplitRead implements SplitRead<InternalRow> {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;

    protected RowType readRowType;

    public DataEvolutionSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
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
                        schema -> rowTypeWithRowLineage(schema.logicalRowType(), true).getFields(),
                        null);

        List<List<DataFileMeta>> splitByRowId = DataEvolutionSplitGenerator.split(files);
        for (List<DataFileMeta> needMergeFiles : splitByRowId) {
            if (needMergeFiles.size() == 1) {
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
        long rowCount = needMergeFiles.get(0).rowCount();
        long firstRowId = needMergeFiles.get(0).firstRowId();
        for (DataFileMeta file : needMergeFiles) {
            checkArgument(
                    file.rowCount() == rowCount,
                    "All files in a field merge split should have the same row count.");
            checkArgument(
                    file.firstRowId() == firstRowId,
                    "All files in a field merge split should have the same first row id and could not be null.");
        }

        // Init all we need to create a compound reader
        List<DataField> allReadFields = readRowType.getFields();
        RecordReader<InternalRow>[] fileRecordReaders = new RecordReader[needMergeFiles.size()];
        int[] readFieldIndex = allReadFields.stream().mapToInt(DataField::id).toArray();
        // which row the read field index belongs to
        int[] rowOffsets = new int[allReadFields.size()];
        // which field index in the reading row
        int[] fieldOffsets = new int[allReadFields.size()];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);

        for (int i = 0; i < needMergeFiles.size(); i++) {
            DataFileMeta file = needMergeFiles.get(i);
            String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
            long schemaId = file.schemaId();
            TableSchema dataSchema = schemaManager.schema(schemaId).project(file.writeCols());
            int[] fieldIds = dataSchema.fields().stream().mapToInt(DataField::id).toArray();
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
                                new FormatKey(file.schemaId(), formatIdentifier, readFieldNames),
                                key ->
                                        formatBuilder.build(
                                                formatIdentifier,
                                                schema,
                                                dataSchema,
                                                readFields,
                                                false));
                fileRecordReaders[i] =
                        createFileReader(partition, file, dataFilePathFactory, formatReaderMapping);
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
                                                : schemaManager.schema(schemaId)));
        return createFileReader(partition, file, dataFilePathFactory, formatReaderMapping);
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
}
