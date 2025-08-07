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
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.reader.CompoundFileReader;
import org.apache.paimon.reader.EmptyFileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RowLineageSplitGenerator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.FormatReaderMapping.Builder;
import org.apache.paimon.utils.IOExceptionSupplier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link SplitRead} to read raw file directly from {@link DataSplit}. */
public class FieldMergeSplitRead extends RawFileSplitRead {

    public FieldMergeSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            // TODO: Enabled file index in merge fields read
            boolean fileIndexReadEnabled) {
        super(
                fileIO,
                schemaManager,
                schema,
                rowType,
                formatDiscover,
                pathFactory,
                fileIndexReadEnabled,
                true);
    }

    @Override
    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, bucket);
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        Builder formatReaderMappingBuilder = formatBuilder();

        List<List<DataFileMeta>> splitByRowId = RowLineageSplitGenerator.split(files);
        for (List<DataFileMeta> needMergeFiles : splitByRowId) {
            if (needMergeFiles.size() == 1) {
                // No need to merge fields, just create a single file reader
                suppliers.add(
                        createFileReader(
                                partition,
                                dataFilePathFactory,
                                needMergeFiles.get(0),
                                formatReaderMappingBuilder,
                                dvFactories));

            } else {
                suppliers.add(
                        () ->
                                createCompoundFileReader(
                                        needMergeFiles,
                                        partition,
                                        dataFilePathFactory,
                                        formatReaderMappingBuilder));
            }
        }

        return ConcatRecordReader.create(suppliers);
    }

    private CompoundFileReader createCompoundFileReader(
            List<DataFileMeta> needMergeFiles,
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            Builder formatReaderMappingBuilder)
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
            int[] fieldIds = file.fieldIds();
            checkArgument(
                    fieldIds != null,
                    "Field index could not be null while construct compound reader.");
            TableSchema dataSchema = schemaManager.schema(schemaId).project(fieldIds);
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
                fileRecordReaders[i] = new EmptyFileRecordReader<>();
                continue;
            }

            Supplier<FormatReaderMapping> formatSupplier =
                    () ->
                            formatReaderMappingBuilder.build(
                                    formatIdentifier,
                                    schema,
                                    dataSchema,
                                    readFields,
                                    // TODO: enabled filter push down
                                    false);

            FormatReaderMapping formatReaderMapping =
                    formatReaderMappings.computeIfAbsent(
                            new FormatKey(
                                    file.schemaId(),
                                    formatIdentifier,
                                    readFields.stream().mapToInt(DataField::id).toArray()),
                            key -> formatSupplier.get());

            fileRecordReaders[i] =
                    createFileReader(
                            partition,
                            file,
                            dataFilePathFactory,
                            formatReaderMapping,
                            // TODO: enabled deletion vector
                            null);
        }
        return new CompoundFileReader(rowOffsets, fieldOffsets, fileRecordReaders);
    }
}
