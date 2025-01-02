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
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.ApplyBitmapIndexRecordReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.io.FileIndexEvaluator;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.EmptyFileRecordReader;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.FormatReaderMapping.Builder;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.RoaringBitmap32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** A {@link SplitRead} to read raw file directly from {@link DataSplit}. */
public class RawFileSplitRead implements SplitRead<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(RawFileSplitRead.class);

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;
    private final boolean fileIndexReadEnabled;

    private RowType readRowType;
    @Nullable private List<Predicate> filters;

    public RawFileSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            boolean fileIndexReadEnabled) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.formatReaderMappings = new HashMap<>();
        this.fileIndexReadEnabled = fileIndexReadEnabled;
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
    public RawFileSplitRead withFilter(Predicate predicate) {
        if (predicate != null) {
            this.filters = splitAnd(predicate);
        }
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        if (split.beforeFiles().size() > 0) {
            LOG.info("Ignore split before files: {}", split.beforeFiles());
        }

        List<DataFileMeta> files = split.dataFiles();
        DeletionVector.Factory dvFactory =
                DeletionVector.factory(fileIO, files, split.deletionFiles().orElse(null));
        List<IOExceptionSupplier<DeletionVector>> dvFactories = new ArrayList<>();
        for (DataFileMeta file : files) {
            dvFactories.add(() -> dvFactory.create(file.fileName()).orElse(null));
        }
        return createReader(split.partition(), split.bucket(), split.dataFiles(), dvFactories);
    }

    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, bucket);
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        List<DataField> readTableFields = readRowType.getFields();
        Builder formatReaderMappingBuilder =
                new Builder(formatDiscover, readTableFields, TableSchema::fields, filters);

        for (int i = 0; i < files.size(); i++) {
            DataFileMeta file = files.get(i);
            String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
            long schemaId = file.schemaId();

            Supplier<FormatReaderMapping> formatSupplier =
                    () ->
                            formatReaderMappingBuilder.build(
                                    formatIdentifier,
                                    schema,
                                    schemaId == schema.id()
                                            ? schema
                                            : schemaManager.schema(schemaId));

            FormatReaderMapping formatReaderMapping =
                    formatReaderMappings.computeIfAbsent(
                            new FormatKey(file.schemaId(), formatIdentifier),
                            key -> formatSupplier.get());

            IOExceptionSupplier<DeletionVector> dvFactory =
                    dvFactories == null ? null : dvFactories.get(i);
            suppliers.add(
                    () ->
                            createFileReader(
                                    partition,
                                    file,
                                    dataFilePathFactory,
                                    formatReaderMapping,
                                    dvFactory));
        }

        return ConcatRecordReader.create(suppliers);
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            IOExceptionSupplier<DeletionVector> dvFactory)
            throws IOException {
        FileIndexResult fileIndexResult = null;
        if (fileIndexReadEnabled) {
            fileIndexResult =
                    FileIndexEvaluator.evaluate(
                            fileIO,
                            formatReaderMapping.getDataSchema(),
                            formatReaderMapping.getDataFilters(),
                            dataFilePathFactory,
                            file);
            if (!fileIndexResult.remain()) {
                return new EmptyFileRecordReader<>();
            }
        }

        RoaringBitmap32 deletion = null;
        DeletionVector deletionVector = dvFactory == null ? null : dvFactory.get();
        if (deletionVector instanceof BitmapDeletionVector) {
            deletion = ((BitmapDeletionVector) deletionVector).get();
        }

        if (deletion != null && fileIndexResult instanceof BitmapIndexResult) {
            RoaringBitmap32 selection = ((BitmapIndexResult) fileIndexResult).get();
            if (RoaringBitmap32.andNot(selection, deletion).isEmpty()) {
                return new EmptyFileRecordReader<>();
            }
        }

        FormatReaderContext formatReaderContext =
                new FormatReaderContext(
                        fileIO,
                        dataFilePathFactory.toPath(file),
                        file.fileSize(),
                        fileIndexResult,
                        deletion);
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        formatReaderMapping.getReaderFactory(),
                        formatReaderContext,
                        formatReaderMapping.getIndexMapping(),
                        formatReaderMapping.getCastMapping(),
                        PartitionUtils.create(formatReaderMapping.getPartitionPair(), partition));

        if (fileIndexResult instanceof BitmapIndexResult) {
            fileRecordReader =
                    new ApplyBitmapIndexRecordReader(
                            fileRecordReader, (BitmapIndexResult) fileIndexResult);
        }

        if (deletionVector != null && !deletionVector.isEmpty()) {
            return new ApplyDeletionVectorReader(fileRecordReader, deletionVector);
        }
        return fileRecordReader;
    }
}
