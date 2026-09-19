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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
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
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.EmptyFileRecordReader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.LimitRecordReader;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.FormatReaderMapping.Builder;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowTracking;

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
    private final boolean rowTrackingEnabled;
    private final boolean nestedFieldEnabled;
    private final boolean ignoreCorruptFiles;
    private final boolean ignoreLostFiles;
    private final List<DataField> changelogExtraValueFields;
    private final List<String> metadataPreserveColumns;
    private final String metadataFieldPrefix;

    private RowType readRowType;
    @Nullable private RowType outerReadRowType;
    @Nullable private int[] metadataFallbackMapping;
    @Nullable private List<Predicate> filters;
    @Nullable private TopN topN;
    @Nullable private Integer limit;
    @Nullable private ReadBatchSizer readBatchSizer;

    public RawFileSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            CoreOptions coreOptions) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.formatReaderMappings = new HashMap<>();
        this.fileIndexReadEnabled = coreOptions.fileIndexReadEnabled();
        this.ignoreCorruptFiles = coreOptions.scanIgnoreCorruptFile();
        this.ignoreLostFiles = coreOptions.scanIgnoreLostFile();
        this.rowTrackingEnabled = coreOptions.rowTrackingEnabled();
        this.nestedFieldEnabled = coreOptions.dataEvolutionNestedFieldEnabled();
        this.metadataPreserveColumns = coreOptions.changelogExposeFieldAsMetadata();
        this.metadataFieldPrefix = coreOptions.changelogMetadataFieldPrefix();
        this.changelogExtraValueFields = createChangelogExtraValueFields(schema, coreOptions);
        this.readRowType = readTypeWithMetadataDependencies(rowType);
        this.outerReadRowType = this.readRowType.equals(rowType) ? null : rowType;
        this.metadataFallbackMapping = createMetadataFallbackMapping(this.readRowType);
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
        RowType adjustedReadType = readTypeWithMetadataDependencies(readRowType);
        if (!this.readRowType.equals(adjustedReadType)) {
            formatReaderMappings.clear();
        }
        this.readRowType = adjustedReadType;
        this.outerReadRowType = adjustedReadType.equals(readRowType) ? null : readRowType;
        this.metadataFallbackMapping = createMetadataFallbackMapping(adjustedReadType);
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
    public SplitRead<InternalRow> withTopN(@Nullable TopN topN) {
        this.topN = topN;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withLimit(@Nullable Integer limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadBatchSizer(ReadBatchSizer sizer) {
        this.readBatchSizer = sizer;
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split s) throws IOException {
        if (s instanceof DataSplit) {
            DataSplit split = (DataSplit) s;
            return createReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null));
        } else {
            IncrementalSplit split = (IncrementalSplit) s;
            if (!split.beforeFiles().isEmpty()) {
                LOG.info("Ignore split before files: {}", split.beforeFiles());
            }
            return createReader(
                    split.partition(),
                    split.bucket(),
                    split.afterFiles(),
                    split.afterDeletionFiles());
        }
    }

    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            List<DeletionFile> deletionFiles)
            throws IOException {
        DeletionVector.Factory dvFactory = DeletionVector.factory(fileIO, files, deletionFiles);
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = new HashMap<>();
        for (DataFileMeta file : files) {
            dvFactories.put(file.fileName(), () -> dvFactory.create(file.fileName()).orElse(null));
        }
        return createReader(partition, bucket, files, dvFactories);
    }

    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, bucket);
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        RowType outputRowType = readRowType;
        Builder formatReaderMappingBuilder =
                createFormatReaderMappingBuilder(outputRowType, topN, limit);

        for (DataFileMeta file : files) {
            suppliers.add(
                    createFileReader(
                            partition,
                            dataFilePathFactory,
                            file,
                            formatReaderMappingBuilder,
                            outputRowType,
                            dvFactories,
                            null));
        }

        RecordReader<InternalRow> reader = ConcatRecordReader.create(suppliers);
        // Apply the final limit after deletion vectors when no later predicate can drop rows.
        if (topN == null && (filters == null || filters.isEmpty())) {
            return LimitRecordReader.limit(reader, limit);
        }
        return reader;
    }

    FileRecordReader<InternalRow> createFileReader(
            DataSplit dataSplit, RoaringBitmap32 selectedPositions) throws IOException {
        DataFileMeta dataFile = dataSplit.dataFiles().get(0);
        DeletionVector.Factory dvFactory =
                DeletionVector.factory(
                        fileIO, dataSplit.dataFiles(), dataSplit.deletionFiles().orElse(null));
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = new HashMap<>();
        dvFactories.put(
                dataFile.fileName(), () -> dvFactory.create(dataFile.fileName()).orElse(null));
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(dataSplit.partition(), dataSplit.bucket());
        RowType outputRowType = readRowType;
        return (FileRecordReader<InternalRow>)
                createFileReader(
                                dataSplit.partition(),
                                dataFilePathFactory,
                                dataFile,
                                // The caller has already selected the rows. Applying a regular
                                // TopN or limit before position filtering can drop hits.
                                createFormatReaderMappingBuilder(outputRowType, null, null),
                                outputRowType,
                                dvFactories,
                                selectedPositions)
                        .get();
    }

    private Builder createFormatReaderMappingBuilder(
            RowType outputRowType, @Nullable TopN pushDownTopN, @Nullable Integer pushDownLimit) {
        return new Builder(
                formatDiscover,
                outputRowType.getFields(),
                schema -> {
                    List<DataField> fields = new ArrayList<>(schema.fields());
                    fields.addAll(changelogExtraValueFields);
                    if (rowTrackingEnabled) {
                        // maybe file has no row id and sequence number, but in manifest entry
                        return rowTypeWithRowTracking(new RowType(fields), true, true).getFields();
                    }
                    return fields;
                },
                filters,
                pushDownTopN,
                pushDownLimit,
                nestedFieldEnabled);
    }

    private ReaderSupplier<InternalRow> createFileReader(
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            Builder formatBuilder,
            RowType outputRowType,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories,
            @Nullable RoaringBitmap32 selectedPositions) {
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

        IOExceptionSupplier<DeletionVector> dvFactory =
                dvFactories == null ? null : dvFactories.get(file.fileName());
        return () ->
                createFileReader(
                        partition,
                        file,
                        dataFilePathFactory,
                        formatReaderMapping,
                        outputRowType,
                        dvFactory,
                        selectedPositions);
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            RowType outputRowType,
            IOExceptionSupplier<DeletionVector> dvFactory,
            @Nullable RoaringBitmap32 selectedPositions)
            throws IOException {
        FileIndexResult fileIndexResult = null;
        DeletionVector deletionVector = dvFactory == null ? null : dvFactory.get();
        if (fileIndexReadEnabled) {
            fileIndexResult =
                    FileIndexEvaluator.evaluate(
                            fileIO,
                            formatReaderMapping.getDataSchema(),
                            formatReaderMapping.getDataFilters(),
                            formatReaderMapping.getTopN(),
                            formatReaderMapping.getLimit(),
                            dataFilePathFactory,
                            file,
                            deletionVector);
            if (!fileIndexResult.remain()) {
                return new EmptyFileRecordReader<>();
            }
        }

        RoaringBitmap32 selection = null;
        if (fileIndexResult instanceof BitmapIndexResult) {
            selection = ((BitmapIndexResult) fileIndexResult).get();
        }
        if (selectedPositions != null) {
            selection =
                    selection == null
                            ? selectedPositions.clone()
                            : RoaringBitmap32.and(selection, selectedPositions);
            if (selection.isEmpty()) {
                return new EmptyFileRecordReader<>();
            }
        }

        FormatReaderContext formatReaderContext =
                new FormatReaderContext(
                        fileIO,
                        dataFilePathFactory.toPath(file),
                        file.fileSize(),
                        selection,
                        readBatchSizer);
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        outputRowType,
                        formatReaderMapping.getReaderFactory(),
                        formatReaderContext,
                        ignoreCorruptFiles,
                        ignoreLostFiles,
                        formatReaderMapping.getIndexMapping(),
                        formatReaderMapping.getCastMapping(),
                        PartitionUtils.create(formatReaderMapping.getPartitionPair(), partition),
                        rowTrackingEnabled,
                        file.firstRowId(),
                        file.maxSequenceNumber(),
                        formatReaderMapping.getSystemFields());

        if (fileIndexResult instanceof BitmapIndexResult) {
            fileRecordReader =
                    new ApplyBitmapIndexRecordReader(
                            fileRecordReader, (BitmapIndexResult) fileIndexResult);
        }

        if (deletionVector != null && !deletionVector.isEmpty()) {
            fileRecordReader = new ApplyDeletionVectorReader(fileRecordReader, deletionVector);
        }
        return applyMetadataFallbackAndOuterProjection(fileRecordReader);
    }

    private RowType readTypeWithMetadataDependencies(RowType requestedReadType) {
        if (changelogExtraValueFields.isEmpty()) {
            return requestedReadType;
        }

        List<String> readFieldNames = requestedReadType.getFieldNames();
        List<DataField> dependencies = new ArrayList<>();
        for (String preserveColumn : metadataPreserveColumns) {
            String metadataName = metadataFieldPrefix + preserveColumn;
            if (readFieldNames.contains(metadataName)
                    && !readFieldNames.contains(preserveColumn)
                    && schema.logicalRowType().containsField(preserveColumn)) {
                dependencies.add(schema.logicalRowType().getField(preserveColumn));
            }
        }

        if (dependencies.isEmpty()) {
            return requestedReadType;
        }
        List<DataField> fields = new ArrayList<>(requestedReadType.getFields());
        fields.addAll(dependencies);
        return new RowType(fields);
    }

    @Nullable
    private int[] createMetadataFallbackMapping(RowType rowType) {
        if (metadataPreserveColumns.isEmpty()) {
            return null;
        }

        int[] mapping = new int[rowType.getFieldCount()];
        Arrays.fill(mapping, -1);
        boolean hasMapping = false;
        List<String> fieldNames = rowType.getFieldNames();
        for (String preserveColumn : metadataPreserveColumns) {
            int metadataIndex = fieldNames.indexOf(metadataFieldPrefix + preserveColumn);
            int physicalIndex = fieldNames.indexOf(preserveColumn);
            if (metadataIndex >= 0 && physicalIndex >= 0) {
                mapping[metadataIndex] = physicalIndex;
                hasMapping = true;
            }
        }
        return hasMapping ? mapping : null;
    }

    private static List<DataField> createChangelogExtraValueFields(
            TableSchema schema, CoreOptions options) {
        List<String> preserveColumns = options.changelogExposeFieldAsMetadata();
        if (preserveColumns.isEmpty()) {
            return java.util.Collections.emptyList();
        }

        RowType valueType = schema.logicalRowType();
        List<DataField> fields = new ArrayList<>();
        int nextId = valueType.getFields().stream().mapToInt(f -> f.id()).max().orElse(0) + 1;
        for (String preserveColumn : preserveColumns) {
            if (!valueType.containsField(preserveColumn)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' specified in '%s' not found in value type. "
                                        + "Available columns: %s",
                                preserveColumn,
                                CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key(),
                                valueType.getFieldNames()));
            }
            DataField physicalField = valueType.getField(preserveColumn);
            fields.add(
                    new DataField(
                            nextId++,
                            options.changelogMetadataFieldPrefix() + physicalField.name(),
                            physicalField.type().copy(true)));
        }
        return fields;
    }

    private FileRecordReader<InternalRow> applyMetadataFallbackAndOuterProjection(
            FileRecordReader<InternalRow> reader) {
        if (metadataFallbackMapping == null && outerReadRowType == null) {
            return reader;
        }

        final FallbackMappingRow fallbackRow =
                metadataFallbackMapping == null
                        ? null
                        : new FallbackMappingRow(metadataFallbackMapping);
        final ProjectedRow projectedRow =
                outerReadRowType == null ? null : ProjectedRow.from(outerReadRowType, readRowType);
        return new FileRecordReader<InternalRow>() {
            @Nullable
            @Override
            public FileRecordIterator<InternalRow> readBatch() throws IOException {
                FileRecordIterator<InternalRow> iterator = reader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.transform(
                        row -> {
                            InternalRow result =
                                    fallbackRow == null ? row : fallbackRow.replace(row, row);
                            return projectedRow == null ? result : projectedRow.replaceRow(result);
                        });
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
