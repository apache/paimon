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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.ElementCountable;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.parquet.reader.ColumnReader;
import org.apache.paimon.format.parquet.reader.ParquetDecimalVector;
import org.apache.paimon.format.parquet.reader.ParquetReadState;
import org.apache.paimon.format.parquet.reader.ParquetTimestampVector;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pool;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.LIST_ELEMENT_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.LIST_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_KEY_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_REPEATED_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_VALUE_NAME;
import static org.apache.paimon.format.parquet.reader.ParquetSplitReaderUtil.buildFieldsList;
import static org.apache.paimon.format.parquet.reader.ParquetSplitReaderUtil.createColumnReader;
import static org.apache.paimon.format.parquet.reader.ParquetSplitReaderUtil.createWritableColumnVector;
import static org.apache.parquet.hadoop.UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY;

/**
 * Parquet {@link FormatReaderFactory} that reads data from the file to {@link
 * VectorizedColumnBatch} in vectorized mode.
 */
public class ParquetReaderFactory implements FormatReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderFactory.class);

    private static final String ALLOCATION_SIZE = "parquet.read.allocation.size";

    private final Options conf;

    private final RowType projectedType;
    private final String[] projectedColumnNames;
    private final DataField[] projectedFields;
    private final int batchSize;
    private final FilterCompat.Filter filter;
    private final Set<Integer> unknownFieldsIndices = new HashSet<>();

    public ParquetReaderFactory(
            Options conf, RowType projectedType, int batchSize, FilterCompat.Filter filter) {
        this.conf = conf;
        this.projectedType = projectedType;
        this.projectedColumnNames = projectedType.getFieldNames().toArray(new String[0]);
        this.projectedFields = projectedType.getFields().toArray(new DataField[0]);
        this.batchSize = batchSize;
        this.filter = filter;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(FormatReaderFactory.Context context)
            throws IOException {
        ParquetReadOptions.Builder builder =
                ParquetReadOptions.builder().withRange(0, context.fileSize());
        setReadOptions(builder);

        ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(context.fileIO(), context.filePath()),
                        builder.build(),
                        context.fileIndex());
        MessageType fileSchema = reader.getFileMetaData().getSchema();
        MessageType requestedSchema = clipParquetSchema(fileSchema);
        reader.setRequestedSchema(requestedSchema);

        checkSchema(fileSchema, requestedSchema);

        Pool<ParquetReaderBatch> poolOfBatches =
                createPoolOfBatches(context.filePath(), requestedSchema);

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(requestedSchema);
        List<ParquetField> fields =
                buildFieldsList(projectedType.getFields(), projectedType.getFieldNames(), columnIO);

        return new ParquetReader(
                reader, requestedSchema, reader.getFilteredRecordCount(), poolOfBatches, fields);
    }

    private void setReadOptions(ParquetReadOptions.Builder builder) {
        builder.useSignedStringMinMax(
                conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
        builder.useDictionaryFilter(
                conf.getBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, true));
        builder.useStatsFilter(conf.getBoolean(ParquetInputFormat.STATS_FILTERING_ENABLED, true));
        builder.useRecordFilter(conf.getBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, true));
        builder.useColumnIndexFilter(
                conf.getBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true));
        builder.usePageChecksumVerification(
                conf.getBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false));
        builder.useBloomFilter(conf.getBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true));
        builder.withMaxAllocationInBytes(conf.getInteger(ALLOCATION_SIZE, 8388608));
        String badRecordThresh = conf.getString(BAD_RECORD_THRESHOLD_CONF_KEY, null);
        if (badRecordThresh != null) {
            builder.set(BAD_RECORD_THRESHOLD_CONF_KEY, badRecordThresh);
        }
        builder.withRecordFilter(filter);
    }

    /** Clips `parquetSchema` according to `fieldNames`. */
    private MessageType clipParquetSchema(GroupType parquetSchema) {
        Type[] types = new Type[projectedColumnNames.length];
        for (int i = 0; i < projectedColumnNames.length; ++i) {
            String fieldName = projectedColumnNames[i];
            if (!parquetSchema.containsField(fieldName)) {
                LOG.warn(
                        "{} does not exist in {}, will fill the field with null.",
                        fieldName,
                        parquetSchema);
                types[i] =
                        ParquetSchemaConverter.convertToParquetType(fieldName, projectedFields[i]);
                unknownFieldsIndices.add(i);
            } else {
                Type parquetType = parquetSchema.getType(fieldName);
                types[i] = clipParquetType(projectedFields[i].type(), parquetType);
            }
        }

        return Types.buildMessage().addFields(types).named("paimon-parquet");
    }

    /** Clips `parquetType` by `readType`. */
    private Type clipParquetType(DataType readType, Type parquetType) {
        switch (readType.getTypeRoot()) {
            case ROW:
                RowType rowType = (RowType) readType;
                GroupType rowGroup = (GroupType) parquetType;
                List<Type> rowGroupFields = new ArrayList<>();
                for (DataField field : rowType.getFields()) {
                    String fieldName = field.name();
                    if (rowGroup.containsField(fieldName)) {
                        Type type = rowGroup.getType(fieldName);
                        rowGroupFields.add(clipParquetType(field.type(), type));
                    } else {
                        // todo: support nested field missing
                        throw new RuntimeException("field " + fieldName + " is missing");
                    }
                }
                return rowGroup.withNewFields(rowGroupFields);
            case MAP:
                MapType mapType = (MapType) readType;
                GroupType mapGroup = (GroupType) parquetType;
                GroupType keyValue = mapGroup.getType(MAP_REPEATED_NAME).asGroupType();
                return ConversionPatterns.mapType(
                        mapGroup.getRepetition(),
                        mapGroup.getName(),
                        MAP_REPEATED_NAME,
                        clipParquetType(mapType.getKeyType(), keyValue.getType(MAP_KEY_NAME)),
                        keyValue.containsField(MAP_VALUE_NAME)
                                ? clipParquetType(
                                        mapType.getValueType(), keyValue.getType(MAP_VALUE_NAME))
                                : null);
            case ARRAY:
                ArrayType arrayType = (ArrayType) readType;
                GroupType arrayGroup = (GroupType) parquetType;
                GroupType list = arrayGroup.getType(LIST_NAME).asGroupType();
                return ConversionPatterns.listOfElements(
                        arrayGroup.getRepetition(),
                        arrayGroup.getName(),
                        clipParquetType(
                                arrayType.getElementType(), list.getType(LIST_ELEMENT_NAME)));
            default:
                return parquetType;
        }
    }

    private void checkSchema(MessageType fileSchema, MessageType requestedSchema)
            throws IOException, UnsupportedOperationException {
        if (projectedColumnNames.length != requestedSchema.getFieldCount()) {
            throw new RuntimeException(
                    "The quality of field type is incompatible with the request schema!");
        }

        /*
         * Check that the requested schema is supported.
         */
        for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
            String[] colPath = requestedSchema.getPaths().get(i);
            if (fileSchema.containsPath(colPath)) {
                ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
                if (!fd.equals(requestedSchema.getColumns().get(i))) {
                    throw new UnsupportedOperationException("Schema evolution not supported.");
                }
            } else {
                if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
                    // Column is missing in data but the required data is non-nullable. This file is
                    // invalid.
                    throw new IOException(
                            "Required column is missing in data file. Col: "
                                    + Arrays.toString(colPath));
                }
            }
        }
    }

    private Pool<ParquetReaderBatch> createPoolOfBatches(
            Path filePath, MessageType requestedSchema) {
        // In a VectorizedColumnBatch, the dictionary will be lazied deserialized.
        // If there are multiple batches at the same time, there may be thread safety problems,
        // because the deserialization of the dictionary depends on some internal structures.
        // We need set poolCapacity to 1.
        Pool<ParquetReaderBatch> pool = new Pool<>(1);
        pool.add(createReaderBatch(filePath, requestedSchema, pool.recycler()));
        return pool;
    }

    private ParquetReaderBatch createReaderBatch(
            Path filePath,
            MessageType requestedSchema,
            Pool.Recycler<ParquetReaderBatch> recycler) {
        WritableColumnVector[] writableVectors = createWritableVectors(requestedSchema);
        VectorizedColumnBatch columnarBatch = createVectorizedColumnBatch(writableVectors);
        return createReaderBatch(filePath, writableVectors, columnarBatch, recycler);
    }

    private WritableColumnVector[] createWritableVectors(MessageType requestedSchema) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedFields.length];
        List<Type> types = requestedSchema.getFields();
        for (int i = 0; i < projectedFields.length; i++) {
            columns[i] =
                    createWritableColumnVector(
                            batchSize,
                            projectedFields[i].type(),
                            types.get(i),
                            requestedSchema.getColumns(),
                            0);
        }
        return columns;
    }

    /**
     * Create readable vectors from writable vectors. Especially for decimal, see {@link
     * ParquetDecimalVector}.
     */
    private VectorizedColumnBatch createVectorizedColumnBatch(
            WritableColumnVector[] writableVectors) {
        ColumnVector[] vectors = new ColumnVector[writableVectors.length];
        for (int i = 0; i < writableVectors.length; i++) {
            switch (projectedFields[i].type().getTypeRoot()) {
                case DECIMAL:
                    vectors[i] =
                            new ParquetDecimalVector(
                                    writableVectors[i],
                                    ((ElementCountable) writableVectors[i]).getLen());
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    vectors[i] = new ParquetTimestampVector(writableVectors[i]);
                    break;
                default:
                    vectors[i] = writableVectors[i];
            }
        }

        return new VectorizedColumnBatch(vectors);
    }

    private class ParquetReader implements FileRecordReader<InternalRow> {

        private ParquetFileReader reader;

        private final MessageType requestedSchema;

        /**
         * The total number of rows this RecordReader will eventually read. The sum of the rows of
         * all the row groups.
         */
        private final long totalRowCount;

        private final Pool<ParquetReaderBatch> pool;

        /** The number of rows that have been returned. */
        private long rowsReturned;

        /** The number of rows that have been reading, including the current in flight row group. */
        private long totalCountLoadedSoFar;

        /** The current row's position in the file. */
        private long currentRowPosition;

        private long nextRowPosition;

        private ParquetReadState currentRowGroupReadState;

        private long currentRowGroupFirstRowIndex;

        /**
         * For each request column, the reader to read this column. This is NULL if this column is
         * missing from the file, in which case we populate the attribute with NULL.
         */
        @SuppressWarnings("rawtypes")
        private ColumnReader[] columnReaders;

        private final List<ParquetField> fields;

        private ParquetReader(
                ParquetFileReader reader,
                MessageType requestedSchema,
                long totalRowCount,
                Pool<ParquetReaderBatch> pool,
                List<ParquetField> fields) {
            this.reader = reader;
            this.requestedSchema = requestedSchema;
            this.totalRowCount = totalRowCount;
            this.pool = pool;
            this.rowsReturned = 0;
            this.totalCountLoadedSoFar = 0;
            this.currentRowPosition = 0;
            this.nextRowPosition = 0;
            this.currentRowGroupFirstRowIndex = 0;
            this.fields = fields;
        }

        @Nullable
        @Override
        public ColumnarRowIterator readBatch() throws IOException {
            final ParquetReaderBatch batch = getCachedEntry();

            if (!nextBatch(batch)) {
                batch.recycle();
                return null;
            }

            return batch.convertAndGetIterator(currentRowPosition);
        }

        /** Advances to the next batch of rows. Returns false if there are no more. */
        private boolean nextBatch(ParquetReaderBatch batch) throws IOException {
            if (rowsReturned >= totalRowCount) {
                return false;
            }

            for (WritableColumnVector v : batch.writableVectors) {
                v.reset();
            }
            batch.columnarBatch.setNumRows(0);
            if (rowsReturned == totalCountLoadedSoFar) {
                readNextRowGroup();
            } else {
                currentRowPosition = nextRowPosition;
            }

            int num = getBachSize();

            for (int i = 0; i < columnReaders.length; ++i) {
                if (columnReaders[i] == null) {
                    batch.writableVectors[i].fillWithNulls();
                } else {
                    //noinspection unchecked
                    columnReaders[i].readToVector(num, batch.writableVectors[i]);
                }
            }
            rowsReturned += num;
            nextRowPosition = getNextRowPosition(num);
            batch.columnarBatch.setNumRows(num);
            return true;
        }

        private void readNextRowGroup() throws IOException {
            PageReadStore rowGroup = reader.readNextFilteredRowGroup();
            if (rowGroup == null) {
                throw new IOException(
                        "expecting more rows but reached last block. Read "
                                + rowsReturned
                                + " out of "
                                + totalRowCount);
            }

            this.currentRowGroupReadState =
                    new ParquetReadState(rowGroup.getRowIndexes().orElse(null));

            List<Type> types = requestedSchema.getFields();
            columnReaders = new ColumnReader[types.size()];
            for (int i = 0; i < types.size(); ++i) {
                if (!unknownFieldsIndices.contains(i)) {
                    columnReaders[i] =
                            createColumnReader(
                                    projectedFields[i].type(),
                                    types.get(i),
                                    requestedSchema.getColumns(),
                                    rowGroup,
                                    fields.get(i),
                                    0);
                }
            }

            totalCountLoadedSoFar += rowGroup.getRowCount();

            if (rowGroup.getRowIndexOffset().isPresent()) { // filter
                currentRowGroupFirstRowIndex = rowGroup.getRowIndexOffset().get();
                long pageIndex = 0;
                if (!this.currentRowGroupReadState.isMaxRange()) {
                    pageIndex = this.currentRowGroupReadState.currentRangeStart();
                }
                currentRowPosition = currentRowGroupFirstRowIndex + pageIndex;
            } else {
                if (reader.rowGroupsFiltered()) {
                    throw new RuntimeException(
                            "There is a bug, rowIndexOffset must be present when row groups are filtered.");
                }
                currentRowGroupFirstRowIndex = nextRowPosition;
                currentRowPosition = nextRowPosition;
            }
        }

        private int getBachSize() throws IOException {

            long rangeBatchSize = Long.MAX_VALUE;
            if (this.currentRowGroupReadState.isFinished()) {
                throw new IOException(
                        "expecting more rows but reached last page block. Read "
                                + rowsReturned
                                + " out of "
                                + totalRowCount);
            } else if (!this.currentRowGroupReadState.isMaxRange()) {
                long pageIndex = this.currentRowPosition - this.currentRowGroupFirstRowIndex;
                rangeBatchSize = this.currentRowGroupReadState.currentRangeEnd() - pageIndex + 1;
            }

            return (int)
                    Math.min(
                            batchSize,
                            Math.min(rangeBatchSize, totalCountLoadedSoFar - rowsReturned));
        }

        private long getNextRowPosition(int num) {
            if (this.currentRowGroupReadState.isMaxRange()) {
                return this.currentRowPosition + num;
            } else {
                long pageIndex = this.currentRowPosition - this.currentRowGroupFirstRowIndex;
                long nextIndex = pageIndex + num;

                if (this.currentRowGroupReadState.currentRangeEnd() < nextIndex) {
                    this.currentRowGroupReadState.nextRange();
                    nextIndex = this.currentRowGroupReadState.currentRangeStart();
                }

                return this.currentRowGroupFirstRowIndex + nextIndex;
            }
        }

        private ParquetReaderBatch getCachedEntry() throws IOException {
            try {
                return pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }
    }

    private ParquetReaderBatch createReaderBatch(
            Path filePath,
            WritableColumnVector[] writableVectors,
            VectorizedColumnBatch columnarBatch,
            Pool.Recycler<ParquetReaderBatch> recycler) {
        return new ParquetReaderBatch(filePath, writableVectors, columnarBatch, recycler);
    }

    private static class ParquetReaderBatch {

        private final WritableColumnVector[] writableVectors;
        protected final VectorizedColumnBatch columnarBatch;
        private final Pool.Recycler<ParquetReaderBatch> recycler;

        private final ColumnarRowIterator result;

        protected ParquetReaderBatch(
                Path filePath,
                WritableColumnVector[] writableVectors,
                VectorizedColumnBatch columnarBatch,
                Pool.Recycler<ParquetReaderBatch> recycler) {
            this.writableVectors = writableVectors;
            this.columnarBatch = columnarBatch;
            this.recycler = recycler;
            this.result =
                    new ColumnarRowIterator(
                            filePath, new ColumnarRow(columnarBatch), this::recycle);
        }

        public void recycle() {
            recycler.recycle(this);
        }

        public ColumnarRowIterator convertAndGetIterator(long rowNumber) {
            result.reset(rowNumber);
            return result;
        }
    }
}
