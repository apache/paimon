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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.heap.HeapBooleanVector;
import org.apache.paimon.data.columnar.heap.HeapByteVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapDoubleVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.heap.HeapShortVector;
import org.apache.paimon.data.columnar.heap.HeapTimestampVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.format.parquet.position.LevelDelegation;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.IntArrayList;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/** Reader to read nested primitive column. */
public class NestedPrimitiveColumnReader implements ColumnReader<WritableColumnVector> {

    private static final Logger LOG = LoggerFactory.getLogger(NestedPrimitiveColumnReader.class);

    private final IntArrayList repetitionLevelList = new IntArrayList(0);
    private final IntArrayList definitionLevelList = new IntArrayList(0);

    private final PageReader pageReader;
    private final ColumnDescriptor descriptor;
    private final Type type;
    private final DataType dataType;
    private final boolean readRowField;
    private final boolean readMapKey;
    /** The dictionary, if this column has dictionary encoding. */
    private final ParquetDataColumnReader dictionary;
    /** Maximum definition level for this column. */
    private final int maxDefLevel;

    private final boolean isUtcTimestamp;

    /** If true, the current page is dictionary encoded. */
    private boolean isCurrentPageDictionaryEncoded;

    private int definitionLevel;
    private int repetitionLevel;

    /** Repetition/Definition/Value readers. */
    private IntIterator repetitionLevelColumn;

    private IntIterator definitionLevelColumn;
    private ParquetDataColumnReader dataColumn;

    /**
     * Helper struct to track intermediate states while reading Parquet pages in the column chunk.
     */
    protected final ParquetReadState readState;

    // flag to indicate if there is no data in parquet data page
    private boolean eof = false;

    private boolean isFirstRow = true;

    private final LastValueContainer lastValue = new LastValueContainer();

    public NestedPrimitiveColumnReader(
            ColumnDescriptor descriptor,
            PageReadStore pageReadStore,
            boolean isUtcTimestamp,
            Type parquetType,
            DataType dataType,
            boolean readRowField,
            boolean readMapKey)
            throws IOException {
        this.descriptor = descriptor;
        this.type = parquetType;
        this.pageReader = pageReadStore.getPageReader(descriptor);
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();
        this.isUtcTimestamp = isUtcTimestamp;
        this.dataType = dataType;
        this.readRowField = readRowField;
        this.readMapKey = readMapKey;

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

        this.readState = new ParquetReadState(pageReadStore.getRowIndexes().orElse(null));

        if (dictionaryPage != null) {
            try {
                this.dictionary =
                        ParquetDataColumnReaderFactory.getDataColumnReaderByTypeOnDictionary(
                                dictionaryPage
                                        .getEncoding()
                                        .initDictionary(descriptor, dictionaryPage),
                                isUtcTimestamp);
                this.isCurrentPageDictionaryEncoded = true;
            } catch (IOException e) {
                throw new IOException(
                        String.format("Could not decode the dictionary for %s", descriptor), e);
            }
        } else {
            this.dictionary = null;
            this.isCurrentPageDictionaryEncoded = false;
        }
    }

    // This won't call, will actually call readAndNewVector
    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
        throw new UnsupportedOperationException("This function should no be called.");
    }

    public WritableColumnVector readAndNewVector(int readNumber, WritableColumnVector vector)
            throws IOException {
        if (isFirstRow) {
            if (!readValue()) {
                return vector;
            }
            isFirstRow = false;
        }

        List<Object> valueList = new ArrayList<>();

        int valueIndex = collectDataFromParquetPage(readNumber, valueList);

        return fillColumnVector(valueIndex, valueList);
    }

    private int collectDataFromParquetPage(int total, List<Object> valueList) throws IOException {
        int valueIndex = 0;
        // repeated type need two loops to read data.

        readState.resetForNewBatch(total);

        while (!eof && readState.rowsToReadInBatch > 0) {

            if (readState.isFinished()) { // finished to read
                eof = true;
                break;
            }

            long pageRowId = readState.rowId;
            long rangeStart = readState.currentRangeStart();
            long rangeEnd = readState.currentRangeEnd();

            if (pageRowId > rangeEnd) {
                readState.nextRange();
                continue;
            }

            boolean needFilterSkip = pageRowId < rangeStart;

            do {

                if (!lastValue.shouldSkip && !needFilterSkip) {
                    valueList.add(lastValue.value);
                    valueIndex++;
                }
            } while (readValue() && (repetitionLevel != 0));

            if (pageRowId == readState.rowId) {
                readState.rowId = readState.rowId + 1;
            }

            if (!needFilterSkip) {
                readState.rowsToReadInBatch = readState.rowsToReadInBatch - 1;
            }
        }

        return valueIndex;
    }

    public LevelDelegation getLevelDelegation() {
        int[] repetition = repetitionLevelList.toArray();
        int[] definition = definitionLevelList.toArray();
        repetitionLevelList.clear();
        definitionLevelList.clear();
        repetitionLevelList.add(repetitionLevel);
        definitionLevelList.add(definitionLevel);
        return new LevelDelegation(repetition, definition);
    }

    /**
     * An ARRAY[ARRAY[INT]] Example: {[[0, null], [1], [], null], [], null} => [5, 4, 5, 3, 2, 1, 0]
     *
     * <ul>
     *   <li>definitionLevel == maxDefLevel => not null value
     *   <li>definitionLevel == maxDefLevel - 1 => null value
     *   <li>definitionLevel == maxDefLevel - 2 => empty set, skip
     *   <li>definitionLevel == maxDefLevel - 3 => null set, skip
     *   <li>definitionLevel == maxDefLevel - 4 => empty outer set, skip
     *   <li>definitionLevel == maxDefLevel - 5 => null outer set, skip
     *   <li>... skip
     * </ul>
     *
     * <p>When (definitionLevel <= maxDefLevel - 2) we skip the value because children ColumnVector
     * for OrcArrayColumnVector don't contain empty and null set value. Stay consistent here.
     *
     * <p>For MAP, the value vector is the same as ARRAY. But the key vector isn't nullable, so just
     * read value when definitionLevel == maxDefLevel.
     *
     * <p>For ROW, RowColumnVector still get null value when definitionLevel == maxDefLevel - 2.
     */
    private boolean readValue() throws IOException {
        int left = readPageIfNeed();
        if (left > 0) {
            // get the values of repetition and definitionLevel
            readAndSaveRepetitionAndDefinitionLevels();
            // read the data if it isn't null
            if (definitionLevel == maxDefLevel) {
                if (isCurrentPageDictionaryEncoded) {
                    int dictionaryId = dataColumn.readValueDictionaryId();
                    lastValue.setValue(dictionaryDecodeValue(dataType, dictionaryId));
                } else {
                    lastValue.setValue(readPrimitiveTypedRow(dataType));
                }
            } else {
                if (readMapKey) {
                    lastValue.skip();
                } else {
                    if (definitionLevel == maxDefLevel - 1) {
                        // null value inner set
                        lastValue.setValue(null);
                    } else if (definitionLevel == maxDefLevel - 2 && readRowField) {
                        lastValue.setValue(null);
                    } else {
                        // current set is empty or null
                        lastValue.skip();
                    }
                }
            }
            return true;
        } else {
            eof = true;
            return false;
        }
    }

    private void readAndSaveRepetitionAndDefinitionLevels() {
        // get the values of repetition and definitionLevel
        repetitionLevel = repetitionLevelColumn.nextInt();
        definitionLevel = definitionLevelColumn.nextInt();
        readState.valuesToReadInPage = readState.valuesToReadInPage - 1;
        repetitionLevelList.add(repetitionLevel);
        definitionLevelList.add(definitionLevel);
    }

    private int readPageIfNeed() throws IOException {
        // Compute the number of values we want to read in this page.
        if (readState.valuesToReadInPage == 0) {
            int pageValueCount = readPage();
            // 返回当前 page 的数据量
            if (pageValueCount < 0) {
                // we've read all the pages; this could happen when we're reading a repeated list
                // and we
                // don't know where the list will end until we've seen all the pages.
                return -1;
            }
        }
        return readState.valuesToReadInPage;
    }

    private Object readPrimitiveTypedRow(DataType category) {
        switch (category.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dataColumn.readBytes();
            case BOOLEAN:
                return dataColumn.readBoolean();
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
            case INTEGER:
                return dataColumn.readInteger();
            case TINYINT:
                return dataColumn.readTinyInt();
            case SMALLINT:
                return dataColumn.readSmallInt();
            case BIGINT:
                return dataColumn.readLong();
            case FLOAT:
                return dataColumn.readFloat();
            case DOUBLE:
                return dataColumn.readDouble();
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return dataColumn.readInteger();
                    case INT64:
                        return dataColumn.readLong();
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        return dataColumn.readBytes();
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return readTimestamp(((TimestampType) category).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return readTimestamp(((LocalZonedTimestampType) category).getPrecision());
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private Timestamp readTimestamp(int precision) {
        if (precision <= 3) {
            return dataColumn.readMillsTimestamp();
        } else if (precision <= 6) {
            return dataColumn.readMicrosTimestamp();
        } else {
            return dataColumn.readNanosTimestamp();
        }
    }

    private Object dictionaryDecodeValue(DataType category, Integer dictionaryValue) {
        if (dictionaryValue == null) {
            return null;
        }

        switch (category.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dictionary.readBytes(dictionaryValue);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                return dictionary.readInteger(dictionaryValue);
            case BOOLEAN:
                return dictionary.readBoolean(dictionaryValue) ? 1 : 0;
            case DOUBLE:
                return dictionary.readDouble(dictionaryValue);
            case FLOAT:
                return dictionary.readFloat(dictionaryValue);
            case TINYINT:
                return dictionary.readTinyInt(dictionaryValue);
            case SMALLINT:
                return dictionary.readSmallInt(dictionaryValue);
            case BIGINT:
                return dictionary.readLong(dictionaryValue);
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return dictionary.readInteger(dictionaryValue);
                    case INT64:
                        return dictionary.readLong(dictionaryValue);
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        return dictionary.readBytes(dictionaryValue);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return dictionaryReadTimestamp(
                        ((TimestampType) category).getPrecision(), dictionaryValue);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return dictionaryReadTimestamp(
                        ((LocalZonedTimestampType) category).getPrecision(), dictionaryValue);
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private Timestamp dictionaryReadTimestamp(int precision, int dictionaryValue) {
        if (precision <= 3) {
            return dictionary.readMillsTimestamp(dictionaryValue);
        } else if (precision <= 6) {
            return dictionary.readMicrosTimestamp(dictionaryValue);
        } else {
            return dictionary.readNanosTimestamp(dictionaryValue);
        }
    }

    private WritableColumnVector fillColumnVector(int total, List valueList) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                HeapBytesVector heapBytesVector = new HeapBytesVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    byte[] src = ((List<byte[]>) valueList).get(i);
                    if (src == null) {
                        heapBytesVector.setNullAt(i);
                    } else {
                        heapBytesVector.appendBytes(i, src, 0, src.length);
                    }
                }
                return heapBytesVector;
            case BOOLEAN:
                HeapBooleanVector heapBooleanVector = new HeapBooleanVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapBooleanVector.setNullAt(i);
                    } else {
                        heapBooleanVector.vector[i] = ((List<Boolean>) valueList).get(i);
                    }
                }
                return heapBooleanVector;
            case TINYINT:
                HeapByteVector heapByteVector = new HeapByteVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapByteVector.setNullAt(i);
                    } else {
                        heapByteVector.vector[i] =
                                (byte) ((List<Integer>) valueList).get(i).intValue();
                    }
                }
                return heapByteVector;
            case SMALLINT:
                HeapShortVector heapShortVector = new HeapShortVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapShortVector.setNullAt(i);
                    } else {
                        heapShortVector.vector[i] =
                                (short) ((List<Integer>) valueList).get(i).intValue();
                    }
                }
                return heapShortVector;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                HeapIntVector heapIntVector = new HeapIntVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapIntVector.setNullAt(i);
                    } else {
                        heapIntVector.vector[i] = ((List<Integer>) valueList).get(i);
                    }
                }
                return heapIntVector;
            case FLOAT:
                HeapFloatVector heapFloatVector = new HeapFloatVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapFloatVector.setNullAt(i);
                    } else {
                        heapFloatVector.vector[i] = ((List<Float>) valueList).get(i);
                    }
                }
                return heapFloatVector;
            case BIGINT:
                HeapLongVector heapLongVector = new HeapLongVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapLongVector.setNullAt(i);
                    } else {
                        heapLongVector.vector[i] = ((List<Long>) valueList).get(i);
                    }
                }
                return heapLongVector;
            case DOUBLE:
                HeapDoubleVector heapDoubleVector = new HeapDoubleVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapDoubleVector.setNullAt(i);
                    } else {
                        heapDoubleVector.vector[i] = ((List<Double>) valueList).get(i);
                    }
                }
                return heapDoubleVector;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                HeapTimestampVector heapTimestampVector = new HeapTimestampVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapTimestampVector.setNullAt(i);
                    } else {
                        heapTimestampVector.setTimestamp(i, ((List<Timestamp>) valueList).get(i));
                    }
                }
                return heapTimestampVector;
            case DECIMAL:
                PrimitiveType.PrimitiveTypeName primitiveTypeName =
                        descriptor.getPrimitiveType().getPrimitiveTypeName();
                switch (primitiveTypeName) {
                    case INT32:
                        HeapIntVector phiv = new HeapIntVector(total);
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                phiv.setNullAt(i);
                            } else {
                                phiv.vector[i] = ((List<Integer>) valueList).get(i);
                            }
                        }
                        return new ParquetDecimalVector(phiv, total);
                    case INT64:
                        HeapLongVector phlv = new HeapLongVector(total);
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                phlv.setNullAt(i);
                            } else {
                                phlv.vector[i] = ((List<Long>) valueList).get(i);
                            }
                        }
                        return new ParquetDecimalVector(phlv, total);
                    default:
                        HeapBytesVector phbv = getHeapBytesVector(total, valueList);
                        return new ParquetDecimalVector(phbv, total);
                }
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private static HeapBytesVector getHeapBytesVector(int total, List valueList) {
        HeapBytesVector phbv = new HeapBytesVector(total);
        for (int i = 0; i < valueList.size(); i++) {
            byte[] src = ((List<byte[]>) valueList).get(i);
            if (valueList.get(i) == null) {
                phbv.setNullAt(i);
            } else {
                phbv.appendBytes(i, src, 0, src.length);
            }
        }
        return phbv;
    }

    protected int readPage() {
        DataPage page = pageReader.readPage();

        if (page == null) {
            return -1;
        }

        long pageFirstRowIndex = page.getFirstRowIndex().orElse(0L);

        int pageValueCount =
                page.accept(
                        new DataPage.Visitor<Integer>() {
                            @Override
                            public Integer visit(DataPageV1 dataPageV1) {
                                return readPageV1(dataPageV1);
                            }

                            @Override
                            public Integer visit(DataPageV2 dataPageV2) {
                                return readPageV2(dataPageV2);
                            }
                        });
        readState.resetForNewPage(pageValueCount, pageFirstRowIndex);
        return pageValueCount;
    }

    private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount)
            throws IOException {
        //        this.pageValueCount = valueCount;
        //        this.endOfPageValueCount = valuesRead + pageValueCount;
        if (dataEncoding.usesDictionary()) {
            this.dataColumn = null;
            if (dictionary == null) {
                throw new IOException(
                        String.format(
                                "Could not read page in col %s because the dictionary was missing for encoding %s.",
                                descriptor, dataEncoding));
            }
            dataColumn =
                    ParquetDataColumnReaderFactory.getDataColumnReaderByType(
                            dataEncoding.getDictionaryBasedValuesReader(
                                    descriptor, VALUES, dictionary.getDictionary()),
                            isUtcTimestamp);
            this.isCurrentPageDictionaryEncoded = true;
        } else {
            dataColumn =
                    ParquetDataColumnReaderFactory.getDataColumnReaderByType(
                            dataEncoding.getValuesReader(descriptor, VALUES), isUtcTimestamp);
            this.isCurrentPageDictionaryEncoded = false;
        }

        try {
            dataColumn.initFromPage(valueCount, in);
        } catch (IOException e) {
            throw new IOException(String.format("Could not read page in col %s.", descriptor), e);
        }
    }

    private int readPageV1(DataPageV1 page) {
        int pageValueCount = page.getValueCount();
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
        this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
        this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
        try {
            BytesInput bytes = page.getBytes();
            LOG.debug("Page size {}  bytes and {} records.", bytes.size(), pageValueCount);
            ByteBufferInputStream in = bytes.toInputStream();
            LOG.debug("Reading repetition levels at {}.", in.position());
            rlReader.initFromPage(pageValueCount, in);
            LOG.debug("Reading definition levels at {}.", in.position());
            dlReader.initFromPage(pageValueCount, in);
            LOG.debug("Reading data at {}.", in.position());
            initDataReader(page.getValueEncoding(), in, pageValueCount);
            return pageValueCount;
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read page %s in col %s.", page, descriptor), e);
        }
    }

    private int readPageV2(DataPageV2 page) {
        int pageValueCount = page.getValueCount();
        this.repetitionLevelColumn =
                newRLEIterator(descriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        this.definitionLevelColumn =
                newRLEIterator(descriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        try {
            LOG.debug(
                    "Page data size {} bytes and {} records.",
                    page.getData().size(),
                    pageValueCount);
            initDataReader(page.getDataEncoding(), page.getData().toInputStream(), pageValueCount);
            return pageValueCount;
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read page %s in col %s.", page, descriptor), e);
        }
    }

    private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
        try {
            if (maxLevel == 0) {
                return new NullIntIterator();
            }
            return new RLEIntIterator(
                    new RunLengthBitPackingHybridDecoder(
                            BytesUtils.getWidthFromMaxInt(maxLevel),
                            new ByteArrayInputStream(bytes.toByteArray())));
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read levels in page for col %s.", descriptor), e);
        }
    }

    /** Utility interface to abstract over different way to read ints with different encodings. */
    interface IntIterator {
        int nextInt();
    }

    /** Reading int from {@link ValuesReader}. */
    protected static final class ValuesReaderIntIterator implements IntIterator {
        ValuesReader delegate;

        public ValuesReaderIntIterator(ValuesReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextInt() {
            return delegate.readInteger();
        }
    }

    /** Reading int from {@link RunLengthBitPackingHybridDecoder}. */
    protected static final class RLEIntIterator implements IntIterator {
        RunLengthBitPackingHybridDecoder delegate;

        public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextInt() {
            try {
                return delegate.readInt();
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }
    }

    /** Reading zero always. */
    protected static final class NullIntIterator implements IntIterator {
        @Override
        public int nextInt() {
            return 0;
        }
    }

    private static class LastValueContainer {
        protected boolean shouldSkip;
        protected Object value;

        protected void setValue(Object value) {
            this.value = value;
            this.shouldSkip = false;
        }

        protected void skip() {
            this.shouldSkip = true;
        }
    }
}
