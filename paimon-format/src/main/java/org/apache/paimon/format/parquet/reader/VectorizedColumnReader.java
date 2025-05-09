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

import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.types.DataType;

import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;

/** Decoder to return values from a single column. */
public class VectorizedColumnReader {
    /** The dictionary, if this column has dictionary encoding. */
    private final Dictionary dictionary;

    /** If true, the current page is dictionary encoded. */
    private boolean isCurrentPageDictionaryEncoded;

    /** Value readers. */
    private ValuesReader dataColumn;

    /** Vectorized RLE decoder for definition levels. */
    private VectorizedRleValuesReader defColumn;

    /** Vectorized RLE decoder for repetition levels. */
    private VectorizedRleValuesReader repColumn;

    /**
     * Helper struct to track intermediate states while reading Parquet pages in the column chunk.
     */
    private final ParquetReadState readState;

    /**
     * The index for the first row in the current page, among all rows across all pages in the
     * column chunk for this reader. If there is no column index, the value is 0.
     */
    private long pageFirstRowIndex;

    private final PageReader pageReader;
    private final ColumnDescriptor descriptor;
    private final ParsedVersion writerVersion;

    public VectorizedColumnReader(
            ColumnDescriptor descriptor,
            boolean isRequired,
            PageReadStore pageReadStore,
            ParsedVersion writerVersion)
            throws IOException {
        this.descriptor = descriptor;
        this.pageReader = pageReadStore.getPageReader(descriptor);
        this.readState =
                new ParquetReadState(
                        descriptor, isRequired, pageReadStore.getRowIndexes().orElse(null));

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            try {
                this.dictionary =
                        dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
                this.isCurrentPageDictionaryEncoded = true;
            } catch (IOException e) {
                throw new IOException("could not decode the dictionary for " + descriptor, e);
            }
        } else {
            this.dictionary = null;
            this.isCurrentPageDictionaryEncoded = false;
        }
        if (pageReader.getTotalValueCount() == 0) {
            throw new IOException("totalValueCount == 0");
        }
        this.writerVersion = writerVersion;
    }

    private boolean isLazyDecodingSupported(
            PrimitiveType.PrimitiveTypeName typeName, DataType paimonType) {
        return true;
    }

    /** Reads `total` rows from this columnReader into column. */
    void readBatch(
            int total,
            DataType type,
            WritableColumnVector column,
            WritableIntVector repetitionLevels,
            WritableIntVector definitionLevels)
            throws IOException {
        WritableIntVector dictionaryIds = null;
        ParquetVectorUpdater updater = ParquetVectorUpdaterFactory.getUpdater(descriptor, type);

        if (dictionary != null) {
            // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be
            // used to
            // decode all previous dictionary encoded pages if we ever encounter a non-dictionary
            // encoded
            // page.
            dictionaryIds = column.reserveDictionaryIds(total);
        }
        readState.resetForNewBatch(total);
        while (readState.rowsToReadInBatch > 0 || !readState.lastListCompleted) {
            if (readState.valuesToReadInPage == 0) {
                int pageValueCount = readPage();
                if (pageValueCount < 0) {
                    // we've read all the pages; this could happen when we're reading a repeated
                    // list and we
                    // don't know where the list will end until we've seen all the pages.
                    break;
                }
                readState.resetForNewPage(pageValueCount, pageFirstRowIndex);
            }
            PrimitiveType.PrimitiveTypeName typeName =
                    descriptor.getPrimitiveType().getPrimitiveTypeName();
            if (isCurrentPageDictionaryEncoded) {
                // Save starting offset in case we need to decode dictionary IDs.
                int startOffset = readState.valueOffset;
                // Save starting row index so we can check if we need to eagerly decode dict ids
                // later
                long startRowId = readState.rowId;

                // Read and decode dictionary ids.
                if (readState.maxRepetitionLevel == 0) {
                    defColumn.readIntegers(
                            readState,
                            dictionaryIds,
                            column,
                            definitionLevels,
                            (VectorizedValuesReader) dataColumn);
                } else {
                    repColumn.readIntegersRepeated(
                            readState,
                            repetitionLevels,
                            defColumn,
                            definitionLevels,
                            dictionaryIds,
                            column,
                            (VectorizedValuesReader) dataColumn);
                }

                // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post
                // process
                // the values to add microseconds precision.
                if (column.hasDictionary()
                        || (startRowId == pageFirstRowIndex
                                && isLazyDecodingSupported(typeName, type))) {
                    column.setDictionary(new ParquetDictionary(dictionary));
                } else {
                    updater.decodeDictionaryIds(
                            readState.valueOffset - startOffset,
                            startOffset,
                            column,
                            dictionaryIds,
                            dictionary);
                }
            } else {
                if (column.hasDictionary() && readState.valueOffset != 0) {
                    // This batch already has dictionary encoded values but this new page is not.
                    // The batch
                    // does not support a mix of dictionary and not so we will decode the
                    // dictionary.
                    updater.decodeDictionaryIds(
                            readState.valueOffset, 0, column, dictionaryIds, dictionary);
                }
                column.setDictionary(null);
                VectorizedValuesReader valuesReader = (VectorizedValuesReader) dataColumn;
                if (readState.maxRepetitionLevel == 0) {
                    defColumn.readBatch(readState, column, definitionLevels, valuesReader, updater);
                } else {
                    repColumn.readBatchRepeated(
                            readState,
                            repetitionLevels,
                            defColumn,
                            definitionLevels,
                            column,
                            valuesReader,
                            updater);
                }
            }
        }
    }

    private int readPage() {
        DataPage page = pageReader.readPage();
        if (page == null) {
            return -1;
        }
        this.pageFirstRowIndex = page.getFirstRowIndex().orElse(0L);

        return page.accept(
                new DataPage.Visitor<Integer>() {
                    @Override
                    public Integer visit(DataPageV1 dataPageV1) {
                        try {
                            return readPageV1(dataPageV1);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public Integer visit(DataPageV2 dataPageV2) {
                        try {
                            return readPageV2(dataPageV2);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private void initDataReader(int pageValueCount, Encoding dataEncoding, ByteBufferInputStream in)
            throws IOException {
        ValuesReader previousReader = this.dataColumn;
        if (dataEncoding.usesDictionary()) {
            this.dataColumn = null;
            if (dictionary == null) {
                throw new IOException(
                        "could not read page in col "
                                + descriptor
                                + " as the dictionary was missing for encoding "
                                + dataEncoding);
            }
            @SuppressWarnings("deprecation")
            Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
            if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
                throw new UnsupportedOperationException(
                        "error: _LEGACY_ERROR_TEMP_3189, encoding: " + dataEncoding);
            }
            this.dataColumn = new VectorizedRleValuesReader();
            this.isCurrentPageDictionaryEncoded = true;
        } else {
            this.dataColumn = getValuesReader(dataEncoding);
            this.isCurrentPageDictionaryEncoded = false;
        }

        try {
            dataColumn.initFromPage(pageValueCount, in);
        } catch (IOException e) {
            throw new IOException("could not read page in col " + descriptor, e);
        }
        // for PARQUET-246 (See VectorizedDeltaByteArrayReader.setPreviousValues)
        if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding)
                && previousReader instanceof RequiresPreviousReader) {
            // previousReader can only be set if reading sequentially
            ((RequiresPreviousReader) dataColumn).setPreviousReader(previousReader);
        }
    }

    private ValuesReader getValuesReader(Encoding encoding) {
        switch (encoding) {
            case PLAIN:
                return new VectorizedPlainValuesReader();
            case DELTA_BYTE_ARRAY:
                return new VectorizedDeltaByteArrayReader();
            case DELTA_LENGTH_BYTE_ARRAY:
                return new VectorizedDeltaLengthByteArrayReader();
            case DELTA_BINARY_PACKED:
                return new VectorizedDeltaBinaryPackedReader();
            case RLE:
                {
                    PrimitiveType.PrimitiveTypeName typeName =
                            this.descriptor.getPrimitiveType().getPrimitiveTypeName();
                    // RLE encoding only supports boolean type `Values`, and  `bitwidth` is always
                    // 1.
                    if (typeName == BOOLEAN) {
                        return new VectorizedRleValuesReader(1);
                    } else {
                        throw new RuntimeException(
                                "error: _LEGACY_ERROR_TEMP_3190, typeName: " + typeName.toString());
                    }
                }
            default:
                throw new RuntimeException("error: _LEGACY_ERROR_TEMP_3189, encoding: " + encoding);
        }
    }

    private int readPageV1(DataPageV1 page) throws IOException {
        if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
            throw new RuntimeException(
                    "error: _LEGACY_ERROR_TEMP_3189, encoding " + page.getDlEncoding().toString());
        }

        int pageValueCount = page.getValueCount();

        int rlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxRepetitionLevel());
        this.repColumn = new VectorizedRleValuesReader(rlBitWidth);

        int dlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
        this.defColumn = new VectorizedRleValuesReader(dlBitWidth);

        try {
            BytesInput bytes = page.getBytes();
            ByteBufferInputStream in = bytes.toInputStream();

            repColumn.initFromPage(pageValueCount, in);
            defColumn.initFromPage(pageValueCount, in);
            initDataReader(pageValueCount, page.getValueEncoding(), in);
            return pageValueCount;
        } catch (IOException e) {
            throw new IOException("could not read page " + page + " in col " + descriptor, e);
        }
    }

    private int readPageV2(DataPageV2 page) throws IOException {
        int pageValueCount = page.getValueCount();

        // do not read the length from the stream. v2 pages handle dividing the page bytes.
        int rlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxRepetitionLevel());
        repColumn = new VectorizedRleValuesReader(rlBitWidth, false);
        repColumn.initFromPage(pageValueCount, page.getRepetitionLevels().toInputStream());

        int dlBitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
        defColumn = new VectorizedRleValuesReader(dlBitWidth, false);
        defColumn.initFromPage(pageValueCount, page.getDefinitionLevels().toInputStream());

        try {
            initDataReader(pageValueCount, page.getDataEncoding(), page.getData().toInputStream());
            return pageValueCount;
        } catch (IOException e) {
            throw new IOException("could not read page " + page + " in col " + descriptor, e);
        }
    }
}
