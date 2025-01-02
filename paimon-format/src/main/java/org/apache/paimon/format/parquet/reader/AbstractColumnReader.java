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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * Abstract {@link ColumnReader}. See {@link ColumnReaderImpl}, part of the code is referred from
 * Apache Spark and Apache Parquet.
 */
public abstract class AbstractColumnReader<VECTOR extends WritableColumnVector>
        implements ColumnReader<VECTOR> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractColumnReader.class);

    private final PageReader pageReader;

    /** The dictionary, if this column has dictionary encoding. */
    protected final Dictionary dictionary;

    /** Maximum definition level for this column. */
    protected final int maxDefLevel;

    protected final ColumnDescriptor descriptor;

    /** If true, the current page is dictionary encoded. */
    private boolean isCurrentPageDictionaryEncoded;

    /**
     * Helper struct to track intermediate states while reading Parquet pages in the column chunk.
     */
    private final ParquetReadState readState;

    /*
     * Input streams:
     * 1.Run length encoder to encode every data, so we have run length stream to get
     *  run length information.
     * 2.Data maybe is real data, maybe is dictionary ids which need be decode to real
     *  data from Dictionary.
     *
     * Run length stream ------> Data stream
     *                  |
     *                   ------> Dictionary ids stream
     */

    /** Run length decoder for data and dictionary. */
    RunLengthDecoder runLenDecoder;

    /** Data input stream. */
    ByteBufferInputStream dataInputStream;

    /** Dictionary decoder to wrap dictionary ids input stream. */
    private RunLengthDecoder dictionaryIdsDecoder;

    public AbstractColumnReader(ColumnDescriptor descriptor, PageReadStore pageReadStore)
            throws IOException {
        this.descriptor = descriptor;
        this.pageReader = pageReadStore.getPageReader(descriptor);
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();

        this.readState = new ParquetReadState(pageReadStore.getRowIndexes().orElse(null));

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
        /*
         * Total number of values in this column (in this row group).
         */
        long totalValueCount = pageReader.getTotalValueCount();
        if (totalValueCount == 0) {
            throw new IOException("totalValueCount == 0");
        }
    }

    protected void checkTypeName(PrimitiveType.PrimitiveTypeName expectedName) {
        PrimitiveType.PrimitiveTypeName actualName =
                descriptor.getPrimitiveType().getPrimitiveTypeName();
        checkArgument(
                actualName == expectedName,
                "Expected type name: %s, actual type name: %s",
                expectedName,
                actualName);
    }

    /** Reads `total` values from this columnReader into column. */
    @Override
    public final void readToVector(int readNumber, VECTOR vector) throws IOException {
        int rowId = 0;
        WritableIntVector dictionaryIds = null;
        if (dictionary != null) {
            dictionaryIds = vector.reserveDictionaryIds(readNumber);
        }

        readState.resetForNewBatch(readNumber);

        while (readState.rowsToReadInBatch > 0) {
            // Compute the number of values we want to read in this page.
            if (readState.valuesToReadInPage == 0) {
                int pageValueCount = readPage();
                if (pageValueCount < 0) {
                    // we've read all the pages; this could happen when we're reading a repeated
                    // list and we
                    // don't know where the list will end until we've seen all the pages.
                    break;
                }
            }

            if (readState.isFinished()) {
                break;
            }

            long pageRowId = readState.rowId;
            int leftInBatch = readState.rowsToReadInBatch;
            int leftInPage = readState.valuesToReadInPage;

            int readBatch = Math.min(leftInBatch, leftInPage);

            long rangeStart = readState.currentRangeStart();
            long rangeEnd = readState.currentRangeEnd();

            if (pageRowId < rangeStart) {
                int toSkip = (int) (rangeStart - pageRowId);
                if (toSkip >= leftInPage) { // drop page
                    pageRowId += leftInPage;
                    leftInPage = 0;
                } else {
                    if (isCurrentPageDictionaryEncoded) {
                        runLenDecoder.skipDictionaryIds(
                                toSkip, maxDefLevel, this.dictionaryIdsDecoder);
                        pageRowId += toSkip;
                        leftInPage -= toSkip;
                    } else {
                        skipBatch(toSkip);
                        pageRowId += toSkip;
                        leftInPage -= toSkip;
                    }
                }
            } else if (pageRowId > rangeEnd) {
                readState.nextRange();
            } else {
                long start = pageRowId;
                long end = Math.min(rangeEnd, pageRowId + readBatch - 1);
                int num = (int) (end - start + 1);

                if (isCurrentPageDictionaryEncoded) {
                    // Read and decode dictionary ids.
                    runLenDecoder.readDictionaryIds(
                            num,
                            dictionaryIds,
                            vector,
                            rowId,
                            maxDefLevel,
                            this.dictionaryIdsDecoder);

                    if (vector.hasDictionary() || (rowId == 0 && supportLazyDecode())) {
                        // Column vector supports lazy decoding of dictionary values so just set the
                        // dictionary.
                        // We can't do this if rowId != 0 AND the column doesn't have a dictionary
                        // (i.e.
                        // some
                        // non-dictionary encoded values have already been added).
                        vector.setDictionary(new ParquetDictionary(dictionary));
                    } else {
                        readBatchFromDictionaryIds(rowId, num, vector, dictionaryIds);
                    }
                } else {
                    if (vector.hasDictionary() && rowId != 0) {
                        // This batch already has dictionary encoded values but this new page is
                        // not.
                        // The batch
                        // does not support a mix of dictionary and not so we will decode the
                        // dictionary.
                        readBatchFromDictionaryIds(0, rowId, vector, vector.getDictionaryIds());
                    }
                    vector.setDictionary(null);
                    readBatch(rowId, num, vector);
                }
                leftInBatch -= num;
                pageRowId += num;
                leftInPage -= num;
                rowId += num;
            }
            readState.rowsToReadInBatch = leftInBatch;
            readState.valuesToReadInPage = leftInPage;
            readState.rowId = pageRowId;
        }
    }

    private int readPage() {
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
        readState.resetForNewPage(pageValueCount, pageFirstRowIndex);
        return pageValueCount;
    }

    private int readPageV1(DataPageV1 page) throws IOException {
        int pageValueCount = page.getValueCount();
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);

        // Initialize the decoders.
        if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
            throw new UnsupportedOperationException(
                    "Unsupported encoding: " + page.getDlEncoding());
        }
        int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
        this.runLenDecoder = new RunLengthDecoder(bitWidth);
        try {
            BytesInput bytes = page.getBytes();
            ByteBufferInputStream in = bytes.toInputStream();
            rlReader.initFromPage(pageValueCount, in);
            this.runLenDecoder.initFromStream(pageValueCount, in);
            prepareNewPage(page.getValueEncoding(), in, pageValueCount);
            return pageValueCount;
        } catch (IOException e) {
            throw new IOException("could not read page " + page + " in col " + descriptor, e);
        }
    }

    private int readPageV2(DataPageV2 page) throws IOException {
        int pageValueCount = page.getValueCount();

        int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
        // do not read the length from the stream. v2 pages handle dividing the page bytes.
        this.runLenDecoder = new RunLengthDecoder(bitWidth, false);
        this.runLenDecoder.initFromStream(
                pageValueCount, page.getDefinitionLevels().toInputStream());
        try {
            prepareNewPage(page.getDataEncoding(), page.getData().toInputStream(), pageValueCount);
            return pageValueCount;
        } catch (IOException e) {
            throw new IOException("could not read page " + page + " in col " + descriptor, e);
        }
    }

    private void prepareNewPage(Encoding dataEncoding, ByteBufferInputStream in, int pageValueCount)
            throws IOException {
        if (dataEncoding.usesDictionary()) {
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
                throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
            }
            this.dataInputStream = null;
            this.dictionaryIdsDecoder = new RunLengthDecoder();
            try {
                this.dictionaryIdsDecoder.initFromStream(pageValueCount, in);
            } catch (IOException e) {
                throw new IOException("could not read dictionary in col " + descriptor, e);
            }
            this.isCurrentPageDictionaryEncoded = true;
        } else {
            if (dataEncoding != Encoding.PLAIN) {
                throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
            }
            this.dictionaryIdsDecoder = null;
            LOG.debug("init from page at offset {} for length {}", in.position(), in.available());
            this.dataInputStream = in.remainingStream();
            this.isCurrentPageDictionaryEncoded = false;
        }

        afterReadPage();
    }

    final void skipDataBuffer(int length) {
        try {
            dataInputStream.skipFully(length);
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to skip " + length + " bytes", e);
        }
    }

    final ByteBuffer readDataBuffer(int length) {
        try {
            return dataInputStream.slice(length).order(ByteOrder.LITTLE_ENDIAN);
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
        }
    }

    /** After read a page, we may need some initialization. */
    protected void afterReadPage() {}

    /**
     * Support lazy dictionary ids decode. See more in {@link ParquetDictionary}. If return false,
     * we will decode all the data first.
     */
    protected boolean supportLazyDecode() {
        return true;
    }

    /** Read batch from {@link #runLenDecoder} and {@link #dataInputStream}. */
    protected abstract void readBatch(int rowId, int num, VECTOR column);

    protected abstract void skipBatch(int num);

    /**
     * Decode dictionary ids to data. From {@link #runLenDecoder} and {@link #dictionaryIdsDecoder}.
     */
    protected abstract void readBatchFromDictionaryIds(
            int rowId, int num, VECTOR column, WritableIntVector dictionaryIds);
}
