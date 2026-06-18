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

package org.apache.parquet.hadoop;

import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Record writer with per-column compression.
 *
 * <p>NOTE: The file is adapted from Apache parquet's InternalParquetRecordWriter.
 */
class ColumnCompressionRecordWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnCompressionRecordWriter.class);

    private final ParquetFileWriter parquetFileWriter;
    private final WriteSupport<T> writeSupport;
    private final MessageType schema;
    private final Map<String, String> extraMetaData;
    private long rowGroupSizeThreshold;
    private final int rowGroupRecordCountThreshold;
    private long nextRowGroupSize;
    private final CompressionCodecFactory codecFactory;
    private final ColumnCompressionCodecs compressionCodecs;
    private final boolean validating;
    private final ParquetProperties props;

    private boolean closed;

    private long recordCount = 0;
    private long recordCountForNextMemCheck;
    private long lastRowGroupEndPos = 0;

    private ColumnWriteStore columnStore;
    private ColumnCompressionPageWriteStore pageStore;
    private BloomFilterWriteStore bloomFilterWriteStore;
    private RecordConsumer recordConsumer;

    private InternalFileEncryptor fileEncryptor;
    private int rowGroupOrdinal;
    private boolean aborted;

    ColumnCompressionRecordWriter(
            ParquetFileWriter parquetFileWriter,
            WriteSupport<T> writeSupport,
            MessageType schema,
            Map<String, String> extraMetaData,
            long rowGroupSize,
            CompressionCodecFactory codecFactory,
            ColumnCompressionCodecs compressionCodecs,
            boolean validating,
            ParquetProperties props) {
        this.parquetFileWriter = parquetFileWriter;
        this.writeSupport = Objects.requireNonNull(writeSupport, "writeSupport cannot be null");
        this.schema = schema;
        this.extraMetaData = extraMetaData;
        this.rowGroupSizeThreshold = rowGroupSize;
        this.rowGroupRecordCountThreshold = props.getRowGroupRowCountLimit();
        this.nextRowGroupSize = rowGroupSizeThreshold;
        this.codecFactory = codecFactory;
        this.compressionCodecs = compressionCodecs;
        this.validating = validating;
        this.props = props;
        this.fileEncryptor = parquetFileWriter.getEncryptor();
        this.rowGroupOrdinal = 0;
        initStore();
        recordCountForNextMemCheck = props.getMinRowCountForPageSizeCheck();
    }

    public ParquetMetadata getFooter() {
        return parquetFileWriter.getFooter();
    }

    private void initStore() {
        ColumnCompressionPageWriteStore columnCompressionPageWriteStore =
                new ColumnCompressionPageWriteStore(
                        codecFactory,
                        compressionCodecs,
                        schema,
                        props.getAllocator(),
                        props.getColumnIndexTruncateLength(),
                        props.getPageWriteChecksumEnabled(),
                        fileEncryptor,
                        rowGroupOrdinal);
        pageStore = columnCompressionPageWriteStore;
        bloomFilterWriteStore = columnCompressionPageWriteStore;

        columnStore = props.newColumnWriteStore(schema, pageStore, bloomFilterWriteStore);
        MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
        this.recordConsumer = columnIO.getRecordWriter(columnStore);
        writeSupport.prepareForWrite(recordConsumer);
    }

    public void close() throws IOException, InterruptedException {
        if (!closed) {
            try {
                if (aborted) {
                    return;
                }
                flushRowGroupToStore();
                FinalizedWriteContext finalWriteContext = writeSupport.finalizeWrite();
                Map<String, String> finalMetadata = new HashMap<>(extraMetaData);
                String modelName = writeSupport.getName();
                if (modelName != null) {
                    finalMetadata.put(ParquetWriter.OBJECT_MODEL_NAME_PROP, modelName);
                }
                finalMetadata.putAll(finalWriteContext.getExtraMetaData());
                parquetFileWriter.end(finalMetadata);
            } finally {
                AutoCloseables.uncheckedClose(
                        columnStore, pageStore, bloomFilterWriteStore, parquetFileWriter);
                closed = true;
            }
        }
    }

    public void write(T value) throws IOException, InterruptedException {
        try {
            writeSupport.write(value);
            ++recordCount;
            checkBlockSizeReached();
        } catch (Throwable t) {
            aborted = true;
            throw t;
        }
    }

    public long getDataSize() {
        return lastRowGroupEndPos + columnStore.getBufferedSize();
    }

    private void checkBlockSizeReached() throws IOException {
        if (recordCount >= rowGroupRecordCountThreshold) {
            LOG.debug("record count reaches threshold: flushing {} records to disk.", recordCount);
            flushRowGroupToStore();
            initStore();
            recordCountForNextMemCheck =
                    min(
                            max(props.getMinRowCountForPageSizeCheck(), recordCount / 2),
                            props.getMaxRowCountForPageSizeCheck());
            this.lastRowGroupEndPos = parquetFileWriter.getPos();
        } else if (recordCount >= recordCountForNextMemCheck) {
            long memSize = columnStore.getBufferedSize();
            long recordSize = memSize / recordCount;
            if (memSize > (nextRowGroupSize - 2 * recordSize)) {
                LOG.debug(
                        "mem size {} > {}: flushing {} records to disk.",
                        memSize,
                        nextRowGroupSize,
                        recordCount);
                flushRowGroupToStore();
                initStore();
                recordCountForNextMemCheck =
                        min(
                                max(props.getMinRowCountForPageSizeCheck(), recordCount / 2),
                                props.getMaxRowCountForPageSizeCheck());
                this.lastRowGroupEndPos = parquetFileWriter.getPos();
            } else {
                recordCountForNextMemCheck =
                        min(
                                max(
                                        props.getMinRowCountForPageSizeCheck(),
                                        (recordCount
                                                        + (long)
                                                                (nextRowGroupSize
                                                                        / ((float) recordSize)))
                                                / 2),
                                recordCount + props.getMaxRowCountForPageSizeCheck());
                LOG.debug(
                        "Checked mem at {} will check again at: {}",
                        recordCount,
                        recordCountForNextMemCheck);
            }
        }
    }

    private void flushRowGroupToStore() throws IOException {
        try {
            recordConsumer.flush();
            LOG.debug(
                    "Flushing mem columnStore to file. allocated memory: {}",
                    columnStore.getAllocatedSize());
            if (columnStore.getAllocatedSize() > (3 * rowGroupSizeThreshold)) {
                LOG.warn("Too much memory used: {}", columnStore.memUsageString());
            }

            if (recordCount > 0) {
                rowGroupOrdinal++;
                parquetFileWriter.startBlock(recordCount);
                columnStore.flush();
                pageStore.flushToFileWriter(parquetFileWriter);
                recordCount = 0;
                parquetFileWriter.endBlock();
                this.nextRowGroupSize =
                        Math.min(parquetFileWriter.getNextRowGroupSize(), rowGroupSizeThreshold);
            }
        } finally {
            AutoCloseables.uncheckedClose(columnStore, pageStore, bloomFilterWriteStore);
            columnStore = null;
            pageStore = null;
            bloomFilterWriteStore = null;
        }
    }
}
