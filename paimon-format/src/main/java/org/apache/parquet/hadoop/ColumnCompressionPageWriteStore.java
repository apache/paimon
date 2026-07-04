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

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.ConcatenatingByteBufferCollector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Page write store with per-column compression.
 *
 * <p>NOTE: The file is adapted from Apache parquet's {@link ColumnChunkPageWriteStore}.
 */
public class ColumnCompressionPageWriteStore implements PageWriteStore, BloomFilterWriteStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(ColumnCompressionPageWriteStore.class);

    private static final ParquetMetadataConverter PARQUET_METADATA_CONVERTER =
            new ParquetMetadataConverter();

    private static final class ColumnChunkPageWriter implements PageWriter, BloomFilterWriter {

        private final ColumnDescriptor path;
        private final CompressionCodecFactory.BytesInputCompressor compressor;

        private final ByteArrayOutputStream tempOutputStream = new ByteArrayOutputStream();
        private final ConcatenatingByteBufferCollector buf;
        private DictionaryPage dictionaryPage;

        private long uncompressedLength;
        private long compressedLength;
        private long totalValueCount;
        private int pageCount;

        private Set<Encoding> rlEncodings = new HashSet<>();
        private Set<Encoding> dlEncodings = new HashSet<>();
        private List<Encoding> dataEncodings = new ArrayList<>();

        private BloomFilter bloomFilter;
        private ColumnIndexBuilder columnIndexBuilder;
        private OffsetIndexBuilder offsetIndexBuilder;
        private Statistics totalStatistics;
        private final SizeStatistics totalSizeStatistics;
        private final GeospatialStatistics totalGeospatialStatistics;
        private final ByteBufferReleaser releaser;

        private final CRC32 crc;
        private final boolean pageWriteChecksumEnabled;

        private final BlockCipher.Encryptor headerBlockEncryptor;
        private final BlockCipher.Encryptor pageBlockEncryptor;
        private final int rowGroupOrdinal;
        private final int columnOrdinal;
        private int pageOrdinal;
        private final byte[] dataPageAAD;
        private final byte[] dataPageHeaderAAD;
        private final byte[] fileAAD;

        private ColumnChunkPageWriter(
                ColumnDescriptor path,
                CompressionCodecFactory.BytesInputCompressor compressor,
                ByteBufferAllocator allocator,
                int columnIndexTruncateLength,
                boolean pageWriteChecksumEnabled,
                BlockCipher.Encryptor headerBlockEncryptor,
                BlockCipher.Encryptor pageBlockEncryptor,
                byte[] fileAAD,
                int rowGroupOrdinal,
                int columnOrdinal) {
            this.path = path;
            this.compressor = compressor;
            this.releaser = new ByteBufferReleaser(allocator);
            this.buf = new ConcatenatingByteBufferCollector(allocator);
            this.columnIndexBuilder =
                    ColumnIndexBuilder.getBuilder(
                            path.getPrimitiveType(), columnIndexTruncateLength);
            this.offsetIndexBuilder = OffsetIndexBuilder.getBuilder();
            this.totalSizeStatistics =
                    SizeStatistics.newBuilder(
                                    path.getPrimitiveType(),
                                    path.getMaxRepetitionLevel(),
                                    path.getMaxDefinitionLevel())
                            .build();
            this.totalGeospatialStatistics =
                    GeospatialStatistics.newBuilder(path.getPrimitiveType()).build();
            this.pageWriteChecksumEnabled = pageWriteChecksumEnabled;
            this.crc = pageWriteChecksumEnabled ? new CRC32() : null;
            this.headerBlockEncryptor = headerBlockEncryptor;
            this.pageBlockEncryptor = pageBlockEncryptor;
            this.fileAAD = fileAAD;
            this.rowGroupOrdinal = rowGroupOrdinal;
            this.columnOrdinal = columnOrdinal;
            this.pageOrdinal = -1;
            if (headerBlockEncryptor != null) {
                dataPageHeaderAAD =
                        AesCipher.createModuleAAD(
                                fileAAD,
                                ModuleType.DataPageHeader,
                                rowGroupOrdinal,
                                columnOrdinal,
                                0);
            } else {
                dataPageHeaderAAD = null;
            }
            if (pageBlockEncryptor != null) {
                dataPageAAD =
                        AesCipher.createModuleAAD(
                                fileAAD, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
            } else {
                dataPageAAD = null;
            }
        }

        @Override
        @Deprecated
        public void writePage(
                BytesInput bytesInput,
                int valueCount,
                Statistics<?> statistics,
                Encoding rlEncoding,
                Encoding dlEncoding,
                Encoding valuesEncoding)
                throws IOException {
            columnIndexBuilder = ColumnIndexBuilder.getNoOpBuilder();
            offsetIndexBuilder = OffsetIndexBuilder.getNoOpBuilder();

            writePage(
                    bytesInput, valueCount, -1, statistics, rlEncoding, dlEncoding, valuesEncoding);
        }

        @Override
        public void writePage(
                BytesInput bytes,
                int valueCount,
                int rowCount,
                Statistics<?> statistics,
                Encoding rlEncoding,
                Encoding dlEncoding,
                Encoding valuesEncoding)
                throws IOException {
            writePage(
                    bytes,
                    valueCount,
                    rowCount,
                    statistics,
                    null,
                    null,
                    rlEncoding,
                    dlEncoding,
                    valuesEncoding);
        }

        @Override
        public void writePage(
                BytesInput bytes,
                int valueCount,
                int rowCount,
                Statistics<?> statistics,
                SizeStatistics sizeStatistics,
                GeospatialStatistics geospatialStatistics,
                Encoding rlEncoding,
                Encoding dlEncoding,
                Encoding valuesEncoding)
                throws IOException {
            pageOrdinal++;
            long uncompressedSize = bytes.size();
            if (uncompressedSize > Integer.MAX_VALUE || uncompressedSize < 0) {
                throw new ParquetEncodingException(
                        "Cannot write page larger than Integer.MAX_VALUE or negative bytes: "
                                + uncompressedSize);
            }
            BytesInput compressedBytes = compressor.compress(bytes);
            if (pageBlockEncryptor != null) {
                AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
                compressedBytes =
                        BytesInput.from(
                                pageBlockEncryptor.encrypt(
                                        compressedBytes.toByteArray(), dataPageAAD));
            }
            long compressedSize = compressedBytes.size();
            if (compressedSize > Integer.MAX_VALUE) {
                throw new ParquetEncodingException(
                        "Cannot write compressed page larger than Integer.MAX_VALUE bytes: "
                                + compressedSize);
            }
            tempOutputStream.reset();
            if (headerBlockEncryptor != null) {
                AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
            }
            if (pageWriteChecksumEnabled) {
                crc.reset();
                crc.update(compressedBytes.toByteArray());
                PARQUET_METADATA_CONVERTER.writeDataPageV1Header(
                        (int) uncompressedSize,
                        (int) compressedSize,
                        valueCount,
                        rlEncoding,
                        dlEncoding,
                        valuesEncoding,
                        (int) crc.getValue(),
                        tempOutputStream,
                        headerBlockEncryptor,
                        dataPageHeaderAAD);
            } else {
                PARQUET_METADATA_CONVERTER.writeDataPageV1Header(
                        (int) uncompressedSize,
                        (int) compressedSize,
                        valueCount,
                        rlEncoding,
                        dlEncoding,
                        valuesEncoding,
                        tempOutputStream,
                        headerBlockEncryptor,
                        dataPageHeaderAAD);
            }
            this.uncompressedLength += uncompressedSize;
            this.compressedLength += compressedSize;
            this.totalValueCount += valueCount;
            this.pageCount += 1;

            mergeColumnStatistics(statistics, sizeStatistics, geospatialStatistics);
            offsetIndexBuilder.add(
                    toIntWithCheck(tempOutputStream.size() + compressedSize),
                    rowCount,
                    sizeStatistics != null
                            ? sizeStatistics.getUnencodedByteArrayDataBytes()
                            : Optional.empty());

            buf.collect(BytesInput.concat(BytesInput.from(tempOutputStream), compressedBytes));
            rlEncodings.add(rlEncoding);
            dlEncodings.add(dlEncoding);
            dataEncodings.add(valuesEncoding);
        }

        @Override
        public void writePageV2(
                int rowCount,
                int nullCount,
                int valueCount,
                BytesInput repetitionLevels,
                BytesInput definitionLevels,
                Encoding dataEncoding,
                BytesInput data,
                Statistics<?> statistics)
                throws IOException {
            writePageV2(
                    rowCount,
                    nullCount,
                    valueCount,
                    repetitionLevels,
                    definitionLevels,
                    dataEncoding,
                    data,
                    statistics,
                    null,
                    null);
        }

        @Override
        public void writePageV2(
                int rowCount,
                int nullCount,
                int valueCount,
                BytesInput repetitionLevels,
                BytesInput definitionLevels,
                Encoding dataEncoding,
                BytesInput data,
                Statistics<?> statistics,
                SizeStatistics sizeStatistics,
                GeospatialStatistics geospatialStatistics)
                throws IOException {
            pageOrdinal++;

            int rlByteLength = toIntWithCheck(repetitionLevels.size());
            int dlByteLength = toIntWithCheck(definitionLevels.size());
            int uncompressedSize =
                    toIntWithCheck(data.size() + repetitionLevels.size() + definitionLevels.size());
            boolean compressed = false;
            BytesInput compressedData = BytesInput.empty();
            if (data.size() > 0) {
                compressedData = compressor.compress(data);
                compressed = true;
            }
            if (pageBlockEncryptor != null) {
                AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
                compressedData =
                        BytesInput.from(
                                pageBlockEncryptor.encrypt(
                                        compressedData.toByteArray(), dataPageAAD));
            }
            int compressedSize =
                    toIntWithCheck(
                            compressedData.size()
                                    + repetitionLevels.size()
                                    + definitionLevels.size());
            tempOutputStream.reset();
            if (headerBlockEncryptor != null) {
                AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
            }
            if (pageWriteChecksumEnabled) {
                crc.reset();
                if (repetitionLevels.size() > 0) {
                    crc.update(repetitionLevels.toByteArray());
                }
                if (definitionLevels.size() > 0) {
                    crc.update(definitionLevels.toByteArray());
                }
                if (compressedData.size() > 0) {
                    crc.update(compressedData.toByteArray());
                }
                PARQUET_METADATA_CONVERTER.writeDataPageV2Header(
                        uncompressedSize,
                        compressedSize,
                        valueCount,
                        nullCount,
                        rowCount,
                        dataEncoding,
                        rlByteLength,
                        dlByteLength,
                        compressed,
                        (int) crc.getValue(),
                        tempOutputStream,
                        headerBlockEncryptor,
                        dataPageHeaderAAD);
            } else {
                PARQUET_METADATA_CONVERTER.writeDataPageV2Header(
                        uncompressedSize,
                        compressedSize,
                        valueCount,
                        nullCount,
                        rowCount,
                        dataEncoding,
                        rlByteLength,
                        dlByteLength,
                        compressed,
                        tempOutputStream,
                        headerBlockEncryptor,
                        dataPageHeaderAAD);
            }
            this.uncompressedLength += uncompressedSize;
            this.compressedLength += compressedSize;
            this.totalValueCount += valueCount;
            this.pageCount += 1;

            mergeColumnStatistics(statistics, sizeStatistics, geospatialStatistics);
            offsetIndexBuilder.add(
                    toIntWithCheck((long) tempOutputStream.size() + compressedSize),
                    rowCount,
                    sizeStatistics != null
                            ? sizeStatistics.getUnencodedByteArrayDataBytes()
                            : Optional.empty());

            buf.collect(
                    BytesInput.concat(
                            BytesInput.from(tempOutputStream),
                            repetitionLevels,
                            definitionLevels,
                            compressedData));
            dataEncodings.add(dataEncoding);
        }

        private int toIntWithCheck(long size) {
            if (size > Integer.MAX_VALUE) {
                throw new ParquetEncodingException(
                        "Cannot write page larger than " + Integer.MAX_VALUE + " bytes: " + size);
            }
            return (int) size;
        }

        private void mergeColumnStatistics(
                Statistics<?> statistics,
                SizeStatistics sizeStatistics,
                GeospatialStatistics geospatialStatistics) {
            totalSizeStatistics.mergeStatistics(sizeStatistics);
            if (!totalSizeStatistics.isValid()) {
                sizeStatistics = null;
            }

            totalGeospatialStatistics.merge(geospatialStatistics);

            if (totalStatistics != null && totalStatistics.isEmpty()) {
                return;
            }

            if (statistics == null || statistics.isEmpty()) {
                totalStatistics = Statistics.getBuilderForReading(path.getPrimitiveType()).build();
                columnIndexBuilder = ColumnIndexBuilder.getNoOpBuilder();
            } else if (totalStatistics == null) {
                totalStatistics = statistics.copy();
                columnIndexBuilder.add(statistics, sizeStatistics);
            } else {
                totalStatistics.mergeStatistics(statistics);
                columnIndexBuilder.add(statistics, sizeStatistics);
            }
        }

        @Override
        public long getMemSize() {
            return buf.size();
        }

        public void writeToFileWriter(ParquetFileWriter writer) throws IOException {
            if (headerBlockEncryptor == null) {
                writer.writeColumnChunk(
                        path,
                        totalValueCount,
                        compressor.getCodecName(),
                        dictionaryPage,
                        buf,
                        uncompressedLength,
                        compressedLength,
                        totalStatistics,
                        totalSizeStatistics,
                        totalGeospatialStatistics,
                        columnIndexBuilder,
                        offsetIndexBuilder,
                        bloomFilter,
                        rlEncodings,
                        dlEncodings,
                        dataEncodings);
            } else {
                writer.writeColumnChunk(
                        path,
                        totalValueCount,
                        compressor.getCodecName(),
                        dictionaryPage,
                        buf,
                        uncompressedLength,
                        compressedLength,
                        totalStatistics,
                        totalSizeStatistics,
                        totalGeospatialStatistics,
                        columnIndexBuilder,
                        offsetIndexBuilder,
                        bloomFilter,
                        rlEncodings,
                        dlEncodings,
                        dataEncodings,
                        headerBlockEncryptor,
                        rowGroupOrdinal,
                        columnOrdinal,
                        fileAAD);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        String.format(
                                        "written %,dB for %s: %,d values, %,dB raw, %,dB comp, %d pages, encodings: %s",
                                        buf.size(),
                                        path,
                                        totalValueCount,
                                        uncompressedLength,
                                        compressedLength,
                                        pageCount,
                                        new HashSet<>(dataEncodings))
                                + (dictionaryPage != null
                                        ? String.format(
                                                ", dic { %,d entries, %,dB raw, %,dB comp}",
                                                dictionaryPage.getDictionarySize(),
                                                dictionaryPage.getUncompressedSize(),
                                                dictionaryPage.getDictionarySize())
                                        : ""));
            }
            rlEncodings.clear();
            dlEncodings.clear();
            dataEncodings.clear();
            pageCount = 0;
            pageOrdinal = -1;
        }

        @Override
        public long allocatedSize() {
            return buf.size();
        }

        @Override
        public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
            if (this.dictionaryPage != null) {
                throw new ParquetEncodingException("Only one dictionary page is allowed");
            }
            BytesInput dictionaryBytes = dictionaryPage.getBytes();
            int uncompressedSize = (int) dictionaryBytes.size();
            BytesInput compressedBytes = compressor.compress(dictionaryBytes);
            if (pageBlockEncryptor != null) {
                byte[] dictionaryPageAAD =
                        AesCipher.createModuleAAD(
                                fileAAD,
                                ModuleType.DictionaryPage,
                                rowGroupOrdinal,
                                columnOrdinal,
                                -1);
                compressedBytes =
                        BytesInput.from(
                                pageBlockEncryptor.encrypt(
                                        compressedBytes.toByteArray(), dictionaryPageAAD));
            }
            this.dictionaryPage =
                    new DictionaryPage(
                            compressedBytes.copy(releaser),
                            uncompressedSize,
                            dictionaryPage.getDictionarySize(),
                            dictionaryPage.getEncoding());
        }

        @Override
        public String memUsageString(String prefix) {
            return buf.memUsageString(prefix + " ColumnChunkPageWriter");
        }

        @Override
        public void close() {
            AutoCloseables.uncheckedClose(buf, releaser);
        }

        @Override
        public void writeBloomFilter(BloomFilter bloomFilter) {
            this.bloomFilter = bloomFilter;
        }
    }

    private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<>();
    private final MessageType schema;

    public ColumnCompressionPageWriteStore(
            CompressionCodecFactory codecFactory,
            ColumnCompressionCodecs compressionCodecs,
            MessageType schema,
            ByteBufferAllocator allocator,
            int columnIndexTruncateLength,
            boolean pageWriteChecksumEnabled,
            InternalFileEncryptor fileEncryptor,
            int rowGroupOrdinal) {
        this.schema = schema;
        if (fileEncryptor == null) {
            for (ColumnDescriptor path : schema.getColumns()) {
                writers.put(
                        path,
                        new ColumnChunkPageWriter(
                                path,
                                codecFactory.getCompressor(compressionCodecs.getCodec(path)),
                                allocator,
                                columnIndexTruncateLength,
                                pageWriteChecksumEnabled,
                                null,
                                null,
                                null,
                                -1,
                                -1));
            }
            return;
        }

        int columnOrdinal = -1;
        byte[] fileAAD = fileEncryptor.getFileAAD();
        for (ColumnDescriptor path : schema.getColumns()) {
            columnOrdinal++;
            BlockCipher.Encryptor headerBlockEncryptor = null;
            BlockCipher.Encryptor pageBlockEncryptor = null;
            ColumnPath columnPath = ColumnPath.get(path.getPath());

            InternalColumnEncryptionSetup columnSetup =
                    fileEncryptor.getColumnSetup(columnPath, true, columnOrdinal);
            if (columnSetup.isEncrypted()) {
                headerBlockEncryptor = columnSetup.getMetaDataEncryptor();
                pageBlockEncryptor = columnSetup.getDataEncryptor();
            }

            writers.put(
                    path,
                    new ColumnChunkPageWriter(
                            path,
                            codecFactory.getCompressor(compressionCodecs.getCodec(path)),
                            allocator,
                            columnIndexTruncateLength,
                            pageWriteChecksumEnabled,
                            headerBlockEncryptor,
                            pageBlockEncryptor,
                            fileAAD,
                            rowGroupOrdinal,
                            columnOrdinal));
        }
    }

    @Override
    public PageWriter getPageWriter(ColumnDescriptor path) {
        return writers.get(path);
    }

    @Override
    public void close() {
        AutoCloseables.uncheckedClose(writers.values());
        writers.clear();
    }

    @Override
    public BloomFilterWriter getBloomFilterWriter(ColumnDescriptor path) {
        return writers.get(path);
    }

    public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
        for (ColumnDescriptor path : schema.getColumns()) {
            ColumnChunkPageWriter pageWriter = writers.get(path);
            pageWriter.writeToFileWriter(writer);
        }
    }
}
