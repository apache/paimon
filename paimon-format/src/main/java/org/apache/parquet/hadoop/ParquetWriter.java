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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;

/** Write records to a Parquet file. */
public class ParquetWriter<T> implements Closeable {

    public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
    public static final int DEFAULT_PAGE_SIZE = ParquetProperties.DEFAULT_PAGE_SIZE;
    public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME =
            CompressionCodecName.UNCOMPRESSED;
    public static final boolean DEFAULT_IS_DICTIONARY_ENABLED =
            ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED;
    public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
    public static final WriterVersion DEFAULT_WRITER_VERSION =
            ParquetProperties.DEFAULT_WRITER_VERSION;

    public static final String OBJECT_MODEL_NAME_PROP = "writer.model.name";

    // max size (bytes) to write as padding and the min size of a row group
    public static final int MAX_PADDING_SIZE_DEFAULT = 8 * 1024 * 1024; // 8MB

    private final InternalParquetRecordWriter<T> writer;
    private final CodecFactory codecFactory;

    ParquetWriter(
            OutputFile file,
            ParquetFileWriter.Mode mode,
            WriteSupport<T> writeSupport,
            CompressionCodecName compressionCodecName,
            long rowGroupSize,
            boolean validating,
            Configuration conf,
            int maxPaddingSize,
            ParquetProperties encodingProps)
            throws IOException {

        WriteSupport.WriteContext writeContext = writeSupport.init(conf);
        MessageType schema = writeContext.getSchema();

        ParquetFileWriter fileWriter =
                new ParquetFileWriter(
                        file,
                        schema,
                        mode,
                        rowGroupSize,
                        maxPaddingSize,
                        encodingProps.getColumnIndexTruncateLength(),
                        encodingProps.getStatisticsTruncateLength(),
                        encodingProps.getPageWriteChecksumEnabled(),
                        (FileEncryptionProperties) null);
        fileWriter.start();

        this.codecFactory = new CodecFactory(conf, encodingProps.getPageSizeThreshold());
        CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(compressionCodecName);
        this.writer =
                new InternalParquetRecordWriter<T>(
                        fileWriter,
                        writeSupport,
                        schema,
                        writeContext.getExtraMetaData(),
                        rowGroupSize,
                        compressor,
                        validating,
                        encodingProps);
    }

    public void write(T object) throws IOException {
        try {
            writer.write(object);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            // release after the writer closes in case it is used for a last flush
            codecFactory.release();
        }
    }

    /** @return the ParquetMetadata written to the (closed) file. */
    public ParquetMetadata getFooter() {
        return writer.getFooter();
    }

    /** @return the total size of data written to the file and buffered in memory */
    public long getDataSize() {
        return writer.getDataSize();
    }

    /**
     * An abstract builder class for ParquetWriter instances.
     *
     * <p>Object models should extend this builder to provide writer configuration options.
     *
     * @param <T> The type of objects written by the constructed ParquetWriter.
     * @param <SELF> The type of this builder that is returned by builder methods
     */
    public abstract static class Builder<T, SELF extends Builder<T, SELF>> {
        private OutputFile file = null;
        private Configuration conf = new Configuration();
        private ParquetFileWriter.Mode mode;
        private CompressionCodecName codecName = DEFAULT_COMPRESSION_CODEC_NAME;
        private long rowGroupSize = DEFAULT_BLOCK_SIZE;
        private int maxPaddingSize = MAX_PADDING_SIZE_DEFAULT;
        private boolean enableValidation = DEFAULT_IS_VALIDATING_ENABLED;
        private ParquetProperties.Builder encodingPropsBuilder = ParquetProperties.builder();

        protected Builder(OutputFile path) {
            this.file = path;
        }

        /** @return this as the correct subclass of ParquetWriter.Builder. */
        protected abstract SELF self();

        /**
         * @param conf a configuration
         * @return an appropriate WriteSupport for the object model.
         */
        protected abstract WriteSupport<T> getWriteSupport(Configuration conf);

        /**
         * Set the {@link Configuration} used by the constructed writer.
         *
         * @param conf a {@code Configuration}
         * @return this builder for method chaining.
         */
        public SELF withConf(Configuration conf) {
            this.conf = conf;
            return self();
        }

        /**
         * Set the {@link ParquetFileWriter.Mode write mode} used when creating the backing file for
         * this writer.
         *
         * @param mode a {@code ParquetFileWriter.Mode}
         * @return this builder for method chaining.
         */
        public SELF withWriteMode(ParquetFileWriter.Mode mode) {
            this.mode = mode;
            return self();
        }

        /**
         * Set the {@link CompressionCodecName compression codec} used by the constructed writer.
         *
         * @param codecName a {@code CompressionCodecName}
         * @return this builder for method chaining.
         */
        public SELF withCompressionCodec(CompressionCodecName codecName) {
            this.codecName = codecName;
            return self();
        }

        /**
         * Set the Parquet format row group size used by the constructed writer.
         *
         * @param rowGroupSize an integer size in bytes
         * @return this builder for method chaining.
         * @deprecated Use {@link #withRowGroupSize(long)} instead
         */
        @Deprecated
        public SELF withRowGroupSize(int rowGroupSize) {
            return withRowGroupSize((long) rowGroupSize);
        }

        /**
         * Set the Parquet format row group size used by the constructed writer.
         *
         * @param rowGroupSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public SELF withRowGroupSize(long rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
            return self();
        }

        /**
         * Set the Parquet format page size used by the constructed writer.
         *
         * @param pageSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public SELF withPageSize(int pageSize) {
            encodingPropsBuilder.withPageSize(pageSize);
            return self();
        }

        /**
         * Sets the Parquet format page row count limit used by the constructed writer.
         *
         * @param rowCount limit for the number of rows stored in a page
         * @return this builder for method chaining
         */
        public SELF withPageRowCountLimit(int rowCount) {
            encodingPropsBuilder.withPageRowCountLimit(rowCount);
            return self();
        }

        /**
         * Set the Parquet format dictionary page size used by the constructed writer.
         *
         * @param dictionaryPageSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public SELF withDictionaryPageSize(int dictionaryPageSize) {
            encodingPropsBuilder.withDictionaryPageSize(dictionaryPageSize);
            return self();
        }

        /**
         * Set the maximum amount of padding, in bytes, that will be used to align row groups with
         * blocks in the underlying filesystem. If the underlying filesystem is not a block
         * filesystem like HDFS, this has no effect.
         *
         * @param maxPaddingSize an integer size in bytes
         * @return this builder for method chaining.
         */
        public SELF withMaxPaddingSize(int maxPaddingSize) {
            this.maxPaddingSize = maxPaddingSize;
            return self();
        }

        /**
         * Enables dictionary encoding for the constructed writer.
         *
         * @return this builder for method chaining.
         */
        public SELF enableDictionaryEncoding() {
            encodingPropsBuilder.withDictionaryEncoding(true);
            return self();
        }

        /**
         * Enable or disable dictionary encoding for the constructed writer.
         *
         * @param enableDictionary whether dictionary encoding should be enabled
         * @return this builder for method chaining.
         */
        public SELF withDictionaryEncoding(boolean enableDictionary) {
            encodingPropsBuilder.withDictionaryEncoding(enableDictionary);
            return self();
        }

        public SELF withByteStreamSplitEncoding(boolean enableByteStreamSplit) {
            encodingPropsBuilder.withByteStreamSplitEncoding(enableByteStreamSplit);
            return self();
        }

        /**
         * Enable or disable dictionary encoding of the specified column for the constructed writer.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enableDictionary whether dictionary encoding should be enabled
         * @return this builder for method chaining.
         */
        public SELF withDictionaryEncoding(String columnPath, boolean enableDictionary) {
            encodingPropsBuilder.withDictionaryEncoding(columnPath, enableDictionary);
            return self();
        }

        /**
         * Enables validation for the constructed writer.
         *
         * @return this builder for method chaining.
         */
        public SELF enableValidation() {
            this.enableValidation = true;
            return self();
        }

        /**
         * Enable or disable validation for the constructed writer.
         *
         * @param enableValidation whether validation should be enabled
         * @return this builder for method chaining.
         */
        public SELF withValidation(boolean enableValidation) {
            this.enableValidation = enableValidation;
            return self();
        }

        /**
         * Set the {@link WriterVersion format version} used by the constructed writer.
         *
         * @param version a {@code WriterVersion}
         * @return this builder for method chaining.
         */
        public SELF withWriterVersion(WriterVersion version) {
            encodingPropsBuilder.withWriterVersion(version);
            return self();
        }

        /**
         * Enables writing page level checksums for the constructed writer.
         *
         * @return this builder for method chaining.
         */
        public SELF enablePageWriteChecksum() {
            encodingPropsBuilder.withPageWriteChecksumEnabled(true);
            return self();
        }

        /**
         * Enables writing page level checksums for the constructed writer.
         *
         * @param enablePageWriteChecksum whether page checksums should be written out
         * @return this builder for method chaining.
         */
        public SELF withPageWriteChecksumEnabled(boolean enablePageWriteChecksum) {
            encodingPropsBuilder.withPageWriteChecksumEnabled(enablePageWriteChecksum);
            return self();
        }

        /**
         * Sets the NDV (number of distinct values) for the specified column.
         *
         * @param columnPath the path of the column (dot-string)
         * @param ndv the NDV of the column
         * @return this builder for method chaining.
         */
        public SELF withBloomFilterNDV(String columnPath, long ndv) {
            encodingPropsBuilder.withBloomFilterNDV(columnPath, ndv);

            return self();
        }

        public SELF withBloomFilterFPP(String columnPath, double fpp) {
            encodingPropsBuilder.withBloomFilterFPP(columnPath, fpp);
            return self();
        }

        /**
         * Sets the bloom filter enabled/disabled.
         *
         * @param enabled whether to write bloom filters
         * @return this builder for method chaining
         */
        public SELF withBloomFilterEnabled(boolean enabled) {
            encodingPropsBuilder.withBloomFilterEnabled(enabled);
            return self();
        }

        /**
         * Sets the bloom filter enabled/disabled for the specified column. If not set for the
         * column specifically the default enabled/disabled state will take place. See {@link
         * #withBloomFilterEnabled(boolean)}.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enabled whether to write bloom filter for the column
         * @return this builder for method chaining
         */
        public SELF withBloomFilterEnabled(String columnPath, boolean enabled) {
            encodingPropsBuilder.withBloomFilterEnabled(columnPath, enabled);
            return self();
        }

        /**
         * Sets the minimum number of rows to write before a page size check is done.
         *
         * @param min writes at least `min` rows before invoking a page size check
         * @return this builder for method chaining
         */
        public SELF withMinRowCountForPageSizeCheck(int min) {
            encodingPropsBuilder.withMinRowCountForPageSizeCheck(min);
            return self();
        }

        /**
         * Sets the maximum number of rows to write before a page size check is done.
         *
         * @param max makes a page size check after `max` rows have been written
         * @return this builder for method chaining
         */
        public SELF withMaxRowCountForPageSizeCheck(int max) {
            encodingPropsBuilder.withMaxRowCountForPageSizeCheck(max);
            return self();
        }

        /**
         * Sets the length to be used for truncating binary values in a binary column index.
         *
         * @param length the length to truncate to
         * @return this builder for method chaining
         */
        public SELF withColumnIndexTruncateLength(int length) {
            encodingPropsBuilder.withColumnIndexTruncateLength(length);
            return self();
        }

        /**
         * Sets the length which the min/max binary values in row groups are truncated to.
         *
         * @param length the length to truncate to
         * @return this builder for method chaining
         */
        public SELF withStatisticsTruncateLength(int length) {
            encodingPropsBuilder.withStatisticsTruncateLength(length);
            return self();
        }

        /**
         * Set a property that will be available to the read path. For writers that use a Hadoop
         * configuration, this is the recommended way to add configuration values.
         *
         * @param property a String property name
         * @param value a String property value
         * @return this builder for method chaining.
         */
        public SELF config(String property, String value) {
            conf.set(property, value);
            return self();
        }

        /**
         * Build a {@link ParquetWriter} with the accumulated configuration.
         *
         * @return a configured {@code ParquetWriter} instance.
         * @throws IOException if there is an error while creating the writer
         */
        public ParquetWriter<T> build() throws IOException {
            return new ParquetWriter<>(
                    file,
                    mode,
                    getWriteSupport(conf),
                    codecName,
                    rowGroupSize,
                    enableValidation,
                    conf,
                    maxPaddingSize,
                    encodingPropsBuilder.build());
        }
    }
}
