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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link ParquetBuilder} for {@link InternalRow}. */
public class RowDataParquetBuilder implements ParquetBuilder<InternalRow> {

    private final RowType rowType;
    private final Options conf;

    public RowDataParquetBuilder(RowType rowType, Options conf) {
        this.rowType = rowType;
        this.conf = conf;
    }

    @Override
    public ParquetWriter<InternalRow> createWriter(OutputFile out, String compression)
            throws IOException {

        return new ParquetRowDataBuilder(out, rowType)
                .withCompressionCodec(CompressionCodecName.fromConf(getCompression(compression)))
                .withRowGroupSize(
                        conf.getLong(
                                ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE))
                .withPageSize(
                        conf.getInteger(
                                ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE))
                .withDictionaryPageSize(
                        conf.getInteger(
                                ParquetOutputFormat.DICTIONARY_PAGE_SIZE,
                                ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE))
                .withMaxPaddingSize(
                        conf.getInteger(
                                ParquetOutputFormat.MAX_PADDING_BYTES,
                                ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
                .withDictionaryEncoding(
                        conf.getBoolean(
                                ParquetOutputFormat.ENABLE_DICTIONARY,
                                ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED))
                .withValidation(conf.getBoolean(ParquetOutputFormat.VALIDATION, false))
                .withWriterVersion(
                        ParquetProperties.WriterVersion.fromString(
                                conf.getString(
                                        ParquetOutputFormat.WRITER_VERSION,
                                        ParquetProperties.DEFAULT_WRITER_VERSION.toString())))
                .withEncryption(
                        getFileEncryptionProperties(
                                conf.getString("parquet.encryption.key", null),
                                conf.getString("parquet.encryption.prefix", null)))
                .build();
    }

    public String getCompression(@Nullable String compression) {
        String compressName;
        if (null != compression) {
            compressName = compression;
        } else {
            compressName =
                    conf.getString(
                            ParquetOutputFormat.COMPRESSION, CompressionCodecName.SNAPPY.name());
        }
        return compressName;
    }

    // The fileEncryptionKey encrypts the parquet footer, and the fileAADPrefix ensures the
    // integrity of the file and helps verify the validity of the file.
    // After implementing kmsClient, FileEncryptionKeys and fileAADPrefix are automatically
    // generated for encryption when parquet is written. The fileEncryptionKey and fileAADPrefix are
    // obtained from kmsClient for decryption when parquet is read.
    // todo Implement kmsClient for automatic encryption and decryption
    public FileEncryptionProperties getFileEncryptionProperties(
            @Nullable String fileEncryptionKey, @Nullable String fileAADPrefix) {
        FileEncryptionProperties fileEncryptionProperties = null;
        if (fileEncryptionKey != null) {
            byte[] encryptionKeyArray = fileEncryptionKey.getBytes(StandardCharsets.UTF_8);
            byte[] aadPrefixArray = fileAADPrefix.getBytes(StandardCharsets.UTF_8);

            fileEncryptionProperties =
                    FileEncryptionProperties.builder(encryptionKeyArray)
                            .withAADPrefix(aadPrefixArray)
                            .build();
        } else {
            checkArgument(fileAADPrefix == null, "AAD prefix set with null encryption key");
        }
        return fileEncryptionProperties;
    }
}
