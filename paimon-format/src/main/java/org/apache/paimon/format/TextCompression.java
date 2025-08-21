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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

/** Utility class for handling text file compression and decompression using Hadoop codecs. */
public class TextCompression {

    /**
     * Creates a compressed output stream using Hadoop's compression codecs.
     *
     * @param out The underlying output stream
     * @param compression The compression format
     * @param options Paimon options for Hadoop configuration
     * @return Compressed output stream
     * @throws IOException If compression stream creation fails
     */
    public static OutputStream createCompressedOutputStream(
            PositionOutputStream out, TextCompressionType compression, Options options)
            throws IOException {
        Optional<CompressionCodec> codecOpt =
                getCompressionCodecByCompression(compression, options);
        if (codecOpt.isPresent()) {
            return codecOpt.get().createOutputStream(out);
        }
        return out;
    }

    /**
     * Creates a decompressed input stream using Hadoop's compression codecs.
     *
     * @param inputStream The underlying input stream
     * @param filePath The file path (used to detect compression from extension)
     * @param options Paimon options for Hadoop configuration
     * @return Decompressed input stream
     */
    public static InputStream createDecompressedInputStream(
            SeekableInputStream inputStream, Path filePath, Options options) {
        try {
            Configuration conf = HadoopUtils.getHadoopConfiguration(options);
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);

            Optional<CompressionCodec> codecOpt =
                    Optional.ofNullable(
                            codecFactory.getCodec(
                                    new org.apache.hadoop.fs.Path(filePath.toString())));

            if (!codecOpt.isPresent()) {
                TextCompressionType compressionType =
                        TextCompression.getTextCompressionType(
                                options.get(CoreOptions.FILE_COMPRESSION), options);
                codecOpt = getCompressionCodecByCompression(compressionType, options);
            }
            if (codecOpt.isPresent()) {
                return codecOpt.get().createInputStream(inputStream);
            }
        } catch (Exception ignore) {
        }
        return inputStream;
    }

    public static TextCompressionType getTextCompressionType(String compression, Options options) {
        TextCompressionType compressionType = TextCompressionType.fromValue(compression);
        Optional<CompressionCodec> codecOpt =
                getCompressionCodecByCompression(compressionType, options);
        if (codecOpt.isPresent()) {
            return TextCompressionType.fromValue(compression);
        }
        return TextCompressionType.NONE;
    }

    /**
     * Gets a compression codec by compression type.
     *
     * @param compressionType The compression type
     * @param options Paimon options for Hadoop configuration
     * @return Optional CompressionCodec instance
     */
    public static Optional<CompressionCodec> getCompressionCodecByCompression(
            TextCompressionType compressionType, Options options) {
        if (compressionType == null || TextCompressionType.NONE == compressionType) {
            return Optional.empty();
        }

        try {
            Configuration conf = HadoopUtils.getHadoopConfiguration(options);
            String codecName = compressionType.hadoopCodecClassName();
            if (codecName != null) {
                Class<?> codecClass = Class.forName(codecName);
                if (CompressionCodec.class.isAssignableFrom(codecClass)) {
                    CompressionCodec codec =
                            (CompressionCodec) codecClass.getDeclaredConstructor().newInstance();
                    if (codec instanceof org.apache.hadoop.conf.Configurable) {
                        ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
                    }
                    // Test if the codec is actually usable by creating a test stream
                    try {
                        codec.createOutputStream(new java.io.ByteArrayOutputStream());
                        return Optional.of(codec);
                    } catch (Exception ignored) {
                        return Optional.empty();
                    }
                }
            }
        } catch (Exception ignore) {
        }
        return Optional.empty();
    }
}
