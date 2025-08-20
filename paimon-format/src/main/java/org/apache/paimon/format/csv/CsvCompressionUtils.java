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

package org.apache.paimon.format.csv;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Utility class for handling CSV file compression and decompression using Hadoop codecs. */
public class CsvCompressionUtils {

    private static final String[] SUPPORTED_COMPRESSIONS = {
        "none", "gzip", "bzip2", "deflate", "snappy", "lz4", "lzo", "zstd"
    };

    /**
     * Creates a Hadoop Configuration for compression codec factory.
     *
     * @param options Paimon options that may contain Hadoop configuration
     * @return Hadoop Configuration instance
     */
    private static Configuration getHadoopConfiguration(Options options) {
        return HadoopUtils.getHadoopConfiguration(options);
    }

    /**
     * Creates a compressed output stream using Hadoop's compression codecs.
     *
     * @param out The underlying output stream
     * @param compression The compression format (following Spark's naming convention)
     * @param options Paimon options for Hadoop configuration
     * @return Compressed output stream
     * @throws IOException If compression stream creation fails
     */
    public static OutputStream createCompressedOutputStream(
            PositionOutputStream out, String compression, Options options) throws IOException {
        if (compression == null || compression.isEmpty() || "none".equalsIgnoreCase(compression)) {
            return out;
        }

        Configuration conf = getHadoopConfiguration(options);
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);

        // Map Spark-style compression names to Hadoop codec names
        String codecName = mapCompressionName(compression);

        // Try to get codec by name first
        CompressionCodec codec = getCodecByName(codecFactory, codecName, conf);

        if (codec == null) {
            // If specific codec is not available, try fallback for known unsupported cases
            if ("zstd".equalsIgnoreCase(compression)) {
                // Fallback to gzip for zstd when native library is not available
                codecName = mapCompressionName("gzip");
                codec = getCodecByName(codecFactory, codecName, conf);
                if (codec != null) {
                    return codec.createOutputStream(out);
                }
            }

            throw new IllegalArgumentException(
                    "Unsupported compression format: "
                            + compression
                            + ". Supported formats are: "
                            + String.join(", ", SUPPORTED_COMPRESSIONS));
        }

        return codec.createOutputStream(out);
    }

    /**
     * Creates a decompressed input stream using Hadoop's compression codecs.
     *
     * @param inputStream The underlying input stream
     * @param filePath The file path (used to detect compression from extension)
     * @param options Paimon options for Hadoop configuration
     * @return Decompressed input stream
     * @throws IOException If decompression stream creation fails
     */
    public static InputStream createDecompressedInputStream(
            SeekableInputStream inputStream, Path filePath, Options options) throws IOException {
        Configuration conf = getHadoopConfiguration(options);
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);

        // Try to detect codec from file path
        CompressionCodec codec =
                codecFactory.getCodec(new org.apache.hadoop.fs.Path(filePath.toString()));

        if (codec == null) {
            // No compression detected, return original stream
            return inputStream;
        }

        return codec.createInputStream(inputStream);
    }

    /**
     * Maps Spark-style compression names to Hadoop codec class names.
     *
     * @param compression The compression format string from Spark
     * @return Hadoop codec class name
     */
    private static String mapCompressionName(String compression) {
        switch (compression.toLowerCase()) {
            case "gzip":
            case "gz":
                return "org.apache.hadoop.io.compress.GzipCodec";
            case "bzip2":
            case "bz2":
                return "org.apache.hadoop.io.compress.BZip2Codec";
            case "deflate":
                return "org.apache.hadoop.io.compress.DeflateCodec";
            case "snappy":
                return "org.apache.hadoop.io.compress.SnappyCodec";
            case "lz4":
                return "org.apache.hadoop.io.compress.Lz4Codec";
            case "lzo":
                return "org.apache.hadoop.io.compress.LzoCodec";
            case "zstd":
                return "org.apache.hadoop.io.compress.ZStandardCodec";
            default:
                return compression;
        }
    }

    /**
     * Gets a compression codec by name, handling various ways of codec instantiation.
     *
     * @param codecFactory The Hadoop codec factory
     * @param codecName The codec class name
     * @param conf Hadoop configuration
     * @return CompressionCodec instance or null if not found
     */
    @Nullable
    private static CompressionCodec getCodecByName(
            CompressionCodecFactory codecFactory, String codecName, Configuration conf) {
        try {
            // Try to get codec by class name
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
                    return codec;
                } catch (Exception e) {
                    // If codec creation fails (e.g., missing native library), return null
                    // This handles cases like zstd where the codec class exists but native support
                    // is missing
                    return null;
                }
            }
        } catch (Exception e) {
            // Try alternative approach using codec factory with a dummy path
            try {
                String extension = getExtensionForCodec(codecName);
                if (extension != null) {
                    org.apache.hadoop.fs.Path dummyPath =
                            new org.apache.hadoop.fs.Path("dummy." + extension);
                    return codecFactory.getCodec(dummyPath);
                }
            } catch (Exception ignored) {
                // Fall through to return null
            }
        }
        return null;
    }

    /**
     * Gets file extension for a given codec class name.
     *
     * @param codecName The codec class name
     * @return File extension or null if not found
     */
    @Nullable
    private static String getExtensionForCodec(String codecName) {
        if (codecName.contains("Gzip")) {
            return "gz";
        } else if (codecName.contains("BZip2")) {
            return "bz2";
        } else if (codecName.contains("Deflate")) {
            return "deflate";
        } else if (codecName.contains("Snappy")) {
            return "snappy";
        } else if (codecName.contains("Lz4")) {
            return "lz4";
        } else if (codecName.contains("Lzo")) {
            return "lzo";
        } else if (codecName.contains("ZStandard")) {
            return "zst";
        }
        return null;
    }

    /**
     * Validates if the compression format is supported by checking if a codec can be created.
     *
     * @param compression The compression format string
     * @param options Paimon options for Hadoop configuration
     * @throws IllegalArgumentException If compression format is not supported
     */
    public static void validateCompressionFormat(String compression, Options options) {
        if (compression == null || compression.isEmpty() || "none".equalsIgnoreCase(compression)) {
            return; // No compression is always valid
        }

        Configuration conf = getHadoopConfiguration(options);
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        String codecName = mapCompressionName(compression);
        CompressionCodec codec = getCodecByName(codecFactory, codecName, conf);

        if (codec == null) {
            throw new IllegalArgumentException(
                    "Unsupported compression format: "
                            + compression
                            + ". Supported formats are: "
                            + String.join(", ", SUPPORTED_COMPRESSIONS));
        }
    }
}
