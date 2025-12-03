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

package org.apache.paimon.format.text;

import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

/** Utility class for handling file compression and decompression using Hadoop codecs. */
public class HadoopCompressionUtils {

    /**
     * Creates a compressed output stream using Hadoop's compression codecs.
     *
     * @param out The underlying output stream
     * @param compression The compression format
     * @return Compressed output stream
     * @throws IOException If compression stream creation fails
     */
    public static OutputStream createCompressedOutputStream(
            PositionOutputStream out, String compression) throws IOException {
        Optional<CompressionCodec> codecOpt = getCompressionCodecByCompression(compression);
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
     * @return Decompressed input stream
     * @throws IOException If decompression stream creation fails
     */
    public static InputStream createDecompressedInputStream(
            SeekableInputStream inputStream, Path filePath) throws IOException {
        try {
            // Handle null filePath gracefully
            if (filePath == null) {
                return inputStream;
            }

            CompressionCodec codec = getCompressionCodec(filePath);
            if (codec != null) {
                return codec.createInputStream(inputStream);
            }
            return inputStream;
        } catch (Exception | UnsatisfiedLinkError e) {
            throw new RuntimeException("Failed to create decompression stream", e);
        }
    }

    public static boolean isCompressed(Path filePath) {
        return getCompressionCodec(filePath) != null;
    }

    public static CompressionCodec getCompressionCodec(Path filePath) {
        CompressionCodecFactory codecFactory =
                new CompressionCodecFactory(new Configuration(false));
        return codecFactory.getCodec(new org.apache.hadoop.fs.Path(filePath.toString()));
    }

    /**
     * Gets a compression codec by compression type.
     *
     * @param compression The compression type
     * @return Optional CompressionCodec instance
     */
    public static Optional<CompressionCodec> getCompressionCodecByCompression(String compression) {
        HadoopCompressionType compressionType =
                HadoopCompressionType.fromValue(compression)
                        .orElseThrow(IllegalArgumentException::new);
        if (HadoopCompressionType.NONE == compressionType) {
            return Optional.empty();
        }

        try {
            String codecName = compressionType.hadoopCodecClassName();
            Class<?> codecClass = Class.forName(codecName);
            CompressionCodec codec =
                    (CompressionCodec) codecClass.getDeclaredConstructor().newInstance();

            // To fix npe when the codec implements Configurable
            if (codec instanceof Configurable) {
                ((Configurable) codec).setConf(new Configuration());
            }

            codec.createOutputStream(new java.io.ByteArrayOutputStream());
            return Optional.of(codec);
        } catch (Exception | UnsatisfiedLinkError e) {
            throw new RuntimeException("Failed to get compression codec", e);
        }
    }
}
