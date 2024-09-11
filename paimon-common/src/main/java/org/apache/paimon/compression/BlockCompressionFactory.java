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

package org.apache.paimon.compression;

import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;

import javax.annotation.Nullable;

/**
 * Each compression codec has an implementation of {@link BlockCompressionFactory} to create
 * compressors and decompressors.
 */
public interface BlockCompressionFactory {

    BlockCompressionType getCompressionType();

    BlockCompressor getCompressor();

    BlockDecompressor getDecompressor();

    /** Creates {@link BlockCompressionFactory} according to the configuration. */
    @Nullable
    static BlockCompressionFactory create(CompressOptions compression) {
        switch (compression.compress().toUpperCase()) {
            case "NONE":
                return null;
            case "ZSTD":
                return new ZstdBlockCompressionFactory(compression.zstdLevel());
            case "LZ4":
                return new Lz4BlockCompressionFactory();
            case "LZO":
                return new AirCompressorFactory(
                        BlockCompressionType.LZO, new LzoCompressor(), new LzoDecompressor());
            default:
                throw new IllegalStateException("Unknown CompressionMethod " + compression);
        }
    }

    /** Creates {@link BlockCompressionFactory} according to the {@link BlockCompressionType}. */
    @Nullable
    static BlockCompressionFactory create(BlockCompressionType compression) {
        switch (compression) {
            case NONE:
                return null;
            case ZSTD:
                return new ZstdBlockCompressionFactory(1);
            case LZ4:
                return new Lz4BlockCompressionFactory();
            case LZO:
                return new AirCompressorFactory(
                        BlockCompressionType.LZO, new LzoCompressor(), new LzoDecompressor());
            default:
                throw new IllegalStateException("Unknown CompressionMethod " + compression);
        }
    }
}
