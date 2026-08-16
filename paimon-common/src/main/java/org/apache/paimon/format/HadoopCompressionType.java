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

import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.paimon.options.description.TextElement.text;

/** Compression types supported by hadoop compression. */
public enum HadoopCompressionType implements DescribedEnum {
    NONE("none", "No compression.", null, null),
    GZIP(
            "gzip",
            "GZIP compression using the deflate algorithm.",
            "org.apache.hadoop.io.compress.GzipCodec",
            "gz"),
    BZIP2(
            "bzip2",
            "BZIP2 compression using the Burrows-Wheeler algorithm.",
            "org.apache.hadoop.io.compress.BZip2Codec",
            "bz2"),
    DEFLATE(
            "deflate",
            "DEFLATE compression using the deflate algorithm.",
            "org.apache.hadoop.io.compress.DeflateCodec",
            "deflate"),
    SNAPPY(
            "snappy",
            "Snappy compression for fast compression and decompression.",
            "org.apache.hadoop.io.compress.SnappyCodec",
            "snappy"),
    LZ4(
            "lz4",
            "LZ4 compression for very fast compression and decompression.",
            "org.apache.hadoop.io.compress.Lz4Codec",
            "lz4"),
    ZSTD(
            "zstd",
            "Zstandard compression for high compression ratio and speed.",
            "org.apache.hadoop.io.compress.ZStandardCodec",
            "zst");

    private final String value;
    private final String description;
    private final @Nullable String className;
    private final @Nullable String fileExtension;

    HadoopCompressionType(
            String value,
            String description,
            @Nullable String className,
            @Nullable String fileExtension) {
        this.value = value;
        this.description = description;
        this.className = className;
        this.fileExtension = fileExtension;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return text(description);
    }

    public String value() {
        return value;
    }

    @Nullable
    public String hadoopCodecClassName() {
        return className;
    }

    @Nullable
    public String fileExtension() {
        return fileExtension;
    }

    public static Optional<HadoopCompressionType> fromValue(String value) {
        for (HadoopCompressionType type : HadoopCompressionType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    public static boolean isCompressExtension(String extension) {
        for (HadoopCompressionType type : HadoopCompressionType.values()) {
            if (extension.equalsIgnoreCase(type.fileExtension)) {
                return true;
            }
        }
        return false;
    }
}
