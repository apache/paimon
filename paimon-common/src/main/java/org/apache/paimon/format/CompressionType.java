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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.options.description.TextElement.text;

/** Compression types supported by Paimon file formats. */
public enum CompressionType implements DescribedEnum {
    NONE("none", "No compression.", null, ""),
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
    private final String className;
    private final String fileExtension;
    private final String description;

    private static final Set<String> SUPPORTED_EXTENSIONS;

    static {
        Set<String> extensions = new HashSet<>();
        for (CompressionType type : CompressionType.values()) {
            if (type != CompressionType.NONE
                    && type.fileExtension() != null
                    && !type.fileExtension().isEmpty()) {
                extensions.add(type.fileExtension().toLowerCase());
            }
        }
        SUPPORTED_EXTENSIONS = Collections.unmodifiableSet(extensions);
    }

    CompressionType(String value, String description, String className, String fileExtension) {
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

    public String hadoopCodecClassName() {
        return className;
    }

    public String fileExtension() {
        return fileExtension;
    }

    public static CompressionType fromValue(String value) {
        if (value == null || value.isEmpty()) {
            return NONE;
        }

        for (CompressionType type : CompressionType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        return NONE;
    }

    /**
     * Check if the given extension is a supported compression extension.
     *
     * @param extension the file extension to check
     * @return true if the extension is a supported compression extension, false otherwise
     */
    public static boolean isSupportedExtension(String extension) {
        if (extension == null || extension.isEmpty()) {
            return false;
        }
        return SUPPORTED_EXTENSIONS.contains(extension.toLowerCase());
    }
}
