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

import static org.apache.paimon.options.description.TextElement.text;

/** Compression types supported by Paimon file formats. */
public enum TextCompressionType implements DescribedEnum {
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

    TextCompressionType(String value, String description, String className, String fileExtension) {
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

    /**
     * Maps compression type to Hadoop codec class name.
     *
     * @return Hadoop codec class name or null if not supported
     */
    public String hadoopCodecClassName() {
        return className;
    }

    /**
     * Gets file extension for the compression type.
     *
     * @return file extension or null for no compression
     */
    public String fileExtension() {
        return fileExtension;
    }

    /**
     * Get CompressionType from string value.
     *
     * @param value the string value
     * @return the corresponding CompressionType
     * @throws IllegalArgumentException if the value is not supported
     */
    public static TextCompressionType fromValue(String value) {
        if (value == null || value.isEmpty()) {
            return NONE;
        }

        for (TextCompressionType type : TextCompressionType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "Invalid compression type '%s'. Supported types are: %s",
                        value, getSupportedTypes()));
    }

    /**
     * Get all supported compression types as a comma-separated string.
     *
     * @return comma-separated string of supported types
     */
    public static String getSupportedTypes() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < TextCompressionType.values().length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(TextCompressionType.values()[i].value);
        }
        return sb.toString();
    }
}
