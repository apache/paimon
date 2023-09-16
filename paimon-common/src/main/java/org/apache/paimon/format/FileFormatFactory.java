/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;

/** Factory to create {@link FileFormat}. */
public interface FileFormatFactory {

    String identifier();

    FileFormat create(FormatContext formatContext);

    /** The format context for reader and writer. */
    class FormatContext {
        private final Options formatOptions;
        private final int readBatchSize;
        private final String compression;
        private final byte[] dataKey;
        private final byte[] dataAADPrefix;
        private final byte[] encryptedKey;
        private final CoreOptions.EncryptionAlgorithm encryptionAlgorithm;
        private final String encryptionColumns;

        private FormatContext(
                Options formatOptions,
                int readBatchSize,
                String compression,
                byte[] dataKey,
                byte[] dataAADPrefix,
                byte[] encryptedKey,
                CoreOptions.EncryptionAlgorithm encryptionAlgorithm,
                String encryptionColumns) {
            this.formatOptions = formatOptions;
            this.readBatchSize = readBatchSize;
            this.compression = compression;
            this.dataKey = dataKey;
            this.dataAADPrefix = dataAADPrefix;
            this.encryptedKey = encryptedKey;
            this.encryptionAlgorithm = encryptionAlgorithm;
            this.encryptionColumns = encryptionColumns;
        }

        public Options formatOptions() {
            return formatOptions;
        }

        public int readBatchSize() {
            return readBatchSize;
        }

        public String compression() {
            return compression;
        }

        public byte[] dataKey() {
            return dataKey;
        }

        public byte[] dataAADPrefix() {
            return dataAADPrefix;
        }

        public byte[] encryptedKey() {
            return encryptedKey;
        }

        public CoreOptions.EncryptionAlgorithm encryptionAlgorithm() {
            return encryptionAlgorithm;
        }

        public String encryptionColumns() {
            return encryptionColumns;
        }
    }

    /** FormatContextBuilder. */
    class FormatContextBuilder {

        private Options formatOptions;
        private int readBatchSize;
        private String compression;
        private byte[] dataKey;
        private byte[] dataAADPrefix;
        private byte[] encryptedKey;
        private CoreOptions.EncryptionAlgorithm encryptionAlgorithm;
        private String encryptionColumns;

        private FormatContextBuilder() {}

        public FormatContextBuilder formatOptions(Options formatOptions) {
            this.formatOptions = formatOptions;
            return this;
        }

        public FormatContextBuilder readBatchSize(int readBatchSize) {
            this.readBatchSize = readBatchSize;
            return this;
        }

        public FormatContextBuilder compression(String compression) {
            this.compression = compression;
            return this;
        }

        public FormatContextBuilder withDataKey(byte[] dataKey) {
            this.dataKey = dataKey;
            return this;
        }

        public FormatContextBuilder withAADPrefix(byte[] dataAADPrefix) {
            this.dataAADPrefix = dataAADPrefix;
            return this;
        }

        public FormatContextBuilder withEncryptedKey(byte[] encryptedKey) {
            this.encryptedKey = encryptedKey;
            return this;
        }

        public FormatContextBuilder withEncryptionAlgorithm(
                CoreOptions.EncryptionAlgorithm encryptionAlgorithm) {
            this.encryptionAlgorithm = encryptionAlgorithm;
            return this;
        }

        public FormatContextBuilder withEncryptionColumns(String encryptionColumns) {
            this.encryptionColumns = encryptionColumns;
            return this;
        }

        public FormatContext build() {
            return new FormatContext(
                    formatOptions,
                    readBatchSize,
                    compression,
                    dataKey,
                    dataAADPrefix,
                    encryptedKey,
                    encryptionAlgorithm,
                    encryptionColumns);
        }
    }

    static FormatContextBuilder formatContextBuilder() {
        return new FormatContextBuilder();
    }
}
