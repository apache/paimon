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
        private final String encryptionTableKeyId;
        private final byte[] plaintextDataKey;
        private final byte[] dataAADPrefix;
        private final String encryptionAlgorithm;
        private final String encryptionColumns;

        private FormatContext(
                Options formatOptions,
                int readBatchSize,
                String compression,
                String encryptionTableKeyId,
                byte[] plaintextDataKey,
                byte[] dataAADPrefix,
                String encryptionAlgorithm,
                String encryptionColumns) {
            this.formatOptions = formatOptions;
            this.readBatchSize = readBatchSize;
            this.compression = compression;
            this.encryptionTableKeyId = encryptionTableKeyId;
            this.plaintextDataKey = plaintextDataKey;
            this.dataAADPrefix = dataAADPrefix;
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

        public String encryptionTableId() {
            return encryptionTableKeyId;
        }

        public byte[] plaintextDataKey() {
            return plaintextDataKey;
        }

        public byte[] dataAADPrefix() {
            return dataAADPrefix;
        }

        public String encryptionAlgorithm() {
            return encryptionAlgorithm;
        }

        public String encryptionColumns() {
            return encryptionColumns;
        }

        public FormatContext newDataKey(byte[] dataKey) {
            return new FormatContext(
                    formatOptions,
                    readBatchSize,
                    compression,
                    encryptionTableKeyId,
                    dataKey,
                    dataAADPrefix,
                    encryptionAlgorithm,
                    encryptionColumns);
        }
    }

    /** Format context builder. */
    class FormatContextBuilder {

        private Options formatOptions;
        private int readBatchSize;
        private String compression;
        private String encryptionTableId;
        private byte[] plaintextDataKey;
        private byte[] dataAADPrefix;
        private String encryptionAlgorithm;
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

        public FormatContextBuilder withEncryptionTableId(String keyId) {
            this.encryptionTableId = keyId;
            return this;
        }

        public FormatContextBuilder withPlaintextDataKey(byte[] plaintextDataKey) {
            this.plaintextDataKey = plaintextDataKey;
            return this;
        }

        public FormatContextBuilder withAADPrefix(byte[] dataAADPrefix) {
            this.dataAADPrefix = dataAADPrefix;
            return this;
        }

        public FormatContextBuilder withEncryptionAlgorithm(String encryptionAlgorithm) {
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
                    encryptionTableId,
                    plaintextDataKey,
                    dataAADPrefix,
                    encryptionAlgorithm,
                    encryptionColumns);
        }
    }

    static FormatContextBuilder formatContextBuilder() {
        return new FormatContextBuilder();
    }
}
