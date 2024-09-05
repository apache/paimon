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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

/** Factory to create {@link FileFormat}. */
public interface FileFormatFactory {

    String identifier();

    FileFormat create(FormatContext formatContext);

    /** the format context. */
    class FormatContext {

        private final Options formatOptions;
        private final int readBatchSize;
        private final int writeBatchSize;
        private final int zstdLevel;
        @Nullable private final MemorySize blockSize;

        @VisibleForTesting
        public FormatContext(Options formatOptions, int readBatchSize, int writeBatchSize) {
            this(formatOptions, readBatchSize, writeBatchSize, 1, null);
        }

        public FormatContext(
                Options formatOptions,
                int readBatchSize,
                int writeBatchSize,
                int zstdLevel,
                @Nullable MemorySize blockSize) {
            this.formatOptions = formatOptions;
            this.readBatchSize = readBatchSize;
            this.writeBatchSize = writeBatchSize;
            this.zstdLevel = zstdLevel;
            this.blockSize = blockSize;
        }

        public Options formatOptions() {
            return formatOptions;
        }

        public int readBatchSize() {
            return readBatchSize;
        }

        public int writeaBatchSize() {
            return writeBatchSize;
        }

        public int zstdLevel() {
            return zstdLevel;
        }

        @Nullable
        public MemorySize blockSize() {
            return blockSize;
        }
    }
}
