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

package org.apache.flink.table.store.orc;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.format.FileFormatImpl;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Optional;

/** Orc {@link FileFormat}. */
public class OrcFileFormat implements FileFormat {

    private final FileFormatImpl format;

    public OrcFileFormat(ClassLoader classLoader, ReadableConfig formatOptions) {
        this.format = new FileFormatImpl(classLoader, "orc", formatOptions);
    }

    @Override
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, int[][] projection, List<ResolvedExpression> filters) {
        return format.createReaderFactory(type, projection, filters);
    }

    @Override
    public BulkWriter.Factory<RowData> createWriterFactory(RowType type) {
        return format.createWriterFactory(type);
    }

    @Override
    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.of(new OrcFileStatsExtractor(type));
    }
}
