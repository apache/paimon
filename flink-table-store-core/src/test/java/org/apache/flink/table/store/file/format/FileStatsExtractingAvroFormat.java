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

package org.apache.flink.table.store.file.format;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.store.file.stats.TestFileStatsExtractor;
import org.apache.flink.table.store.file.writer.FormatWriter;
import org.apache.flink.table.store.file.writer.RowFormatWriter;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** An avro {@link FileFormat} for test. It provides a {@link FileStatsExtractor}. */
public class FileStatsExtractingAvroFormat extends FileFormatImpl {

    public FileStatsExtractingAvroFormat() {
        super(FileStatsExtractingAvroFormat.class.getClassLoader(), "avro", new Configuration());
    }

    @Override
    public FormatWriter.Factory<RowData> createWriterFactory(RowType writeSchema) {
        BulkWriter.Factory<RowData> bulkWriter =
                writerFactory
                        .createEncodingFormat(null, formatOptions)
                        .createRuntimeEncoder(SINK_CONTEXT, fromLogicalToDataType(writeSchema));

        FileStatsExtractor extractor = new TestFileStatsExtractor(this, writeSchema);
        return new RowFormatWriter.RowFormatWriterFactory(bulkWriter, writeSchema, extractor);
    }
}
