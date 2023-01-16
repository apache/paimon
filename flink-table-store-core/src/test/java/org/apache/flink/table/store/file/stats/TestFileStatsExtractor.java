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

package org.apache.flink.table.store.file.stats;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.ObjectSerializer;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.format.FieldStatsCollector;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.store.format.FormatReaderFactory;
import org.apache.flink.table.store.types.RowType;

import java.io.IOException;
import java.util.List;

/**
 * {@link FileStatsExtractor} for test. It reads all records from the file and use {@link
 * FieldStatsCollector} to collect the stats.
 */
public class TestFileStatsExtractor implements FileStatsExtractor {

    private final FileFormat format;
    private final RowType rowType;

    public TestFileStatsExtractor(FileFormat format, RowType rowType) {
        this.format = format;
        this.rowType = rowType;
    }

    @Override
    public FieldStats[] extract(Path path) throws IOException {
        IdentityObjectSerializer serializer = new IdentityObjectSerializer(rowType);
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType);
        List<InternalRow> records = FileUtils.readListFromFile(path, serializer, readerFactory);
        FieldStatsCollector statsCollector = new FieldStatsCollector(rowType);
        for (InternalRow record : records) {
            statsCollector.collect(record);
        }
        return statsCollector.extract();
    }

    private static class IdentityObjectSerializer extends ObjectSerializer<InternalRow> {

        public IdentityObjectSerializer(RowType rowType) {
            super(rowType);
        }

        @Override
        public InternalRow toRow(InternalRow record) {
            return record;
        }

        @Override
        public InternalRow fromRow(InternalRow rowData) {
            return rowData;
        }
    }
}
