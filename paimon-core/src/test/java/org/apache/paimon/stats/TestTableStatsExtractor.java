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

package org.apache.paimon.stats;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.TableStatsCollector;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.FileUtils.createFormatReader;

/**
 * {@link TableStatsExtractor} for test. It reads all records from the file and use {@link
 * TableStatsCollector} to collect the stats.
 */
public class TestTableStatsExtractor implements TableStatsExtractor {

    private final FileFormat format;
    private final RowType rowType;
    private final FieldStatsCollector.Factory[] stats;

    public TestTableStatsExtractor(
            FileFormat format, RowType rowType, FieldStatsCollector.Factory[] stats) {
        this.format = format;
        this.rowType = rowType;
        this.stats = stats;
        Preconditions.checkArgument(
                rowType.getFieldCount() == stats.length,
                "The stats collector is not aligned to write schema.");
    }

    @Override
    public FieldStats[] extract(FileIO fileIO, Path path) throws IOException {
        return extractWithFileInfo(fileIO, path).getLeft();
    }

    @Override
    public Pair<FieldStats[], FileInfo> extractWithFileInfo(FileIO fileIO, Path path)
            throws IOException {
        IdentityObjectSerializer serializer = new IdentityObjectSerializer(rowType);
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType);
        List<InternalRow> records = readListFromFile(fileIO, path, serializer, readerFactory);

        TableStatsCollector statsCollector = new TableStatsCollector(rowType, stats);
        for (InternalRow record : records) {
            statsCollector.collect(record);
        }
        return Pair.of(statsCollector.extract(), new FileInfo(records.size()));
    }

    private static <T> List<T> readListFromFile(
            FileIO fileIO,
            Path path,
            ObjectSerializer<T> serializer,
            FormatReaderFactory readerFactory)
            throws IOException {
        List<T> result = new ArrayList<>();
        createFormatReader(fileIO, readerFactory, path, null)
                .forEachRemaining(row -> result.add(serializer.fromRow(row)));
        return result;
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
