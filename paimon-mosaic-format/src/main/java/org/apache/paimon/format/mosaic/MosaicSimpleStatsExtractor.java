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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.format.mosaic.MosaicObjects.convertStatsValue;

/** Extracts statistics from Mosaic file metadata. */
public class MosaicSimpleStatsExtractor implements SimpleStatsExtractor {

    private final RowType rowType;
    private final SimpleColStatsCollector.Factory[] statsCollectors;

    public MosaicSimpleStatsExtractor(
            RowType rowType, SimpleColStatsCollector.Factory[] statsCollectors) {
        this.rowType = rowType;
        this.statsCollectors = statsCollectors;
    }

    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
        MosaicInputFileAdapter inputFile = new MosaicInputFileAdapter(fileIO, path);
        try (BufferAllocator allocator = new RootAllocator();
                MosaicReader reader = MosaicReader.open(inputFile, length, allocator)) {
            return extractFromStats(reader.numRowGroups(), reader::getRowGroupStatistics);
        }
    }

    @Override
    public SimpleColStats[] extract(
            FileIO fileIO, Path path, long length, @Nullable Object writerMetadata) {
        if (writerMetadata instanceof MosaicWriterMetadata) {
            MosaicWriterMetadata meta = (MosaicWriterMetadata) writerMetadata;
            return extractFromStats(meta.numRowGroups(), meta::getRowGroupStatistics);
        }
        return extract(fileIO, path, length);
    }

    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
            FileIO fileIO, Path path, long length) {
        MosaicInputFileAdapter inputFile = new MosaicInputFileAdapter(fileIO, path);
        try (BufferAllocator allocator = new RootAllocator();
                MosaicReader reader = MosaicReader.open(inputFile, length, allocator)) {
            int numRowGroups = reader.numRowGroups();
            SimpleColStats[] stats = extractFromStats(numRowGroups, reader::getRowGroupStatistics);
            long rowCount = 0;
            for (int rg = 0; rg < numRowGroups; rg++) {
                rowCount += reader.rowGroupNumRows(rg);
            }
            return Pair.of(stats, new FileInfo(rowCount));
        }
    }

    @SuppressWarnings("unchecked")
    private SimpleColStats[] extractFromStats(
            int numRowGroups, RowGroupStatsProvider statsProvider) {
        int fieldCount = rowType.getFieldCount();
        Object[] minValues = new Object[fieldCount];
        Object[] maxValues = new Object[fieldCount];
        long[] nullCounts = new long[fieldCount];

        for (int rg = 0; rg < numRowGroups; rg++) {
            List<ColumnStatistics> stats = statsProvider.getRowGroupStatistics(rg);
            for (ColumnStatistics stat : stats) {
                int colIdx = stat.getColumnIndex();
                if (colIdx < 0 || colIdx >= fieldCount) {
                    continue;
                }

                nullCounts[colIdx] += stat.getNullCount();

                if (stat.hasMinMax()) {
                    DataType dataType = rowType.getFields().get(colIdx).type();
                    Object min = convertStatsValue(stat.getMin(), dataType);
                    Object max = convertStatsValue(stat.getMax(), dataType);
                    if (min != null) {
                        if (minValues[colIdx] == null) {
                            minValues[colIdx] = min;
                        } else {
                            if (((Comparable<Object>) min).compareTo(minValues[colIdx]) < 0) {
                                minValues[colIdx] = min;
                            }
                        }
                    }
                    if (max != null) {
                        if (maxValues[colIdx] == null) {
                            maxValues[colIdx] = max;
                        } else {
                            if (((Comparable<Object>) max).compareTo(maxValues[colIdx]) > 0) {
                                maxValues[colIdx] = max;
                            }
                        }
                    }
                }
            }
        }

        SimpleColStatsCollector[] collectors = SimpleColStatsCollector.create(statsCollectors);
        SimpleColStats[] result = new SimpleColStats[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            SimpleColStats fieldStats =
                    new SimpleColStats(minValues[i], maxValues[i], nullCounts[i]);
            result[i] = collectors[i].convert(fieldStats);
        }
        return result;
    }

    @FunctionalInterface
    private interface RowGroupStatsProvider {
        List<ColumnStatistics> getRowGroupStatistics(int rowGroupIndex);
    }
}
