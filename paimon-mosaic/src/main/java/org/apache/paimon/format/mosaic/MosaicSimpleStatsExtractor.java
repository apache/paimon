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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        try (MosaicInputFileAdapter inputFile = new MosaicInputFileAdapter(fileIO, path);
                BufferAllocator allocator = new RootAllocator();
                MosaicReader reader = MosaicReader.open(inputFile, length, allocator)) {
            return extractFromStats(reader.numRowGroups(), reader::getRowGroupStatistics, null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract stats from " + path, e);
        }
    }

    @Override
    public SimpleColStats[] extract(
            FileIO fileIO, Path path, long length, @Nullable Object writerMetadata) {
        if (writerMetadata instanceof MosaicWriterMetadata) {
            MosaicWriterMetadata meta = (MosaicWriterMetadata) writerMetadata;
            Set<Integer> statsFieldIndices = resolveStatsFieldIndices(meta.statsColumnNames());
            return extractFromStats(
                    meta.numRowGroups(), meta::getRowGroupStatistics, statsFieldIndices);
        }
        return extract(fileIO, path, length);
    }

    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
            FileIO fileIO, Path path, long length) {
        try (MosaicInputFileAdapter inputFile = new MosaicInputFileAdapter(fileIO, path);
                BufferAllocator allocator = new RootAllocator();
                MosaicReader reader = MosaicReader.open(inputFile, length, allocator)) {
            int numRowGroups = reader.numRowGroups();
            SimpleColStats[] stats =
                    extractFromStats(numRowGroups, reader::getRowGroupStatistics, null);
            long rowCount = 0;
            for (int rg = 0; rg < numRowGroups; rg++) {
                rowCount += reader.rowGroupNumRows(rg);
            }
            return Pair.of(stats, new FileInfo(rowCount));
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract stats from " + path, e);
        }
    }

    @SuppressWarnings("unchecked")
    private SimpleColStats[] extractFromStats(
            int numRowGroups,
            RowGroupStatsProvider statsProvider,
            @Nullable Set<Integer> statsFieldIndices) {
        int fieldCount = rowType.getFieldCount();
        List<String> fieldNames = rowType.getFieldNames();
        Object[] minValues = new Object[fieldCount];
        Object[] maxValues = new Object[fieldCount];
        long[] nullCounts = new long[fieldCount];
        Set<Integer> seenColumns = new HashSet<>();

        for (int rg = 0; rg < numRowGroups; rg++) {
            Map<String, ColumnStatistics> statsMap = statsProvider.getRowGroupStatistics(rg);
            for (Map.Entry<String, ColumnStatistics> entry : statsMap.entrySet()) {
                int colIdx = fieldNames.indexOf(entry.getKey());
                if (colIdx < 0) {
                    continue;
                }

                ColumnStatistics stat = entry.getValue();
                seenColumns.add(colIdx);
                nullCounts[colIdx] += stat.getNullCount();

                if (stat.hasMinMax()) {
                    DataType dataType = rowType.getFields().get(colIdx).type();
                    Object min = convertStatsValue(stat.getMin(), dataType);
                    Object max = convertStatsValue(stat.getMax(), dataType);
                    if (min instanceof Comparable) {
                        if (minValues[colIdx] == null) {
                            minValues[colIdx] = min;
                        } else {
                            if (((Comparable<Object>) min).compareTo(minValues[colIdx]) < 0) {
                                minValues[colIdx] = min;
                            }
                        }
                    }
                    if (max instanceof Comparable) {
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

        Set<Integer> trackedColumns = statsFieldIndices != null ? statsFieldIndices : seenColumns;
        SimpleColStatsCollector[] collectors = SimpleColStatsCollector.create(statsCollectors);
        SimpleColStats[] result = new SimpleColStats[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            if (!trackedColumns.contains(i) || !seenColumns.contains(i)) {
                result[i] = collectors[i].convert(new SimpleColStats(null, null, null));
            } else {
                SimpleColStats fieldStats =
                        new SimpleColStats(minValues[i], maxValues[i], nullCounts[i]);
                result[i] = collectors[i].convert(fieldStats);
            }
        }
        return result;
    }

    private Set<Integer> resolveStatsFieldIndices(List<String> statsColumnNames) {
        Set<Integer> indices = new HashSet<>();
        List<String> fieldNames = rowType.getFieldNames();
        for (String name : statsColumnNames) {
            int idx = fieldNames.indexOf(name);
            if (idx >= 0) {
                indices.add(idx);
            }
        }
        return indices;
    }

    @FunctionalInterface
    private interface RowGroupStatsProvider {
        Map<String, ColumnStatistics> getRowGroupStatistics(int rowGroupIndex);
    }
}
