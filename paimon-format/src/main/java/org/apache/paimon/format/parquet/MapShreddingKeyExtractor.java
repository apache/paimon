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

package org.apache.paimon.format.parquet;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.InternalRowToSizeVisitor;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Extract file-level hot keys for map shredding columns. */
public class MapShreddingKeyExtractor {

    private final List<MapShreddingUtils.ResolvedMapShreddingColumn> columns;
    private final InternalRowSerializer serializer;
    private final int maxKeys;
    private final long memoryLimit;
    private final int rowLimit;
    private final List<InternalRow> bufferedRows;
    private final Map<String, Map<String, Long>> keyValueSizes;
    private final List<BiFunction<InternalRow, Integer, Integer>> rowFieldSizeVisitors;

    private boolean finished;
    private long currentBufferedSize;

    public MapShreddingKeyExtractor(RowType rowType, Options options) {
        this.columns =
                MapShreddingUtils.resolveColumns(
                        rowType,
                        MapShreddingUtils.parseColumnPaths(
                                options.get(CoreOptions.MAP_SHREDDING_COLUMNS)));
        this.serializer = new InternalRowSerializer(rowType);
        this.maxKeys = options.get(CoreOptions.MAP_SHREDDING_MAX_KEYS);
        this.memoryLimit =
                options.get(CoreOptions.MAP_SHREDDING_MAX_INFER_BUFFER_MEMORY).getBytes();
        this.rowLimit = options.get(CoreOptions.MAP_SHREDDING_MAX_INFER_BUFFER_ROW);
        this.bufferedRows = new ArrayList<>();
        this.keyValueSizes = new LinkedHashMap<>();
        this.rowFieldSizeVisitors = createRowFieldSizeVisitors(rowType);

        if (columns.isEmpty() || maxKeys <= 0 || memoryLimit <= 0 || rowLimit <= 0) {
            this.finished = true;
        } else {
            columns.forEach(column -> keyValueSizes.put(column.path(), new LinkedHashMap<>()));
        }
    }

    public boolean finished() {
        return finished;
    }

    public List<InternalRow> bufferedRows() {
        return bufferedRows;
    }

    public void add(InternalRow row) {
        if (finished) {
            throw new IllegalStateException("MapShreddingKeyExtractor is already finished.");
        }

        InternalRow copied = serializer.copy(row);
        bufferedRows.add(copied);
        currentBufferedSize += estimateRowSize(copied);

        for (MapShreddingUtils.ResolvedMapShreddingColumn column : columns) {
            InternalMap map = MapShreddingUtils.extractMap(copied, column);
            if (map == null) {
                continue;
            }

            InternalArray keyArray = map.keyArray();
            InternalArray valueArray = map.valueArray();
            BiFunction<DataGetters, Integer, Integer> valueSizer =
                    column.valueType().accept(new InternalRowToSizeVisitor());
            Map<String, Long> sizes = keyValueSizes.get(column.path());
            for (int i = 0; i < map.size(); i++) {
                if (keyArray.isNullAt(i)) {
                    continue;
                }
                BinaryString key = keyArray.getString(i);
                long valueSize = valueSizer.apply(valueArray, i);
                sizes.merge(key.toString(), valueSize, Long::sum);
            }
        }

        if (bufferedRows.size() >= rowLimit || currentBufferedSize >= memoryLimit) {
            populate();
        }
    }

    public Map<String, List<String>> populate() {
        Map<String, List<String>> result = currentDynamicKeys();
        finished = true;
        return result;
    }

    public Map<String, List<String>> finish() {
        return finished ? currentDynamicKeys() : populate();
    }

    public Map<String, List<String>> currentDynamicKeys() {
        Map<String, List<String>> result = new LinkedHashMap<>();
        keyValueSizes.forEach(
                (path, sizes) ->
                        result.put(
                                path,
                                sizes.entrySet().stream()
                                        .sorted(
                                                Map.Entry.<String, Long>comparingByValue()
                                                        .reversed())
                                        .limit(maxKeys)
                                        .map(Map.Entry::getKey)
                                        .collect(Collectors.toList())));
        return result;
    }

    private long estimateRowSize(InternalRow row) {
        long size = 0L;
        for (int i = 0; i < rowFieldSizeVisitors.size(); i++) {
            size += rowFieldSizeVisitors.get(i).apply(row, i);
        }
        return size;
    }

    private List<BiFunction<InternalRow, Integer, Integer>> createRowFieldSizeVisitors(
            RowType rowType) {
        List<BiFunction<InternalRow, Integer, Integer>> visitors = new ArrayList<>();
        for (DataType fieldType : rowType.getFieldTypes()) {
            BiFunction<DataGetters, Integer, Integer> visitor =
                    fieldType.accept(new InternalRowToSizeVisitor());
            visitors.add((row, pos) -> visitor.apply(row, pos));
        }
        return visitors;
    }
}
