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

package org.apache.paimon.data.shredding;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Cross-file state for shared-shredding MAP columns. */
public class MapSharedShreddingContext {

    private static final int WINDOW_SIZE = 20;
    private static final double PERCENTILE_RATIO = 0.90;
    private static final int MAX_CLOSE_ABSOLUTE_SLACK = 4;
    private static final double MAX_CLOSE_RELATIVE_RATIO = 1.25;

    private final Map<String, Integer> columnToMaxColumns;
    private final Map<String, Deque<Integer>> recentMaxRowWidths;
    private final List<String> shreddingColumnNames;

    public MapSharedShreddingContext(Map<String, Integer> columnToMaxColumns) {
        this.columnToMaxColumns = new TreeMap<>(columnToMaxColumns);
        this.recentMaxRowWidths = new TreeMap<>();
        this.shreddingColumnNames =
                Collections.unmodifiableList(new ArrayList<>(this.columnToMaxColumns.keySet()));
    }

    /** Returns the physical column count K to use for each shared-shredding field. */
    public Map<String, Integer> computeNextK() {
        Map<String, Integer> result = new TreeMap<>();
        for (Map.Entry<String, Integer> entry : columnToMaxColumns.entrySet()) {
            String fieldName = entry.getKey();
            int maxColumns = entry.getValue();
            Deque<Integer> widths = recentMaxRowWidths.get(fieldName);
            if (widths == null || widths.isEmpty()) {
                result.put(fieldName, maxColumns);
            } else {
                int adaptiveWidth = computeAdaptiveWidth(new ArrayList<>(widths));
                result.put(fieldName, Math.max(1, Math.min(adaptiveWidth, maxColumns)));
            }
        }
        return result;
    }

    /** Reports one completed file's maximum row width for a shared-shredding field. */
    public void reportFileStats(String fieldName, int maxRowWidth) {
        Deque<Integer> widths =
                recentMaxRowWidths.computeIfAbsent(fieldName, ignored -> new ArrayDeque<>());
        widths.addLast(maxRowWidth);
        while (widths.size() > WINDOW_SIZE) {
            widths.removeFirst();
        }
    }

    public List<String> getShreddingColumnNames() {
        return shreddingColumnNames;
    }

    public boolean isEmpty() {
        return columnToMaxColumns.isEmpty();
    }

    private static int computeAdaptiveWidth(List<Integer> values) {
        checkArgument(!values.isEmpty(), "values should not be empty.");

        Collections.sort(values);
        int maxWidth = values.get(values.size() - 1);
        int percentileRank = (int) Math.ceil(PERCENTILE_RATIO * values.size());
        percentileRank = Math.max(1, Math.min(percentileRank, values.size()));
        int percentileWidth = values.get(percentileRank - 1);

        int relativeCloseThreshold =
                (int) Math.ceil((double) percentileWidth * MAX_CLOSE_RELATIVE_RATIO);
        if (maxWidth - percentileWidth <= MAX_CLOSE_ABSOLUTE_SLACK
                || maxWidth <= relativeCloseThreshold) {
            return maxWidth;
        }
        return percentileWidth;
    }
}
