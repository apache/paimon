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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Per-row physical column allocator for one shared-shredding MAP column.
 *
 * <p>This is a simple temporary implementation which assigns fields to physical columns by row
 * order. A later version will use a more sophisticated LRU-style allocator to improve column reuse
 * across rows.
 */
public class MapSharedShreddingColumnAllocator {

    private final int numColumns;
    private final Map<Integer, Set<Integer>> fieldToColumns = new TreeMap<>();
    private final Set<Integer> overflowFieldSet = new TreeSet<>();
    private int maxRowWidth = 0;

    public MapSharedShreddingColumnAllocator(int numColumns) {
        this.numColumns = numColumns;
    }

    public RowAllocation allocateRow(List<Integer> fieldIds) {
        maxRowWidth = Math.max(maxRowWidth, fieldIds.size());

        int[] colToField = new int[numColumns];
        for (int i = 0; i < numColumns; i++) {
            colToField[i] = -1;
        }

        int assignLimit = Math.min(fieldIds.size(), numColumns);
        for (int i = 0; i < assignLimit; i++) {
            int fieldId = fieldIds.get(i);
            colToField[i] = fieldId;
            fieldToColumns.computeIfAbsent(fieldId, ignored -> new TreeSet<>()).add(i);
        }

        List<Integer> overflowFields = new ArrayList<>();
        for (int i = assignLimit; i < fieldIds.size(); i++) {
            int fieldId = fieldIds.get(i);
            overflowFields.add(fieldId);
            overflowFieldSet.add(fieldId);
        }

        return new RowAllocation(colToField, overflowFields);
    }

    public Map<Integer, List<Integer>> fieldToColumns() {
        Map<Integer, List<Integer>> result = new TreeMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : fieldToColumns.entrySet()) {
            result.put(
                    entry.getKey(),
                    Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(result);
    }

    public Set<Integer> overflowFieldSet() {
        return Collections.unmodifiableSet(overflowFieldSet);
    }

    public int maxRowWidth() {
        return maxRowWidth;
    }

    public int numColumns() {
        return numColumns;
    }

    /** Physical column allocation for one row. */
    public static class RowAllocation {

        private final int[] colToField;
        private final List<Integer> overflowFields;

        private RowAllocation(int[] colToField, List<Integer> overflowFields) {
            this.colToField = colToField;
            this.overflowFields = Collections.unmodifiableList(overflowFields);
        }

        public int[] colToField() {
            return colToField;
        }

        public List<Integer> overflowFields() {
            return overflowFields;
        }
    }
}
