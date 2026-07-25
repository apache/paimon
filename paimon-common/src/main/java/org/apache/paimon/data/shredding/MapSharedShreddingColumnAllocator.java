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
 * <p>Implementations decide the physical column placement for each row. This base class accumulates
 * the file-level metadata shared by all placement policies.
 */
public abstract class MapSharedShreddingColumnAllocator {

    protected final int numColumns;
    private final Map<Integer, Set<Integer>> fieldToColumns = new TreeMap<>();
    private final Set<Integer> overflowFieldSet = new TreeSet<>();
    private int maxRowWidth = 0;

    protected MapSharedShreddingColumnAllocator(int numColumns) {
        this.numColumns = numColumns;
    }

    /** Allocates physical columns for one row's field IDs. */
    public abstract RowAllocation allocateRow(List<Integer> fieldIds);

    /** Commits one row allocation and updates accumulated file-level metadata. */
    protected void commitRow(RowAllocation allocation, List<Integer> fieldIds) {
        maxRowWidth = Math.max(maxRowWidth, fieldIds.size());

        for (int column = 0; column < numColumns; column++) {
            int fieldId = allocation.colToField[column];
            if (fieldId != -1) {
                fieldToColumns.computeIfAbsent(fieldId, ignored -> new TreeSet<>()).add(column);
            }
        }

        overflowFieldSet.addAll(allocation.overflowFields);
    }

    protected int[] emptyColumnMapping() {
        int[] colToField = new int[numColumns];
        for (int i = 0; i < numColumns; i++) {
            colToField[i] = -1;
        }
        return colToField;
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

        RowAllocation(int[] colToField, List<Integer> overflowFields) {
            this.colToField = colToField.clone();
            this.overflowFields = Collections.unmodifiableList(new ArrayList<>(overflowFields));
        }

        public int[] colToField() {
            return colToField.clone();
        }

        public List<Integer> overflowFields() {
            return overflowFields;
        }
    }
}
