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

/**
 * Allocator that retains resident field-to-column assignments and evicts the least recently used
 * column when necessary.
 */
public class LruMapSharedShreddingColumnAllocator extends MapSharedShreddingColumnAllocator {

    private final int[] residentFieldByColumn;
    private final long[] lastUsed;
    private long lruClock;

    public LruMapSharedShreddingColumnAllocator(int numColumns) {
        super(numColumns);
        this.residentFieldByColumn = emptyColumnMapping();
        this.lastUsed = new long[numColumns];
    }

    @Override
    public RowAllocation allocateRow(List<Integer> fieldIds) {
        List<Integer> sortedFieldIds = new ArrayList<>(fieldIds);
        Collections.sort(sortedFieldIds);

        int[] colToField = emptyColumnMapping();
        int[] nextResidentFieldByColumn = residentFieldByColumn.clone();
        boolean[] usedColumns = new boolean[numColumns];
        List<Integer> unassignedFields = new ArrayList<>();

        for (Integer fieldId : sortedFieldIds) {
            int column = findResidentColumn(fieldId);
            if (column == -1) {
                unassignedFields.add(fieldId);
            } else {
                usedColumns[column] = true;
                colToField[column] = fieldId;
            }
        }

        List<Integer> overflowFields = new ArrayList<>();
        for (Integer fieldId : unassignedFields) {
            int column = selectColumn(usedColumns, nextResidentFieldByColumn);
            if (column == -1) {
                overflowFields.add(fieldId);
                continue;
            }

            usedColumns[column] = true;
            colToField[column] = fieldId;
            nextResidentFieldByColumn[column] = fieldId;
        }

        RowAllocation allocation = new RowAllocation(colToField, overflowFields);
        updateLastUsed(colToField);
        System.arraycopy(nextResidentFieldByColumn, 0, residentFieldByColumn, 0, numColumns);
        commitRow(allocation, sortedFieldIds);
        return allocation;
    }

    private int findResidentColumn(int fieldId) {
        for (int column = 0; column < numColumns; column++) {
            if (residentFieldByColumn[column] == fieldId) {
                return column;
            }
        }
        return -1;
    }

    private int selectColumn(boolean[] usedColumns, int[] plannedResidentFieldByColumn) {
        int selectedColumn = -1;
        long selectedLastUsed = Long.MAX_VALUE;
        for (int column = 0; column < numColumns; column++) {
            if (usedColumns[column]) {
                continue;
            }
            if (plannedResidentFieldByColumn[column] == -1) {
                return column;
            }
            if (lastUsed[column] < selectedLastUsed) {
                selectedColumn = column;
                selectedLastUsed = lastUsed[column];
            }
        }
        return selectedColumn;
    }

    private void updateLastUsed(int[] colToField) {
        boolean touched = false;
        for (int column = 0; column < numColumns; column++) {
            if (colToField[column] != -1) {
                lastUsed[column] = lruClock;
                touched = true;
            }
        }
        if (touched) {
            lruClock++;
        }
    }
}
