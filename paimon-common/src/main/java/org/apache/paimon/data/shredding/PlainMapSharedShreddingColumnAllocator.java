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
import java.util.List;

/** Allocator that maps fields to physical columns in the input MAP entry order. */
public class PlainMapSharedShreddingColumnAllocator extends MapSharedShreddingColumnAllocator {

    public PlainMapSharedShreddingColumnAllocator(int numColumns) {
        super(numColumns);
    }

    @Override
    public RowAllocation allocateRow(List<Integer> fieldIds) {
        RowAllocation allocation = allocateLeadingColumns(fieldIds);
        commitRow(allocation, fieldIds);
        return allocation;
    }

    protected RowAllocation allocateLeadingColumns(List<Integer> fieldIds) {
        int[] colToField = emptyColumnMapping();
        List<Integer> overflowFields = new ArrayList<>();
        for (int i = 0; i < fieldIds.size(); i++) {
            int fieldId = fieldIds.get(i);
            if (i < numColumns) {
                colToField[i] = fieldId;
            } else {
                overflowFields.add(fieldId);
            }
        }
        return new RowAllocation(colToField, overflowFields);
    }
}
