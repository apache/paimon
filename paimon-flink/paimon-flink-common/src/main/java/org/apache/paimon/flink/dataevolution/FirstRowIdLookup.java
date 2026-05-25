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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.utils.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Lookup related first row id for each row id through binary search. */
public class FirstRowIdLookup implements Serializable {
    // a sorted list of first row ids
    private final List<Long> firstRowIds;

    public FirstRowIdLookup(List<Long> firstRowIds) {
        this.firstRowIds = firstRowIds;
    }

    public long lookup(long rowId) {
        int index = Collections.binarySearch(firstRowIds, rowId);
        long firstRowId;
        if (index >= 0) {
            firstRowId = firstRowIds.get(index);
        } else {
            // (-index - 1) is the position of the first element greater than the rowId
            Preconditions.checkState(
                    -index - 2 >= 0,
                    String.format(
                            "Unexpected RowID: %s which is smaller than the smallest FirstRowId: %s",
                            rowId, firstRowIds.get(0)));
            firstRowId = firstRowIds.get(-index - 2);
        }
        return firstRowId;
    }
}
