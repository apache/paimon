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

package org.apache.paimon.globalindex;

import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

public class IndexedSplitReadUtil {

    public static Info readInfo(RowType readRowType, IndexedSplit indexedSplit) {
        Map<Long, Float> rowIdToScore = null;
        if (indexedSplit.scores() != null) {
            rowIdToScore = new HashMap<>();
            int index = 0;
            for (Range range : indexedSplit.rowRanges()) {
                for (long i = range.from; i <= range.to; i++) {
                    rowIdToScore.put(i, indexedSplit.scores()[index++]);
                }
            }
        }

        int rowIdIndex = readRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        RowType actualReadType = readRowType;
        ProjectedRow projectedRow = null;

        if (rowIdToScore != null && rowIdIndex == -1) {
            actualReadType = SpecialFields.rowTypeWithRowId(readRowType);
            rowIdIndex = actualReadType.getFieldCount() - 1;
            int[] mappings = new int[readRowType.getFieldCount()];
            for (int i = 0; i < readRowType.getFieldCount(); i++) {
                mappings[i] = i;
            }
            projectedRow = ProjectedRow.from(mappings);
        }

        return new Info(rowIdToScore, rowIdIndex, actualReadType, projectedRow);
    }

    public static class Info {
        @Nullable public final Map<Long, Float> rowIdToScore;
        public final int rowIdIndex;
        public final RowType actualReadType;
        @Nullable public final ProjectedRow projectedRow;

        public Info(
                @Nullable Map<Long, Float> rowIdToScore,
                int rowIdIndex,
                RowType actualReadType,
                @Nullable ProjectedRow projectedRow) {
            this.rowIdToScore = rowIdToScore;
            this.rowIdIndex = rowIdIndex;
            this.actualReadType = actualReadType;
            this.projectedRow = projectedRow;
        }
    }
}
