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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.shredding.ShreddingWritePlanFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Creates per-file shared-shredding MAP write plans. */
public class MapSharedShreddingWritePlanFactory implements ShreddingWritePlanFactory {

    private static final int INFER_BUFFER_ROW_COUNT = 1;

    private final RowType logicalRowType;
    private final Map<String, Integer> fieldToMaxColumns;
    private final Map<String, Integer> fieldToPosition;

    public MapSharedShreddingWritePlanFactory(RowType logicalRowType, Options options) {
        this.logicalRowType = logicalRowType;
        CoreOptions coreOptions = new CoreOptions(options);
        List<String> shreddingFields =
                MapSharedShreddingUtils.detectShreddingColumns(logicalRowType, coreOptions);
        this.fieldToMaxColumns =
                MapSharedShreddingUtils.buildColumnToNumColumns(shreddingFields, coreOptions);
        this.fieldToPosition = new LinkedHashMap<>();
        for (String field : shreddingFields) {
            fieldToPosition.put(field, logicalRowType.getFieldIndex(field));
        }
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public boolean shouldCreateWritePlan() {
        return !fieldToMaxColumns.isEmpty();
    }

    @Override
    public boolean shouldInferWritePlan() {
        return shouldCreateWritePlan();
    }

    @Override
    public int inferBufferRowCount() {
        return INFER_BUFFER_ROW_COUNT;
    }

    @Override
    public ShreddingWritePlan createWritePlan(List<InternalRow> sampleRows) {
        checkArgument(shouldCreateWritePlan(), "MAP shared-shredding write plan is not active.");

        Map<String, Integer> fieldToNumColumns = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : fieldToMaxColumns.entrySet()) {
            int maxColumns = entry.getValue();
            int numColumns = maxColumns;
            if (!sampleRows.isEmpty()) {
                int fieldPosition = fieldToPosition.get(entry.getKey());
                int maxRowWidth = 0;
                int sampleCount = Math.min(sampleRows.size(), INFER_BUFFER_ROW_COUNT);
                for (int i = 0; i < sampleCount; i++) {
                    InternalRow row = sampleRows.get(i);
                    InternalMap map =
                            row.isNullAt(fieldPosition) ? null : row.getMap(fieldPosition);
                    maxRowWidth = Math.max(maxRowWidth, map == null ? 0 : map.size());
                }
                numColumns = Math.max(1, Math.min(maxRowWidth, maxColumns));
            }
            fieldToNumColumns.put(entry.getKey(), numColumns);
        }

        // TODO: Infer the column count from recent file metadata instead of current-file samples.
        return new MapSharedShreddingWritePlan(logicalRowType, fieldToNumColumns);
    }
}
