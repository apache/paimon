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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A physical write plan for the shared-shredding MAP layout. */
public class MapSharedShreddingWritePlan implements ShreddingWritePlan {

    private final RowType logicalRowType;
    private final MapSharedShreddingRowConverter converter;
    @Nullable private final MapSharedShreddingContext context;

    @Nullable private Map<String, Map<String, String>> fieldMetadata;

    public MapSharedShreddingWritePlan(
            RowType logicalRowType,
            Map<String, Integer> fieldToNumColumns,
            @Nullable MapSharedShreddingContext context) {
        this.logicalRowType = logicalRowType;
        this.converter = new MapSharedShreddingRowConverter(logicalRowType, fieldToNumColumns);
        this.context = context;
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public RowType physicalRowType() {
        return converter.physicalType();
    }

    @Override
    public InternalRow toPhysicalRow(InternalRow row) {
        return converter.convert(row);
    }

    @Override
    public Map<String, Map<String, String>> fieldMetadata(String compression) {
        if (fieldMetadata == null) {
            fieldMetadata = buildFieldMetadata(compression == null ? "none" : compression);
        }
        return fieldMetadata;
    }

    private Map<String, Map<String, String>> buildFieldMetadata(String compression) {
        Map<String, Map<String, String>> metadata = new LinkedHashMap<>();
        for (String fieldName : converter.shreddingFieldNames()) {
            MapSharedShreddingFieldMeta fieldMeta = converter.buildFieldMeta(fieldName);
            Map<String, String> fieldMetadata = new LinkedHashMap<>();
            MapSharedShreddingUtils.serializeMetadata(fieldMeta, compression, fieldMetadata);
            metadata.put(fieldName, Collections.unmodifiableMap(fieldMetadata));

            if (context != null) {
                context.reportFileStats(fieldName, fieldMeta.maxRowWidth());
            }
        }
        return Collections.unmodifiableMap(metadata);
    }
}
