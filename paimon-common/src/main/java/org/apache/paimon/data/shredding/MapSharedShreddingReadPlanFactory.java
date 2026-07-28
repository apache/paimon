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

import org.apache.paimon.format.shredding.ShreddingReadPlanFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/** Creates per-file shared-shredding MAP read plans. */
public class MapSharedShreddingReadPlanFactory implements ShreddingReadPlanFactory {

    private final RowType logicalRowType;

    public MapSharedShreddingReadPlanFactory(RowType logicalRowType) {
        this.logicalRowType = logicalRowType;
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public boolean shouldCreateReadPlan(
            Map<String, Map<String, String>> fieldMetadata, @Nullable Object fileSchema) {
        for (DataField field : logicalRowType.getFields()) {
            Map<String, String> metadata = fieldMetadata.get(field.name());
            if (MapSharedShreddingUtils.hasShreddingMetadata(metadata)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ShreddingReadPlan createReadPlan(
            Map<String, Map<String, String>> fieldMetadata, @Nullable Object fileSchema) {
        return new MapSharedShreddingReadPlan(
                logicalRowType, readSharedShreddingMetas(logicalRowType, fieldMetadata));
    }

    private static Map<String, MapSharedShreddingFieldMeta> readSharedShreddingMetas(
            RowType logicalRowType, Map<String, Map<String, String>> fieldMetadata) {
        Map<String, MapSharedShreddingFieldMeta> metas = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : fieldMetadata.entrySet()) {
            if (logicalRowType != null && !logicalRowType.containsField(entry.getKey())) {
                continue;
            }
            if (MapSharedShreddingUtils.hasShreddingMetadata(entry.getValue())) {
                metas.put(
                        entry.getKey(),
                        MapSharedShreddingUtils.deserializeMetadata(entry.getValue()));
            }
        }
        return metas;
    }
}
