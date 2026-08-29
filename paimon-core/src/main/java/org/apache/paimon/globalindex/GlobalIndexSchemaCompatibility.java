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

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates global index manifest entries against the current table schema. */
public final class GlobalIndexSchemaCompatibility {

    public static List<IndexManifestEntry> filterCompatible(
            FileStoreTable table, Collection<IndexManifestEntry> entries) {
        RowType currentRowType = table.rowType();
        Map<Long, RowType> historicalRowTypes = new HashMap<>();
        historicalRowTypes.put(table.schema().id(), currentRowType);
        Set<Long> missingSchemaIds = new HashSet<>();
        List<IndexManifestEntry> compatible = new ArrayList<>();
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            Long schemaId = entry.schemaId();
            if (globalIndex == null || schemaId == null) {
                continue;
            }

            RowType historicalRowType = historicalRowTypes.get(schemaId);
            if (historicalRowType == null && !missingSchemaIds.contains(schemaId)) {
                try {
                    historicalRowType =
                            table.schemaManager().tryGetSchema(schemaId).logicalRowType();
                    historicalRowTypes.put(schemaId, historicalRowType);
                } catch (FileNotFoundException e) {
                    missingSchemaIds.add(schemaId);
                }
            }
            if (historicalRowType != null
                    && compatibleIndexedFields(globalIndex, historicalRowType, currentRowType)) {
                compatible.add(entry);
            }
        }
        return compatible;
    }

    private static boolean compatibleIndexedFields(
            GlobalIndexMeta globalIndex, RowType historicalRowType, RowType currentRowType) {
        for (int fieldId : globalIndex.getIndexedFieldIds()) {
            if (!historicalRowType.containsField(fieldId)
                    || !currentRowType.containsField(fieldId)) {
                return false;
            }
            if (!historicalRowType
                    .getField(fieldId)
                    .type()
                    .equalsIgnoreNullable(currentRowType.getField(fieldId).type())) {
                return false;
            }
        }
        return true;
    }

    private GlobalIndexSchemaCompatibility() {}
}
