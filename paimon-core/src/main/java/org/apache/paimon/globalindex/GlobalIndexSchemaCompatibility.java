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
import org.apache.paimon.index.IndexFileMeta;
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

/** Validates global indexes against the current table schema. */
public final class GlobalIndexSchemaCompatibility {

    public static List<IndexFileMeta> filterCompatible(
            FileStoreTable table, Collection<IndexFileMeta> indexFiles) {
        RowType currentRowType = table.rowType();
        Map<Long, RowType> buildRowTypes = new HashMap<>();
        buildRowTypes.put(table.schema().id(), currentRowType);
        Set<Long> missingSchemaIds = new HashSet<>();
        List<IndexFileMeta> compatible = new ArrayList<>();
        for (IndexFileMeta indexFile : indexFiles) {
            GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
            if (globalIndex == null || globalIndex.buildSchemaId() == null) {
                continue;
            }

            long buildSchemaId = globalIndex.buildSchemaId();
            RowType buildRowType = buildRowTypes.get(buildSchemaId);
            if (buildRowType == null && !missingSchemaIds.contains(buildSchemaId)) {
                try {
                    buildRowType =
                            table.schemaManager().tryGetSchema(buildSchemaId).logicalRowType();
                    buildRowTypes.put(buildSchemaId, buildRowType);
                } catch (FileNotFoundException e) {
                    missingSchemaIds.add(buildSchemaId);
                }
            }
            if (buildRowType != null
                    && compatibleIndexedFields(globalIndex, buildRowType, currentRowType)) {
                compatible.add(indexFile);
            }
        }
        return compatible;
    }

    private static boolean compatibleIndexedFields(
            GlobalIndexMeta globalIndex, RowType buildRowType, RowType currentRowType) {
        for (int fieldId : globalIndex.getIndexedFieldIds()) {
            if (!buildRowType.containsField(fieldId) || !currentRowType.containsField(fieldId)) {
                return false;
            }
            if (!buildRowType
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
