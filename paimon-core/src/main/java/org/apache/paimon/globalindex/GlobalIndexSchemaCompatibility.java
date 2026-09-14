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
        return partitionByCompatibility(table, entries).compatible();
    }

    public static CompatibilityResult partitionByCompatibility(
            FileStoreTable table, Collection<IndexManifestEntry> entries) {
        CompatibilityChecker checker = new CompatibilityChecker(table);
        List<IndexManifestEntry> compatible = new ArrayList<>();
        List<IndexManifestEntry> incompatible = new ArrayList<>();
        for (IndexManifestEntry entry : entries) {
            if (checker.isCompatible(entry.indexFile(), entry.schemaId())) {
                compatible.add(entry);
            } else {
                incompatible.add(entry);
            }
        }
        return new CompatibilityResult(compatible, incompatible);
    }

    public static List<IndexFileMeta> filterCompatibleFiles(
            FileStoreTable table, Collection<IndexFileMeta> indexFiles) {
        CompatibilityChecker checker = new CompatibilityChecker(table);
        List<IndexFileMeta> compatible = new ArrayList<>();
        for (IndexFileMeta indexFile : indexFiles) {
            if (checker.isCompatible(indexFile, indexFile.schemaId())) {
                compatible.add(indexFile);
            }
        }
        return compatible;
    }

    /** Global index manifest entries grouped by compatibility with the current table schema. */
    public static final class CompatibilityResult {

        private final List<IndexManifestEntry> compatible;
        private final List<IndexManifestEntry> incompatible;

        private CompatibilityResult(
                List<IndexManifestEntry> compatible, List<IndexManifestEntry> incompatible) {
            this.compatible = compatible;
            this.incompatible = incompatible;
        }

        public List<IndexManifestEntry> compatible() {
            return compatible;
        }

        public List<IndexManifestEntry> incompatible() {
            return incompatible;
        }
    }

    private static class CompatibilityChecker {

        private final FileStoreTable table;
        private final RowType currentRowType;
        private final Map<Long, RowType> historicalRowTypes = new HashMap<>();
        private final Set<Long> missingSchemaIds = new HashSet<>();

        private CompatibilityChecker(FileStoreTable table) {
            this.table = table;
            this.currentRowType = table.rowType();
            historicalRowTypes.put(table.schema().id(), currentRowType);
        }

        private boolean isCompatible(IndexFileMeta indexFile, Long schemaId) {
            GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
            if (globalIndex == null || schemaId == null) {
                return false;
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
            return historicalRowType != null
                    && compatibleIndexedFields(globalIndex, historicalRowType, currentRowType);
        }
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
