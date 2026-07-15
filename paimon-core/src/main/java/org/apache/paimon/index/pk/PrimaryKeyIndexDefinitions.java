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

package org.apache.paimon.index.pk;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Resolves all configured source-backed primary-key indexes. */
public class PrimaryKeyIndexDefinitions {

    private final List<PrimaryKeyIndexDefinition> definitions;

    private PrimaryKeyIndexDefinitions(List<PrimaryKeyIndexDefinition> definitions) {
        this.definitions = Collections.unmodifiableList(definitions);
    }

    public static PrimaryKeyIndexDefinitions create(TableSchema schema) {
        CoreOptions options = new CoreOptions(schema.options());
        List<String> vectorColumns = options.primaryKeyVectorIndexColumns();
        List<String> btreeColumns = options.primaryKeyBTreeIndexColumns();
        List<String> bitmapColumns = options.primaryKeyBitmapIndexColumns();
        List<String> fullTextColumns = options.primaryKeyFullTextIndexColumns();
        validateNoDuplicates(vectorColumns, CoreOptions.PK_VECTOR_INDEX_COLUMNS.key());
        validateNoDuplicates(btreeColumns, CoreOptions.PK_BTREE_INDEX_COLUMNS.key());
        validateNoDuplicates(bitmapColumns, CoreOptions.PK_BITMAP_INDEX_COLUMNS.key());
        validateNoDuplicates(fullTextColumns, CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS.key());
        validateOneIndexPerColumn(vectorColumns, btreeColumns, bitmapColumns, fullTextColumns);
        List<PrimaryKeyIndexDefinition> definitions = new ArrayList<>();

        for (DataField field : schema.fields()) {
            String column = field.name();
            if (btreeColumns.contains(column)) {
                definitions.add(
                        new PrimaryKeyIndexDefinition(
                                column,
                                field.id(),
                                BTreeGlobalIndexerFactory.IDENTIFIER,
                                options.primaryKeyBTreeIndexOptions(column),
                                PrimaryKeyIndexDefinition.Family.BTREE,
                                options.primaryKeyIndexCompactionLevelFanout(column),
                                options.primaryKeyIndexCompactionStaleRatioThreshold(column)));
            } else if (bitmapColumns.contains(column)) {
                definitions.add(
                        new PrimaryKeyIndexDefinition(
                                column,
                                field.id(),
                                BitmapGlobalIndexerFactory.IDENTIFIER,
                                options.primaryKeyBitmapIndexOptions(column),
                                PrimaryKeyIndexDefinition.Family.BITMAP,
                                options.primaryKeyIndexCompactionLevelFanout(column),
                                options.primaryKeyIndexCompactionStaleRatioThreshold(column)));
            } else if (vectorColumns.contains(column)) {
                definitions.add(
                        new PrimaryKeyIndexDefinition(
                                column,
                                field.id(),
                                options.primaryKeyVectorIndexType(column),
                                options.primaryKeyVectorIndexOptions(column),
                                PrimaryKeyIndexDefinition.Family.VECTOR,
                                options.primaryKeyIndexCompactionLevelFanout(column),
                                options.primaryKeyIndexCompactionStaleRatioThreshold(column)));
            } else if (fullTextColumns.contains(column)) {
                definitions.add(
                        new PrimaryKeyIndexDefinition(
                                column,
                                field.id(),
                                "full-text",
                                options.primaryKeyFullTextIndexOptions(column),
                                PrimaryKeyIndexDefinition.Family.FULL_TEXT,
                                options.primaryKeyIndexCompactionLevelFanout(column),
                                options.primaryKeyIndexCompactionStaleRatioThreshold(column)));
            }
        }

        return new PrimaryKeyIndexDefinitions(definitions);
    }

    private static void validateNoDuplicates(List<String> columns, String optionKey) {
        Set<String> uniqueColumns = new HashSet<>();
        for (String column : columns) {
            checkArgument(
                    uniqueColumns.add(column),
                    "%s contains duplicate column '%s'.",
                    optionKey,
                    column);
        }
    }

    private static void validateOneIndexPerColumn(
            List<String> vectorColumns,
            List<String> btreeColumns,
            List<String> bitmapColumns,
            List<String> fullTextColumns) {
        Set<String> indexedColumns = new HashSet<>();
        validateUniqueColumns(indexedColumns, vectorColumns);
        validateUniqueColumns(indexedColumns, btreeColumns);
        validateUniqueColumns(indexedColumns, bitmapColumns);
        validateUniqueColumns(indexedColumns, fullTextColumns);
    }

    private static void validateUniqueColumns(Set<String> indexedColumns, List<String> columns) {
        for (String column : columns) {
            checkArgument(
                    indexedColumns.add(column),
                    "Column '%s' can own at most one primary-key index.",
                    column);
        }
    }

    public List<PrimaryKeyIndexDefinition> definitions() {
        return definitions;
    }
}
