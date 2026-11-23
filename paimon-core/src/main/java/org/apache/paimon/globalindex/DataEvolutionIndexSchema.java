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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.index.DataEvolutionIndexSourceMeta.UNKNOWN_SCHEMA_ID;

/** Checks scalar index key compatibility with the table schema used for a Data Evolution scan. */
public final class DataEvolutionIndexSchema {

    private final FileStoreTable table;
    private final RowType readType;
    private final Map<Long, Optional<RowType>> schemas = new HashMap<>();
    private final Map<Long, Map<Integer, Boolean>> fieldCompatibility = new HashMap<>();

    public DataEvolutionIndexSchema(FileStoreTable table) {
        this.table = table;
        this.readType = table.rowType();
    }

    public boolean isCompatible(IndexFileMeta file) {
        GlobalIndexMeta meta = file.globalIndexMeta();
        if (meta == null) {
            return false;
        }
        List<Integer> fieldIds = meta.getIndexedFieldIds();
        long sourceSchemaId = UNKNOWN_SCHEMA_ID;
        byte[] sourceMeta = meta.sourceMeta();
        if (table.coreOptions().dataEvolutionEnabled()
                && !"full-text".equals(file.indexType())
                && !CoreOptions.isVectorIndexType(file.indexType())
                && DataEvolutionIndexSourceMeta.isDataEvolutionMeta(sourceMeta)) {
            try {
                sourceSchemaId =
                        DataEvolutionIndexSourceMeta.deserialize(sourceMeta).sourceSchemaId();
            } catch (IllegalArgumentException e) {
                // An unrecognized source cannot establish compatible index keys.
                return false;
            }
        }
        if (sourceSchemaId == UNKNOWN_SCHEMA_ID) {
            return fieldIds.stream().allMatch(readType::containsField);
        }
        Optional<RowType> sourceType = schema(sourceSchemaId);
        if (!sourceType.isPresent()) {
            return false;
        }
        Map<Integer, Boolean> fields =
                fieldCompatibility.computeIfAbsent(sourceSchemaId, id -> new HashMap<>());
        return fieldIds.stream()
                .allMatch(
                        id ->
                                fields.computeIfAbsent(
                                        id, fieldId -> sameType(sourceType.get(), fieldId)));
    }

    private boolean sameType(RowType source, int fieldId) {
        return source.containsField(fieldId)
                && readType.containsField(fieldId)
                && source.getField(fieldId)
                        .type()
                        .equalsIgnoreNullable(readType.getField(fieldId).type());
    }

    private Optional<RowType> schema(long schemaId) {
        return schemas.computeIfAbsent(
                schemaId,
                id -> {
                    if (id == table.schema().id()) {
                        return Optional.of(readType);
                    }
                    try {
                        return Optional.of(table.schemaManager().tryGetSchema(id).logicalRowType());
                    } catch (FileNotFoundException e) {
                        return Optional.empty();
                    }
                });
    }
}
