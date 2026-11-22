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

import org.apache.paimon.Snapshot;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.index.DataEvolutionIndexSourceMeta.UNKNOWN_SCHEMA_ID;

/** Checks index encoding compatibility with the table schema used for a Data Evolution scan. */
public final class DataEvolutionIndexSchema {

    private final FileStoreTable table;
    private final RowType readType;
    @Nullable private final Snapshot snapshot;
    private final Map<Long, Optional<RowType>> schemas = new HashMap<>();
    private final Map<Integer, Boolean> legacyFields = new HashMap<>();

    public DataEvolutionIndexSchema(FileStoreTable table, @Nullable Snapshot snapshot) {
        this.table = table;
        this.readType = table.rowType();
        this.snapshot = snapshot;
    }

    public boolean isCompatible(IndexFileMeta file) {
        GlobalIndexMeta meta = file.globalIndexMeta();
        return meta != null && isCompatible(meta, meta.getIndexedFieldIds());
    }

    public boolean isPrimaryFieldCompatible(IndexFileMeta file) {
        GlobalIndexMeta meta = file.globalIndexMeta();
        return meta != null && isCompatible(meta, Collections.singletonList(meta.indexFieldId()));
    }

    private boolean isCompatible(GlobalIndexMeta meta, List<Integer> fieldIds) {
        if (!table.coreOptions().dataEvolutionEnabled()) {
            return fieldIds.stream().allMatch(readType::containsField);
        }
        long sourceSchemaId = UNKNOWN_SCHEMA_ID;
        byte[] sourceMeta = meta.sourceMeta();
        if (DataEvolutionIndexSourceMeta.isDataEvolutionMeta(sourceMeta)) {
            try {
                sourceSchemaId =
                        DataEvolutionIndexSourceMeta.deserialize(sourceMeta).sourceSchemaId();
            } catch (IllegalArgumentException e) {
                // An unrecognized source cannot establish compatible index keys.
                return false;
            }
        }
        if (sourceSchemaId != UNKNOWN_SCHEMA_ID) {
            Optional<RowType> sourceType = schema(sourceSchemaId);
            return sourceType.isPresent()
                    && fieldIds.stream().allMatch(id -> sameType(sourceType.get(), id));
        }
        return fieldIds.stream()
                .allMatch(id -> legacyFields.computeIfAbsent(id, this::legacyCompatible));
    }

    private boolean legacyCompatible(int fieldId) {
        if (!readType.containsField(fieldId)) {
            return false;
        }
        long readSchemaId = table.schema().id();
        long lastSchemaId =
                snapshot == null ? readSchemaId : Math.max(readSchemaId, snapshot.schemaId());
        // A legacy index can use any schema that contained the field. A schema-only ALTER
        // may have advanced the build schema beyond the source snapshot's schema.
        for (long id = 0; id <= lastSchemaId; id++) {
            if (id == readSchemaId) {
                continue;
            }
            Optional<RowType> historical = schema(id);
            if (!historical.isPresent()
                    || (historical.get().containsField(fieldId)
                            && !sameType(historical.get(), fieldId))) {
                return false;
            }
        }
        return true;
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
