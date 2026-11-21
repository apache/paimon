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

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory.IDENTIFIER;

/** Checks that a Data Evolution BTree can be decoded using the read schema. */
final class BTreeIndexSchema {

    private static final String SCHEMA_TAG = "btree-global-index-schema-";
    private static final Pattern BUILD_SCHEMA =
            Pattern.compile(
                    SCHEMA_TAG
                            + "([0-9]+)-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\\.index");

    private final FileStoreTable table;
    private final RowType readSchema;
    private final Map<Long, Optional<RowType>> schemas = new HashMap<>();
    private final Map<Integer, Boolean> legacyFields = new HashMap<>();

    BTreeIndexSchema(FileStoreTable table) {
        this.table = table;
        this.readSchema = table.rowType();
    }

    boolean isCompatible(IndexFileMeta file) {
        if (!IDENTIFIER.equals(file.indexType())) {
            return true;
        }
        String name = file.fileName();
        if (!name.startsWith(SCHEMA_TAG)) {
            // Legacy keys are safe if the field's type has stayed the same since it was added.
            return file.globalIndexMeta().getIndexedFieldIds().stream()
                    .allMatch(field -> legacyFields.computeIfAbsent(field, this::legacyCompatible));
        }
        Matcher matcher = BUILD_SCHEMA.matcher(name);
        if (!matcher.matches()) {
            return false;
        }
        long schemaId;
        try {
            schemaId = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            return false;
        }
        Optional<RowType> written = schema(schemaId);
        return written.isPresent()
                && file.globalIndexMeta().getIndexedFieldIds().stream()
                        .allMatch(field -> sameType(written.get(), field));
    }

    private boolean legacyCompatible(int field) {
        if (!readSchema.containsField(field)) {
            return false;
        }
        for (long id = 0; id < table.schema().id(); id++) {
            Optional<RowType> old = schema(id);
            if (!old.isPresent()
                    || (old.get().containsField(field) && !sameType(old.get(), field))) {
                return false;
            }
        }
        return true;
    }

    private boolean sameType(RowType written, int field) {
        return written.containsField(field)
                && readSchema.containsField(field)
                && written.getField(field)
                        .type()
                        .equalsIgnoreNullable(readSchema.getField(field).type());
    }

    private Optional<RowType> schema(long id) {
        return schemas.computeIfAbsent(
                id,
                key -> {
                    TableSchema current = table.schema();
                    if (key == current.id()) {
                        return Optional.of(readSchema);
                    }
                    try {
                        return Optional.of(
                                table.schemaManager().tryGetSchema(key).logicalRowType());
                    } catch (FileNotFoundException e) {
                        return Optional.empty();
                    }
                });
    }
}
