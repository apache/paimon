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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.fileFields;

/** Resolves physical write fields and checks projections against the current table schema. */
final class DataEvolutionCompactProjection {

    private final RowType rowType;
    private final boolean nestedFieldEnabled;
    private final Set<Integer> currentFieldIds;
    private final LongFunction<TableSchema> schemaFetcher;

    DataEvolutionCompactProjection(FileStoreTable table) {
        this.rowType = table.rowType();
        this.nestedFieldEnabled = table.coreOptions().dataEvolutionNestedFieldEnabled();
        this.currentFieldIds =
                fieldIds(
                        rowType.getFields().stream()
                                .filter(field -> !table.partitionKeys().contains(field.name()))
                                .collect(Collectors.toList()),
                        nestedFieldEnabled);
        Map<Long, TableSchema> schemas = new HashMap<>();
        this.schemaFetcher = id -> schemas.computeIfAbsent(id, table.schemaManager()::schema);
    }

    Set<Integer> currentFields(DataFileMeta file) {
        Set<Integer> fields =
                fieldIds(
                        fileFields(schemaFetcher.apply(file.schemaId()), file), nestedFieldEnabled);
        fields.retainAll(currentFieldIds);
        return fields;
    }

    boolean canRewrite(List<DataFileMeta> files) {
        Set<Integer> writtenFields = new HashSet<>();
        for (DataFileMeta file : files) {
            writtenFields.addAll(currentFields(file));
            if (nestedFieldEnabled) {
                for (DataField field : fileFields(schemaFetcher.apply(file.schemaId()), file)) {
                    if (field.type() instanceof RowType
                            && rowType.containsField(field.id())
                            && !currentFieldIds.containsAll(
                                    fieldIds(Collections.singletonList(field), true))) {
                        // Dropped leaves can still supply parent nullness. Read the whole group
                        // to preserve those parents without manufacturing NULL siblings.
                        return false;
                    }
                }
            }
        }
        if (writtenFields.isEmpty()) {
            return false;
        }
        if (nestedFieldEnabled) {
            try {
                project(rowType, writtenFields, true).collectLeafPaths(rowType);
            } catch (UnsupportedOperationException e) {
                // Added leaves or ambiguous dotted paths can make old whole structs unencodable.
                return false;
            }
        }
        return true;
    }

    static RowType project(RowType rowType, Set<Integer> fieldIds, boolean nestedFieldEnabled) {
        return new RowType(
                rowType.isNullable(),
                projectFields(rowType.getFields(), fieldIds, nestedFieldEnabled));
    }

    /** ROW leaves are merge units in nested mode; ARRAY and MAP fields remain whole units. */
    static Set<Integer> fieldIds(List<DataField> fields, boolean nestedFieldEnabled) {
        Set<Integer> fieldIds = new HashSet<>();
        for (DataField field : fields) {
            if (nestedFieldEnabled && field.type() instanceof RowType) {
                fieldIds.addAll(fieldIds(((RowType) field.type()).getFields(), true));
            } else {
                fieldIds.add(field.id());
            }
        }
        return fieldIds;
    }

    private static List<DataField> projectFields(
            List<DataField> fields, Set<Integer> fieldIds, boolean nestedFieldEnabled) {
        List<DataField> projected = new ArrayList<>();
        for (DataField field : fields) {
            if (nestedFieldEnabled && field.type() instanceof RowType) {
                RowType type = (RowType) field.type();
                List<DataField> children = projectFields(type.getFields(), fieldIds, true);
                if (!children.isEmpty()) {
                    projected.add(field.newType(new RowType(type.isNullable(), children)));
                }
            } else if (fieldIds.contains(field.id())) {
                projected.add(field);
            }
        }
        return projected;
    }
}
