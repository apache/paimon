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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.DataEvolutionUtils.fileFields;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Rewrites a newest-first prefix of column versions without replacing the older normal files. */
public class DataEvolutionIncrementalCompactTask extends DataEvolutionNormalCompactTask {

    public DataEvolutionIncrementalCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        super(partition, files);
        checkArgument(
                files.stream()
                        .allMatch(
                                file ->
                                        file.nonNullRowIdRange()
                                                .equals(files.get(0).nonNullRowIdRange())),
                "Incremental compaction requires identical normal-file row-ID ranges.");
    }

    @Override
    public TaskType type() {
        return TaskType.INCREMENTAL_NORMAL;
    }

    @Override
    protected RowType readWriteType(FileStoreTable table) {
        boolean nestedFieldEnabled = table.coreOptions().dataEvolutionNestedFieldEnabled();
        Set<Integer> fieldIds = new HashSet<>();
        for (DataFileMeta file : compactBefore) {
            fieldIds.addAll(
                    fieldIds(
                            fileFields(table.schemaManager().schema(file.schemaId()), file),
                            nestedFieldEnabled));
        }
        for (String partitionKey : table.partitionKeys()) {
            fieldIds.add(table.rowType().getField(partitionKey).id());
        }
        RowType normalType = super.readWriteType(table);
        return new RowType(
                normalType.isNullable(),
                projectFields(normalType.getFields(), fieldIds, nestedFieldEnabled));
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
