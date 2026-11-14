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
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.append.dataevolution.DataEvolutionCompactProjection.fieldIds;
import static org.apache.paimon.append.dataevolution.DataEvolutionCompactProjection.project;
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
        SchemaManager schemaManager = table.schemaManager();
        Map<Long, TableSchema> schemas = new HashMap<>();
        Set<Integer> fieldIds = new HashSet<>();
        for (DataFileMeta file : compactBefore) {
            fieldIds.addAll(
                    fieldIds(
                            fileFields(
                                    schemas.computeIfAbsent(file.schemaId(), schemaManager::schema),
                                    file),
                            nestedFieldEnabled));
        }
        for (String partitionKey : table.partitionKeys()) {
            fieldIds.add(table.rowType().getField(partitionKey).id());
        }
        RowType normalType = super.readWriteType(table);
        return project(normalType, fieldIds, nestedFieldEnabled);
    }
}
