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

package org.apache.paimon.flink.clone.files;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.clone.schema.CloneSchemaInfo;
import org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.hive.clone.HiveCloneUtils;
import org.apache.paimon.hive.clone.HivePartitionFiles;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** List files for table. */
public class ListCloneFilesFunction
        extends CloneFilesProcessFunction<CloneSchemaInfo, CloneFileInfo> {

    private static final long serialVersionUID = 1L;

    @Nullable private final String whereSql;

    public ListCloneFilesFunction(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            @Nullable String whereSql) {
        super(sourceCatalogConfig, targetCatalogConfig);
        this.whereSql = whereSql;
    }

    @Override
    public void processElement(
            CloneSchemaInfo cloneSchemaInfo,
            ProcessFunction<CloneSchemaInfo, CloneFileInfo>.Context context,
            Collector<CloneFileInfo> collector)
            throws Exception {
        Tuple2<Identifier, Identifier> tuple = cloneSchemaInfo.identifierTuple();

        FileStoreTable table = (FileStoreTable) targetCatalog.getTable(tuple.f1);
        PartitionPredicate predicate =
                getPartitionPredicate(whereSql, table.schema().logicalPartitionType(), tuple.f0);

        try {
            List<HivePartitionFiles> allPartitions =
                    HiveCloneUtils.listFiles(
                            hiveCatalog,
                            tuple.f0,
                            table.schema().logicalPartitionType(),
                            table.coreOptions().partitionDefaultName(),
                            predicate);
            for (HivePartitionFiles partitionFiles : allPartitions) {
                CloneFileInfo.fromHive(tuple.f1, partitionFiles).forEach(collector::collect);
            }
        } catch (Exception e) {
            throw new Exception(
                    "Failed to list clone files for table " + tuple.f0.getFullName(), e);
        }
    }

    @Nullable
    public static PartitionPredicate getPartitionPredicate(
            @Nullable String whereSql, RowType partitionType, Identifier tableId) throws Exception {
        if (whereSql == null) {
            return null;
        }

        SimpleSqlPredicateConvertor simpleSqlPredicateConvertor =
                new SimpleSqlPredicateConvertor(partitionType);
        try {
            Predicate predicate = simpleSqlPredicateConvertor.convertSqlToPredicate(whereSql);
            return PartitionPredicate.fromPredicate(partitionType, predicate);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse partition filter sql '"
                            + whereSql
                            + "' for table "
                            + tableId.getFullName(),
                    e);
        }
    }
}
