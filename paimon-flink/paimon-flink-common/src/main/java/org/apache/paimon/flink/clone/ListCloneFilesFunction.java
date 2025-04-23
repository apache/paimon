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

package org.apache.paimon.flink.clone;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.hive.migrate.HiveCloneUtils;
import org.apache.paimon.hive.migrate.HivePartitionFiles;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** List files for table. */
public class ListCloneFilesFunction
        extends CloneProcessFunction<Tuple2<Identifier, Identifier>, CloneFileInfo> {

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
            Tuple2<Identifier, Identifier> tuple,
            ProcessFunction<Tuple2<Identifier, Identifier>, CloneFileInfo>.Context context,
            Collector<CloneFileInfo> collector)
            throws Exception {
        String sourceType = sourceCatalogConfig.get(CatalogOptions.METASTORE.key());
        checkNotNull(sourceType);

        Schema schema = HiveCloneUtils.hiveTableToPaimonSchema(hiveCatalog, tuple.f0);
        Map<String, String> options = schema.options();
        // only support Hive to unaware-bucket table now
        options.put(CoreOptions.BUCKET.key(), "-1");
        schema =
                new Schema(
                        schema.fields(),
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment());
        try {
            Table existedTable = targetCatalog.getTable(tuple.f1);

            checkState(
                    existedTable instanceof FileStoreTable,
                    String.format(
                            "existed paimon table '%s' is not a FileStoreTable, but a %s",
                            tuple.f1, existedTable.getClass().getName()));
            checkCompatible(schema, (FileStoreTable) existedTable);

            LOG.info("paimon table '{}' already exists, use it as target table.", tuple.f1);
        } catch (Catalog.TableNotExistException e) {
            LOG.info("create target paimon table '{}'.", tuple.f1);

            targetCatalog.createTable(tuple.f1, schema, false);
        }

        FileStoreTable table = (FileStoreTable) targetCatalog.getTable(tuple.f1);
        PartitionPredicate predicate =
                getPartitionPredicate(whereSql, table.schema().logicalPartitionType(), tuple.f0);

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
    }

    private void checkCompatible(Schema sourceSchema, FileStoreTable existedTable) {
        Schema existedSchema = existedTable.schema().toSchema();

        // check primary keys
        checkState(
                existedSchema.primaryKeys().isEmpty(),
                "Can not clone data to existed paimon table which has primary keys. Existed paimon table is "
                        + existedTable.name());

        // check bucket
        checkState(
                existedTable.coreOptions().bucket() == -1,
                "Can not clone data to existed paimon table which bucket is not -1. Existed paimon table is "
                        + existedTable.name());

        // check partition keys
        List<String> sourcePartitionFields = sourceSchema.partitionKeys();
        List<String> existedPartitionFields = existedSchema.partitionKeys();

        checkState(
                sourcePartitionFields.size() == existedPartitionFields.size()
                        && new HashSet<>(existedPartitionFields).containsAll(sourcePartitionFields),
                "source table partition keys is not compatible with existed paimon table partition keys.");

        // check all fields
        List<DataField> sourceFields = sourceSchema.fields();
        List<DataField> existedFields = existedSchema.fields();

        checkState(
                existedFields.size() >= sourceFields.size()
                        && new HashSet<>(existedPartitionFields).containsAll(sourcePartitionFields),
                "source table partition keys is not compatible with existed paimon table partition keys.");
    }

    @VisibleForTesting
    @Nullable
    static PartitionPredicate getPartitionPredicate(
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
