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

package org.apache.paimon.spark.copy;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Copy schema and get latest snapshot. */
public class CopySchemaOperator extends CopyFilesOperator {

    private static final Logger LOG = LoggerFactory.getLogger(CopySchemaOperator.class);

    public CopySchemaOperator(SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
    }

    public Snapshot execute(Identifier sourceIdentifier, Identifier targetIdentifier)
            throws Exception {
        Table originalSourceTable = sourceCatalog.getTable(sourceIdentifier);
        Preconditions.checkState(
                originalSourceTable instanceof FileStoreTable,
                String.format(
                        "Only support copy FileStoreTable, but this table %s is %s.",
                        sourceIdentifier, sourceIdentifier.getClass()));
        FileStoreTable sourceTable = (FileStoreTable) originalSourceTable;

        // 1. create target table
        targetCatalog.createDatabase(targetIdentifier.getDatabaseName(), true);

        try {
            Table existedTable = targetCatalog.getTable(targetIdentifier);
            Preconditions.checkState(
                    existedTable instanceof FileStoreTable,
                    String.format(
                            "existed paimon table '%s' is not a FileStoreTable, but a %s",
                            targetIdentifier, existedTable.getClass().getName()));
            checkCompatible(sourceTable, (FileStoreTable) existedTable);

            LOG.info("paimon table '{}' already exists, use it as target table.", targetIdentifier);
        } catch (Catalog.TableNotExistException e) {
            LOG.info("create target paimon table '{}'.", targetIdentifier);

            targetCatalog.createTable(
                    targetIdentifier, newSchemaFromTableSchema(sourceTable.schema()), false);
        }

        // 2. get latest snapshot files
        FileStore<?> sourceStore = sourceTable.store();
        SnapshotManager sourceSnapshotManager = sourceStore.snapshotManager();
        Snapshot latestSnapshot = sourceSnapshotManager.latestSnapshot();
        return latestSnapshot;
    }

    private static Schema newSchemaFromTableSchema(TableSchema tableSchema) {
        return new Schema(
                ImmutableList.copyOf(tableSchema.fields()),
                ImmutableList.copyOf(tableSchema.partitionKeys()),
                ImmutableList.copyOf(tableSchema.primaryKeys()),
                ImmutableMap.copyOf(
                        Iterables.filter(
                                tableSchema.options().entrySet(),
                                entry -> !Objects.equals(entry.getKey(), CoreOptions.PATH.key()))),
                tableSchema.comment());
    }

    private void checkCompatible(FileStoreTable sourceTable, FileStoreTable existedTable) {
        Schema sourceSchema = sourceTable.schema().toSchema();
        Schema existedSchema = existedTable.schema().toSchema();

        // check bucket
        checkState(
                sourceTable.coreOptions().bucket() == existedTable.coreOptions().bucket(),
                "source table bucket is not compatible with existed paimon table bucket.");

        // check format
        checkState(
                Objects.equals(
                        sourceTable.coreOptions().formatType(),
                        existedTable.coreOptions().formatType()),
                "source table format is not compatible with existed paimon table format.");

        // check primary keys
        List<String> sourcePrimaryKeys = sourceSchema.primaryKeys();
        List<String> existedPrimaryKeys = existedSchema.primaryKeys();
        checkState(
                sourcePrimaryKeys.size() == existedPrimaryKeys.size()
                        && new HashSet<>(existedPrimaryKeys).containsAll(sourcePrimaryKeys),
                "source table primary keys is not compatible with existed paimon table primary keys.");

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
                        && new HashSet<>(existedFields).containsAll(sourceFields),
                "source table fields is not compatible with existed paimon table fields.");
    }
}
