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
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.apache.spark.sql.SparkSession;

import java.util.Objects;

/** Copy schema and get latest snapshot. */
public class CopySchemaOperator extends CopyFilesOperator {

    public static final String INDEX_MANIFEST_FILES_TAG = "index-manifest-files";
    public static final String DATA_MANIFEST_FILES_TAG = "data-manifest-files";

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
        targetCatalog.createTable(
                targetIdentifier, newSchemaFromTableSchema(sourceTable.schema()), false);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);

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
}
