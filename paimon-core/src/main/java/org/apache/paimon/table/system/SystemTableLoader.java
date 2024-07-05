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

package org.apache.paimon.table.system;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.apache.paimon.table.system.AggregationFieldsTable.AGGREGATION;
import static org.apache.paimon.table.system.AllTableOptionsTable.ALL_TABLE_OPTIONS;
import static org.apache.paimon.table.system.AuditLogTable.AUDIT_LOG;
import static org.apache.paimon.table.system.BranchesTable.BRANCHES;
import static org.apache.paimon.table.system.CatalogOptionsTable.CATALOG_OPTIONS;
import static org.apache.paimon.table.system.ConsumersTable.CONSUMERS;
import static org.apache.paimon.table.system.FilesTable.FILES;
import static org.apache.paimon.table.system.ManifestsTable.MANIFESTS;
import static org.apache.paimon.table.system.OptionsTable.OPTIONS;
import static org.apache.paimon.table.system.PartitionsTable.PARTITIONS;
import static org.apache.paimon.table.system.ReadOptimizedTable.READ_OPTIMIZED;
import static org.apache.paimon.table.system.SchemasTable.SCHEMAS;
import static org.apache.paimon.table.system.SinkTableLineageTable.SINK_TABLE_LINEAGE;
import static org.apache.paimon.table.system.SnapshotsTable.SNAPSHOTS;
import static org.apache.paimon.table.system.SourceTableLineageTable.SOURCE_TABLE_LINEAGE;
import static org.apache.paimon.table.system.StatisticTable.STATISTICS;
import static org.apache.paimon.table.system.TagsTable.TAGS;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Loader to load system {@link Table}s. */
public class SystemTableLoader {

    @Nullable
    public static Table load(String type, FileIO fileIO, FileStoreTable dataTable) {
        Path location = dataTable.location();
        switch (type.toLowerCase()) {
            case MANIFESTS:
                return new ManifestsTable(dataTable);
            case SNAPSHOTS:
                return new SnapshotsTable(fileIO, location, dataTable);
            case OPTIONS:
                return new OptionsTable(fileIO, location);
            case SCHEMAS:
                return new SchemasTable(fileIO, location);
            case PARTITIONS:
                return new PartitionsTable(dataTable);
            case AUDIT_LOG:
                return new AuditLogTable(dataTable);
            case FILES:
                return new FilesTable(dataTable);
            case TAGS:
                return new TagsTable(fileIO, location);
            case BRANCHES:
                return new BranchesTable(fileIO, location);
            case CONSUMERS:
                return new ConsumersTable(fileIO, location);
            case READ_OPTIMIZED:
                return new ReadOptimizedTable(dataTable);
            case AGGREGATION:
                return new AggregationFieldsTable(fileIO, location);
            case STATISTICS:
                return new StatisticTable(fileIO, location, dataTable);
            default:
                return null;
        }
    }

    @Nullable
    public static Table loadGlobal(
            String tableName,
            FileIO fileIO,
            Supplier<Map<String, Map<String, Path>>> allTablePaths,
            Options catalogOptions,
            @Nullable LineageMetaFactory lineageMetaFactory) {
        switch (tableName.toLowerCase()) {
            case ALL_TABLE_OPTIONS:
                return new AllTableOptionsTable(fileIO, allTablePaths.get());
            case CATALOG_OPTIONS:
                return new CatalogOptionsTable(catalogOptions);
            case SOURCE_TABLE_LINEAGE:
                {
                    checkNotNull(
                            lineageMetaFactory,
                            String.format(
                                    "Lineage meta should be configured for catalog with %s",
                                    LINEAGE_META.key()));
                    return new SourceTableLineageTable(lineageMetaFactory, catalogOptions);
                }
            case SINK_TABLE_LINEAGE:
                {
                    checkNotNull(
                            lineageMetaFactory,
                            String.format(
                                    "Lineage meta should be configured for catalog with %s",
                                    LINEAGE_META.key()));
                    return new SinkTableLineageTable(lineageMetaFactory, catalogOptions);
                }
            default:
                return null;
        }
    }

    public static List<String> loadGlobalTableNames() {
        return Arrays.asList(
                ALL_TABLE_OPTIONS, CATALOG_OPTIONS, SOURCE_TABLE_LINEAGE, SINK_TABLE_LINEAGE);
    }
}
