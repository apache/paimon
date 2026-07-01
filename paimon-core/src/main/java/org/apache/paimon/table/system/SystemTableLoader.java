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

import org.apache.paimon.PagedList;
import org.apache.paimon.TableType;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.table.system.AggregationFieldsTable.AGGREGATION_FIELDS;
import static org.apache.paimon.table.system.AllPartitionsTable.ALL_PARTITIONS;
import static org.apache.paimon.table.system.AllTableOptionsTable.ALL_TABLE_OPTIONS;
import static org.apache.paimon.table.system.AllTablesTable.ALL_TABLES;
import static org.apache.paimon.table.system.AuditLogTable.AUDIT_LOG;
import static org.apache.paimon.table.system.BinlogTable.BINLOG;
import static org.apache.paimon.table.system.BranchesTable.BRANCHES;
import static org.apache.paimon.table.system.BucketsTable.BUCKETS;
import static org.apache.paimon.table.system.CatalogOptionsTable.CATALOG_OPTIONS;
import static org.apache.paimon.table.system.ConsumersTable.CONSUMERS;
import static org.apache.paimon.table.system.FileKeyRangesTable.FILE_KEY_RANGES;
import static org.apache.paimon.table.system.FilesTable.FILES;
import static org.apache.paimon.table.system.ManifestsTable.MANIFESTS;
import static org.apache.paimon.table.system.OptionsTable.OPTIONS;
import static org.apache.paimon.table.system.PartitionsTable.PARTITIONS;
import static org.apache.paimon.table.system.ReadOptimizedTable.READ_OPTIMIZED;
import static org.apache.paimon.table.system.RowTrackingTable.ROW_TRACKING;
import static org.apache.paimon.table.system.SchemasTable.SCHEMAS;
import static org.apache.paimon.table.system.SnapshotsTable.SNAPSHOTS;
import static org.apache.paimon.table.system.StatisticTable.STATISTICS;
import static org.apache.paimon.table.system.TableIndexesTable.TABLE_INDEXES;
import static org.apache.paimon.table.system.TagsTable.TAGS;

/** Loader to load system {@link Table}s. */
public class SystemTableLoader {

    public static final Map<String, Function<FileStoreTable, Table>> SYSTEM_TABLE_LOADERS =
            new ImmutableMap.Builder<String, Function<FileStoreTable, Table>>()
                    .put(MANIFESTS, ManifestsTable::new)
                    .put(SNAPSHOTS, SnapshotsTable::new)
                    .put(OPTIONS, OptionsTable::new)
                    .put(SCHEMAS, SchemasTable::new)
                    .put(PARTITIONS, PartitionsTable::new)
                    .put(BUCKETS, BucketsTable::new)
                    .put(AUDIT_LOG, AuditLogTable::new)
                    .put(FILES, FilesTable::new)
                    .put(FILE_KEY_RANGES, FileKeyRangesTable::new)
                    .put(TAGS, TagsTable::new)
                    .put(BRANCHES, BranchesTable::new)
                    .put(CONSUMERS, ConsumersTable::new)
                    .put(READ_OPTIMIZED, ReadOptimizedTable::new)
                    .put(AGGREGATION_FIELDS, AggregationFieldsTable::new)
                    .put(STATISTICS, StatisticTable::new)
                    .put(BINLOG, BinlogTable::new)
                    .put(TABLE_INDEXES, TableIndexesTable::new)
                    .put(ROW_TRACKING, RowTrackingTable::new)
                    .build();

    public static final List<String> SYSTEM_TABLES = new ArrayList<>(SYSTEM_TABLE_LOADERS.keySet());

    public static final List<String> GLOBAL_SYSTEM_TABLES =
            Arrays.asList(ALL_TABLES, ALL_PARTITIONS, ALL_TABLE_OPTIONS, CATALOG_OPTIONS);

    @Nullable
    public static Table load(String type, FileStoreTable dataTable) {
        return Optional.ofNullable(SYSTEM_TABLE_LOADERS.get(type.toLowerCase()))
                .map(f -> f.apply(dataTable))
                .orElse(null);
    }

    public static List<String> loadGlobalTableNames(Options catalogOptions) {
        List<String> tableNames = new ArrayList<>(GLOBAL_SYSTEM_TABLES);
        if (!catalogOptions.get(CatalogOptions.CATALOG_OPTIONS_TABLE_ENABLED)) {
            tableNames.remove(CATALOG_OPTIONS);
        }
        return tableNames;
    }

    public static PagedList<String> loadGlobalTableNamesPaged(
            Options catalogOptions,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) {
        validatePrefixSqlPattern(tableNamePattern);
        List<String> tableNames =
                loadGlobalTableNames(catalogOptions).stream()
                        .filter(tableName -> matchesNamePattern(tableName, tableNamePattern))
                        .filter(tableName -> matchesTableType(tableType))
                        .sorted()
                        .collect(Collectors.toList());

        Integer pageSize = maxResults != null && maxResults > 0 ? maxResults : null;
        List<String> pagedTableNames = new ArrayList<>();
        for (String tableName : tableNames) {
            if (pageToken != null && tableName.compareTo(pageToken) <= 0) {
                continue;
            }
            if (pageSize != null && pagedTableNames.size() >= pageSize) {
                break;
            }
            pagedTableNames.add(tableName);
        }

        String nextPageToken =
                pageSize != null && pagedTableNames.size() == pageSize
                        ? pagedTableNames.get(pagedTableNames.size() - 1)
                        : null;
        return new PagedList<>(pagedTableNames, nextPageToken);
    }

    public static List<String> loadGlobalTableNames() {
        return loadGlobalTableNames(new Options());
    }

    private static boolean matchesNamePattern(String name, @Nullable String namePattern) {
        if (StringUtils.isEmpty(namePattern)) {
            return true;
        }
        return Pattern.compile(sqlPatternToRegex(namePattern)).matcher(name).matches();
    }

    private static boolean matchesTableType(@Nullable String tableType) {
        return StringUtils.isEmpty(tableType) || TableType.TABLE.toString().equals(tableType);
    }

    private static void validatePrefixSqlPattern(@Nullable String pattern) {
        if (StringUtils.isEmpty(pattern)) {
            return;
        }

        boolean escaped = false;
        boolean inWildcardZone = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '%') {
                inWildcardZone = true;
            } else if (inWildcardZone) {
                throw new IllegalArgumentException(
                        "Can only support prefix sql like pattern query now.");
            }
        }
    }

    private static String sqlPatternToRegex(String pattern) {
        StringBuilder regex = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escaped) {
                regex.append(c);
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '%') {
                regex.append(".*");
            } else {
                regex.append(c);
            }
        }
        return "^" + regex + "$";
    }
}
