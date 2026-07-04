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

package org.apache.paimon.flink.lineage;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.jdbc.JdbcCatalogFactory;
import org.apache.paimon.jdbc.JdbcCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Lineage utilities for building {@link SourceLineageVertex} and {@link LineageVertex} from a
 * Paimon table name and catalog options.
 */
public class LineageUtils {

    /** Default namespace when the catalog warehouse path is not available. */
    private static final String DEFAULT_NAMESPACE = "paimon";

    private static final String CATALOG_PREFIX = "catalog.";

    /** Catalog option keys safe to include in lineage facets (no credentials or secrets). */
    private static final Set<String> CATALOG_OPTION_ALLOWLIST =
            new HashSet<>(
                    Arrays.asList(CatalogOptions.WAREHOUSE.key(), CatalogOptions.METASTORE.key()));

    private static final String DEFAULT_CATALOG_IDENTIFIER = FlinkCatalogFactory.IDENTIFIER;

    private static final Set<String> PAIMON_OPTION_KEYS =
            Stream.of(CoreOptions.getOptions(), IcebergOptions.getOptions())
                    .flatMap(List::stream)
                    .map(opt -> opt.key())
                    .collect(Collectors.toSet());

    /** Extracts the {@link CatalogContext} from a table, or null if not available. */
    @Nullable
    private static CatalogContext catalogContext(Table table) {
        if (table instanceof FileStoreTable) {
            return ((FileStoreTable) table).catalogEnvironment().catalogContext();
        }
        if (table instanceof FormatTable) {
            return ((FormatTable) table).catalogContext();
        }
        return null;
    }

    /**
     * Builds the config map for a dataset facet from a {@link Table}. Includes {@link CoreOptions}
     * and {@link IcebergOptions}, partition keys, primary keys, and a safe subset of catalog-level
     * options prefixed with {@code "catalog."}.
     */
    private static Map<String, String> buildConfigMap(
            Table table, @Nullable CatalogContext catalogContext) {
        Map<String, String> config = new HashMap<>();
        config.put("partition-keys", String.join(",", table.partitionKeys()));
        config.put("primary-keys", String.join(",", table.primaryKeys()));

        table.options().entrySet().stream()
                .filter(e -> PAIMON_OPTION_KEYS.contains(e.getKey()))
                .forEach(e -> config.put(e.getKey(), e.getValue()));

        config.put("type", "paimon");

        if (catalogContext != null) {
            catalogContext
                    .options()
                    .toMap()
                    .forEach(
                            (k, v) -> {
                                if (CATALOG_OPTION_ALLOWLIST.contains(k)) {
                                    config.put(CATALOG_PREFIX + k, v);
                                }
                            });
        }

        return config;
    }

    /**
     * Returns the catalog warehouse path as the lineage namespace. Falls back to the table path
     * when no warehouse is configured, or {@code "paimon"} as a last resort.
     */
    public static String getNamespace(Table table, @Nullable CatalogContext catalogContext) {
        if (catalogContext != null) {
            String warehouse = catalogContext.options().get(CatalogOptions.WAREHOUSE);
            if (warehouse != null) {
                return warehouse;
            }
        }
        Path path = CoreOptions.path(table.options());
        if (path != null) {
            return path.toString();
        }
        return DEFAULT_NAMESPACE;
    }

    @VisibleForTesting
    static String resolveNameByMetastore(Table table, @Nullable String defaultName) {
        if (defaultName != null) {
            return defaultName;
        }

        CatalogContext ctx = catalogContext(table);
        if (ctx != null) {
            Options catalogOptions = ctx.options();
            // If jdbc metastore is used, use catalog-key as the catalog identifier.
            if (JdbcCatalogFactory.IDENTIFIER.equals(
                    catalogOptions.get(CatalogOptions.METASTORE))) {
                String catalogKeyValue = catalogOptions.get(JdbcCatalogOptions.CATALOG_KEY);
                if (!StringUtils.isNullOrWhitespaceOnly(catalogKeyValue)) {
                    return catalogKeyValue + "." + table.fullName();
                }
            }
        }
        return DEFAULT_CATALOG_IDENTIFIER + "." + table.fullName();
    }

    /**
     * Creates a {@link SourceLineageVertex} for a Paimon source table.
     *
     * @param name fully qualified table name, e.g. {@code "paimon.mydb.mytable"}
     * @param isBounded whether the source is bounded (batch) or unbounded (streaming)
     * @param table the Paimon table
     */
    public static SourceLineageVertex sourceLineageVertex(
            String name, boolean isBounded, Table table) {
        CatalogContext ctx = catalogContext(table);
        LineageDataset dataset =
                new PaimonLineageDataset(
                        resolveNameByMetastore(table, name),
                        getNamespace(table, ctx),
                        buildConfigMap(table, ctx),
                        table.rowType());
        Boundedness boundedness =
                isBounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
        return new PaimonSourceLineageVertex(boundedness, Collections.singletonList(dataset));
    }

    /**
     * Creates a {@link SourceLineageVertex} for a Paimon DataStream source table. The table name is
     * derived from the table's full name, prefixed with the {@code catalog-key} if available.
     *
     * @param isBounded whether the source is bounded (batch) or unbounded (streaming)
     * @param table the Paimon table
     */
    public static SourceLineageVertex sourceLineageVertex(boolean isBounded, Table table) {
        return sourceLineageVertex(null, isBounded, table);
    }

    /**
     * Creates a {@link LineageVertex} for a Paimon sink table.
     *
     * @param name fully qualified table name, e.g. {@code "paimon.mydb.mytable"}
     * @param table the Paimon table
     */
    public static LineageVertex sinkLineageVertex(String name, Table table) {
        CatalogContext ctx = catalogContext(table);
        LineageDataset dataset =
                new PaimonLineageDataset(
                        resolveNameByMetastore(table, name),
                        getNamespace(table, ctx),
                        buildConfigMap(table, ctx),
                        table.rowType());
        return new PaimonSinkLineageVertex(Collections.singletonList(dataset));
    }

    /**
     * Creates a {@link LineageVertex} for a Paimon DataStream sink table. The table name is derived
     * from the table's full name, prefixed with the {@code catalog-key} if available.
     *
     * @param table the Paimon table
     */
    public static LineageVertex sinkLineageVertex(Table table) {
        return sinkLineageVertex(null, table);
    }
}
