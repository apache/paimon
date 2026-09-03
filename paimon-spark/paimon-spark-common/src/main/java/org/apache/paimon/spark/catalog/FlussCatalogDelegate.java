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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.stream.Collectors;

/** Loads LakeStream tables from the Fluss Spark catalog used by Spark reads and writes. */
public final class FlussCatalogDelegate {

    static final String FLUSS_OPTION_PREFIX = "fluss.";
    static final String FLUSS_BOOTSTRAP_SERVERS = "fluss.bootstrap.servers";

    private static final String FLUSS_CATALOG_CLASS = "org.apache.fluss.spark.SparkCatalog";

    private final String catalogName;
    private final Map<String, String> flussOptions;
    private final CatalogLoader catalogLoader;

    private volatile TableCatalog flussCatalog;

    public FlussCatalogDelegate(Map<String, String> catalogOptions, String catalogName) {
        this(catalogOptions, catalogName, FlussCatalogDelegate::loadFlussCatalog);
    }

    @VisibleForTesting
    public FlussCatalogDelegate(
            Map<String, String> catalogOptions, String catalogName, CatalogLoader catalogLoader) {
        this.catalogName = catalogName;
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(catalogOptions);
        this.flussOptions =
                options.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(FLUSS_OPTION_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        entry ->
                                                entry.getKey()
                                                        .substring(FLUSS_OPTION_PREFIX.length()),
                                        Map.Entry::getValue));
        this.catalogLoader = catalogLoader;
    }

    boolean isConfigured() {
        return flussOptions.containsKey("bootstrap.servers");
    }

    public Table loadTable(Identifier identifier) throws NoSuchTableException {
        return catalog().loadTable(identifier);
    }

    private TableCatalog catalog() {
        if (flussCatalog == null) {
            synchronized (this) {
                if (flussCatalog == null) {
                    if (!isConfigured()) {
                        throw new IllegalStateException(
                                String.format(
                                        "Paimon catalog '%s' contains a Fluss LakeStream table, "
                                                + "but Fluss access is not configured. Add '%s' "
                                                + "and any required 'fluss.client.security.*' "
                                                + "options to the Paimon catalog.",
                                        catalogName, FLUSS_BOOTSTRAP_SERVERS));
                    }
                    try {
                        TableCatalog catalog = catalogLoader.load(contextClassLoader());
                        catalog.initialize(catalogName, new CaseInsensitiveStringMap(flussOptions));
                        flussCatalog = catalog;
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                String.format(
                                        "Failed to create the Fluss delegate for Paimon catalog "
                                                + "'%s'. Make sure a Fluss Spark connector "
                                                + "matching the Spark version is on the classpath.",
                                        catalogName),
                                e);
                    }
                }
            }
        }
        return flussCatalog;
    }

    private static TableCatalog loadFlussCatalog(ClassLoader classLoader) throws Exception {
        Class<?> catalogClass = Class.forName(FLUSS_CATALOG_CLASS, true, classLoader);
        Object catalog = catalogClass.getDeclaredConstructor().newInstance();
        if (!(catalog instanceof TableCatalog)) {
            throw new IllegalStateException(
                    FLUSS_CATALOG_CLASS + " does not implement Spark TableCatalog.");
        }
        return (TableCatalog) catalog;
    }

    private static ClassLoader contextClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader == null ? FlussCatalogDelegate.class.getClassLoader() : classLoader;
    }

    /** Loads a Fluss {@link TableCatalog} with the supplied class loader. */
    @VisibleForTesting
    public interface CatalogLoader {
        TableCatalog load(ClassLoader classLoader) throws Exception;
    }
}
