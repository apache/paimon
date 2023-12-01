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

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.paimon.spark;

import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.spark.analysis.NoSuchProcedureException;
import org.apache.paimon.spark.catalog.ProcedureCatalog;
import org.apache.paimon.spark.procedure.Procedure;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A Spark catalog that can also load non-Paimon tables.
 *
 * <p>Most of the content of this class is referenced from Iceberg's SparkSessionCatalog.
 *
 * @param <T> CatalogPlugin class to avoid casting to TableCatalog and SupportsNamespaces.
 */
public class SparkGenericCatalog<T extends TableCatalog & SupportsNamespaces>
        implements ProcedureCatalog, CatalogExtension {

    private static final Logger LOG = LoggerFactory.getLogger(SparkGenericCatalog.class);

    private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};

    private String catalogName = null;
    private SparkCatalog paimonCatalog = null;
    private T sessionCatalog = null;

    @Override
    public String[] defaultNamespace() {
        return DEFAULT_NAMESPACE;
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return getSessionCatalog().listNamespaces();
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        return getSessionCatalog().listNamespaces(namespace);
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        return getSessionCatalog().namespaceExists(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        return getSessionCatalog().loadNamespaceMetadata(namespace);
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        getSessionCatalog().createNamespace(namespace, metadata);
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        getSessionCatalog().alterNamespace(namespace, changes);
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        return getSessionCatalog().dropNamespace(namespace, cascade);
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        // delegate to the session catalog because all tables share the same namespace
        return getSessionCatalog().listTables(namespace);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        try {
            return paimonCatalog.loadTable(ident);
        } catch (NoSuchTableException e) {
            return throwsOldIfExceptionHappens(() -> getSessionCatalog().loadTable(ident), e);
        }
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        try {
            return paimonCatalog.loadTable(ident, version);
        } catch (NoSuchTableException e) {
            return throwsOldIfExceptionHappens(
                    () -> getSessionCatalog().loadTable(ident, version), e);
        }
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        try {
            return paimonCatalog.loadTable(ident, timestamp);
        } catch (NoSuchTableException e) {
            return throwsOldIfExceptionHappens(
                    () -> getSessionCatalog().loadTable(ident, timestamp), e);
        }
    }

    @Override
    public void invalidateTable(Identifier ident) {
        // We do not need to check whether the table exists and whether
        // it is an Paimon table to reduce remote service requests.
        paimonCatalog.invalidateTable(ident);
        getSessionCatalog().invalidateTable(ident);
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        String provider = properties.get("provider");
        if (usePaimon(provider)) {
            return paimonCatalog.createTable(ident, schema, partitions, properties);
        } else {
            // delegate to the session catalog
            return getSessionCatalog().createTable(ident, schema, partitions, properties);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        if (paimonCatalog.tableExists(ident)) {
            return paimonCatalog.alterTable(ident, changes);
        } else {
            return getSessionCatalog().alterTable(ident, changes);
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        return paimonCatalog.dropTable(ident) || getSessionCatalog().dropTable(ident);
    }

    @Override
    public boolean purgeTable(Identifier ident) {
        return paimonCatalog.purgeTable(ident) || getSessionCatalog().purgeTable(ident);
    }

    @Override
    public void renameTable(Identifier from, Identifier to)
            throws NoSuchTableException, TableAlreadyExistsException {
        if (paimonCatalog.tableExists(from)) {
            paimonCatalog.renameTable(from, to);
        } else {
            getSessionCatalog().renameTable(from, to);
        }
    }

    @Override
    public final void initialize(String name, CaseInsensitiveStringMap options) {
        if (options.containsKey(METASTORE.key())
                && options.get(METASTORE.key()).equalsIgnoreCase("hive")) {
            String uri = options.get(CatalogOptions.URI.key());
            if (uri != null) {
                Configuration conf = SparkSession.active().sessionState().newHadoopConf();
                String envHmsUri = conf.get("hive.metastore.uris", null);
                if (envHmsUri != null) {
                    Preconditions.checkArgument(
                            uri.equals(envHmsUri),
                            "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
                            envHmsUri,
                            uri);
                }
            }
        }
        if (SparkSession.active().sharedState().externalCatalog().unwrapped()
                instanceof InMemoryCatalog) {
            LOG.warn("InMemoryCatalog here may cause bad effect.");
        }

        this.catalogName = name;
        this.paimonCatalog = new SparkCatalog();

        this.paimonCatalog.initialize(
                name, autoFillConfigurations(options, SparkSession.active().sessionState().conf()));
    }

    private CaseInsensitiveStringMap autoFillConfigurations(
            CaseInsensitiveStringMap options, SQLConf conf) {
        Map<String, String> newOptions = new HashMap<>(options.asCaseSensitiveMap());
        if (!options.containsKey(WAREHOUSE.key())) {
            String warehouse = conf.warehousePath();
            newOptions.put(WAREHOUSE.key(), warehouse);
        }
        String metastore = conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION());
        if (HiveCatalogOptions.IDENTIFIER.equals(metastore)) {
            newOptions.put(METASTORE.key(), metastore);
            String uri;
            if ((uri = conf.getConfString("spark.sql.catalog.spark_catalog.uri", null)) != null
                    && !options.containsKey(URI.key())) {
                newOptions.put(URI.key(), uri);
            }
        }

        return new CaseInsensitiveStringMap(newOptions);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setDelegateCatalog(CatalogPlugin sparkSessionCatalog) {
        if (sparkSessionCatalog instanceof TableCatalog
                && sparkSessionCatalog instanceof SupportsNamespaces) {
            this.sessionCatalog = (T) sparkSessionCatalog;
        } else {
            throw new IllegalArgumentException("Invalid session catalog: " + sparkSessionCatalog);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    private boolean usePaimon(String provider) {
        return provider == null || "paimon".equalsIgnoreCase(provider);
    }

    private T getSessionCatalog() {
        checkNotNull(
                sessionCatalog,
                "Delegated SessionCatalog is missing. "
                        + "Please make sure your are replacing Spark's default catalog, named 'spark_catalog'.");
        return sessionCatalog;
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0 || isSystemNamespace(namespace) || namespaceExists(namespace)) {
            return new Identifier[0];
        }

        throw new NoSuchNamespaceException(namespace);
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        throw new NoSuchFunctionException(ident);
    }

    private static boolean isSystemNamespace(String[] namespace) {
        return namespace.length == 1 && namespace[0].equalsIgnoreCase("system");
    }

    private Table throwsOldIfExceptionHappens(Callable<Table> call, NoSuchTableException e)
            throws NoSuchTableException {
        try {
            return call.call();
        } catch (Exception exception) {
            throw e;
        }
    }

    @Override
    public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
        return paimonCatalog.loadProcedure(identifier);
    }
}
