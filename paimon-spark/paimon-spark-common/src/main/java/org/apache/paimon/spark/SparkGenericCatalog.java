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

package org.apache.paimon.spark;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;
import org.apache.paimon.spark.util.SQLConfUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.PaimonCatalogUtils;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.paimon.shims;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.paimon.options.CatalogOptions.ALLOW_UPPER_CASE;
import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.spark.SparkCatalogOptions.CREATE_UNDERLYING_SESSION_CATALOG;
import static org.apache.paimon.spark.SparkCatalogOptions.DEFAULT_DATABASE;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A Spark catalog that can also load non-Paimon tables.
 *
 * <p>Most of the content of this class is referenced from Iceberg's SparkSessionCatalog.
 */
public class SparkGenericCatalog extends SparkBaseCatalog implements CatalogExtension {

    private static final Logger LOG = LoggerFactory.getLogger(SparkGenericCatalog.class);

    private SparkCatalog sparkCatalog = null;

    private boolean underlyingSessionCatalogEnabled = false;

    private CatalogPlugin sessionCatalog = null;

    @Override
    public Catalog paimonCatalog() {
        return this.sparkCatalog.paimonCatalog();
    }

    @Override
    public String[] defaultNamespace() {
        return asNamespaceCatalog().defaultNamespace();
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return asNamespaceCatalog().listNamespaces();
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        return asNamespaceCatalog().listNamespaces(namespace);
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        return asNamespaceCatalog().namespaceExists(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        return asNamespaceCatalog().loadNamespaceMetadata(namespace);
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        asNamespaceCatalog().createNamespace(namespace, metadata);
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        asNamespaceCatalog().alterNamespace(namespace, changes);
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        if (namespace.length == 1 && namespaceExists(namespace) && cascade) {
            for (Identifier table : listTables(namespace)) {
                try {
                    dropTable(table);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to drop {}, fallback to use sessionCatalog to drop, for {}",
                            table,
                            e.getMessage());
                }
            }
        }
        return asNamespaceCatalog().dropNamespace(namespace, cascade);
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        // delegate to the session catalog because all tables share the same namespace
        return asTableCatalog().listTables(namespace);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        try {
            return sparkCatalog.loadTable(ident);
        } catch (NoSuchTableException e) {
            return throwsOldIfExceptionHappens(() -> asTableCatalog().loadTable(ident), e);
        }
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        try {
            return sparkCatalog.loadTable(ident, version);
        } catch (NoSuchTableException e) {
            return throwsOldIfExceptionHappens(() -> asTableCatalog().loadTable(ident, version), e);
        }
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        try {
            return sparkCatalog.loadTable(ident, timestamp);
        } catch (NoSuchTableException e) {
            return throwsOldIfExceptionHappens(
                    () -> asTableCatalog().loadTable(ident, timestamp), e);
        }
    }

    @Override
    public void invalidateTable(Identifier ident) {
        // We do not need to check whether the table exists and whether
        // it is an Paimon table to reduce remote service requests.
        sparkCatalog.invalidateTable(ident);
        asTableCatalog().invalidateTable(ident);
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        String provider = properties.get(TableCatalog.PROP_PROVIDER);
        if (usePaimon(provider)) {
            return sparkCatalog.createTable(ident, schema, partitions, properties);
        } else {
            // delegate to the session catalog
            return shims.createTable(asTableCatalog(), ident, schema, partitions, properties);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        if (sparkCatalog.tableExists(ident)) {
            return sparkCatalog.alterTable(ident, changes);
        } else {
            return asTableCatalog().alterTable(ident, changes);
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        return sparkCatalog.dropTable(ident) || asTableCatalog().dropTable(ident);
    }

    @Override
    public boolean purgeTable(Identifier ident) {
        return sparkCatalog.purgeTable(ident) || asTableCatalog().purgeTable(ident);
    }

    @Override
    public void renameTable(Identifier from, Identifier to)
            throws NoSuchTableException, TableAlreadyExistsException {
        if (sparkCatalog.tableExists(from)) {
            sparkCatalog.renameTable(from, to);
        } else {
            asTableCatalog().renameTable(from, to);
        }
    }

    @Override
    public final void initialize(String name, CaseInsensitiveStringMap options) {
        SparkSession sparkSession = SparkSession.active();
        SessionState sessionState = sparkSession.sessionState();
        Configuration hadoopConf = sessionState.newHadoopConf();
        SparkConf sparkConf = new SparkConf();
        if (options.containsKey(METASTORE.key())
                && options.get(METASTORE.key()).equalsIgnoreCase("hive")) {
            String uri = options.get(CatalogOptions.URI.key());
            if (uri != null) {
                String envHmsUri = hadoopConf.get("hive.metastore.uris", null);
                if (envHmsUri != null) {
                    Preconditions.checkArgument(
                            uri.equals(envHmsUri),
                            "Inconsistent Hive metastore URIs: %s (Spark session) != %s (spark_catalog)",
                            envHmsUri,
                            uri);
                }
            }
        }
        if ("in-memory"
                .equals(sparkSession.conf().get(StaticSQLConf.CATALOG_IMPLEMENTATION().key()))) {
            LOG.warn("InMemoryCatalog here may cause bad effect.");
        }

        this.catalogName = name;
        this.sparkCatalog = new SparkCatalog();

        CaseInsensitiveStringMap newOptions =
                autoFillConfigurations(options, sessionState.conf(), hadoopConf);
        sparkCatalog.initialize(name, newOptions);

        if (options.getBoolean(
                CREATE_UNDERLYING_SESSION_CATALOG.key(),
                CREATE_UNDERLYING_SESSION_CATALOG.defaultValue())) {
            this.underlyingSessionCatalogEnabled = true;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                sparkConf.set("spark.hadoop." + entry.getKey(), entry.getValue());
                hadoopConf.set(entry.getKey(), entry.getValue());
            }
            ExternalCatalog externalCatalog =
                    PaimonCatalogUtils.buildExternalCatalog(sparkConf, hadoopConf);
            this.sessionCatalog = new V2SessionCatalog(new SessionCatalog(externalCatalog));
        }
    }

    private CaseInsensitiveStringMap autoFillConfigurations(
            CaseInsensitiveStringMap options, SQLConf sqlConf, Configuration hadoopConf) {
        Map<String, String> newOptions = new HashMap<>(options.asCaseSensitiveMap());
        fillAliyunConfigurations(newOptions, hadoopConf);
        fillCommonConfigurations(newOptions, sqlConf);

        // if spark is case-insensitive, set allow upper case to catalog
        if (!sqlConf.caseSensitiveAnalysis()) {
            newOptions.put(ALLOW_UPPER_CASE.key(), "true");
        }

        return new CaseInsensitiveStringMap(newOptions);
    }

    private void fillAliyunConfigurations(Map<String, String> options, Configuration hadoopConf) {
        if (!options.containsKey(METASTORE.key())) {
            // In Alibaba Cloud EMR, `hive.metastore.type` has two types: DLF or LOCAL, for DLF, we
            // set `metastore` to dlf, for LOCAL, do nothing.
            String aliyunEMRHiveMetastoreType = hadoopConf.get("hive.metastore.type", null);
            if ("dlf".equalsIgnoreCase(aliyunEMRHiveMetastoreType)) {
                options.put(METASTORE.key(), "dlf");
            }
        }
    }

    private void fillCommonConfigurations(Map<String, String> options, SQLConf sqlConf) {
        if (!options.containsKey(WAREHOUSE.key())) {
            String warehouse = sqlConf.warehousePath();
            options.put(WAREHOUSE.key(), warehouse);
        }
        if (!options.containsKey(METASTORE.key())) {
            String metastore = sqlConf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION());
            if (HiveCatalogOptions.IDENTIFIER.equals(metastore)) {
                options.put(METASTORE.key(), metastore);
            }
        }
        options.put(CatalogOptions.FORMAT_TABLE_ENABLED.key(), "false");
        String sessionCatalogDefaultDatabase = SQLConfUtils.defaultDatabase(sqlConf);
        if (options.containsKey(DEFAULT_DATABASE.key())) {
            String userDefineDefaultDatabase = options.get(DEFAULT_DATABASE.key());
            if (!userDefineDefaultDatabase.equals(sessionCatalogDefaultDatabase)) {
                LOG.warn(
                        String.format(
                                "The current spark version does not support configuring default database, switch database to %s",
                                sessionCatalogDefaultDatabase));
                options.put(DEFAULT_DATABASE.key(), sessionCatalogDefaultDatabase);
            }
        } else {
            options.put(DEFAULT_DATABASE.key(), sessionCatalogDefaultDatabase);
        }
    }

    @Override
    public void setDelegateCatalog(CatalogPlugin delegate) {
        if (!underlyingSessionCatalogEnabled) {
            this.sessionCatalog = delegate;
        }
    }

    private CatalogPlugin getDelegateCatalog() {
        checkNotNull(
                sessionCatalog,
                "Delegated SessionCatalog is missing, '%s' can only be used with 'spark_catalog'.",
                SparkGenericCatalog.class.getName());
        return sessionCatalog;
    }

    private TableCatalog asTableCatalog() {
        return (TableCatalog) getDelegateCatalog();
    }

    private SupportsNamespaces asNamespaceCatalog() {
        return (SupportsNamespaces) getDelegateCatalog();
    }

    private FunctionCatalog asFunctionCatalog() {
        return (FunctionCatalog) getDelegateCatalog();
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        try {
            return sparkCatalog.listFunctions(namespace);
        } catch (NoSuchNamespaceException e) {
            return asFunctionCatalog().listFunctions(namespace);
        }
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        try {
            return sparkCatalog.loadFunction(ident);
        } catch (NoSuchFunctionException e) {
            return asFunctionCatalog().loadFunction(ident);
        }
    }

    private Table throwsOldIfExceptionHappens(Callable<Table> call, NoSuchTableException e)
            throws NoSuchTableException {
        try {
            return call.call();
        } catch (Exception exception) {
            throw e;
        }
    }
}
