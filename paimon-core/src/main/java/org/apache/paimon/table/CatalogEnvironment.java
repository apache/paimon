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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.CatalogSnapshotCommit;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.catalog.TableRollback;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogLoader;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.tag.SnapshotLoaderImpl;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotLoader;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.function.LongConsumer;

import static org.apache.paimon.options.CatalogOptions.METASTORE;

/** Catalog environment in table which contains log factory, metastore client factory. */
public class CatalogEnvironment implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final String READ_VIA_OPTION = RESTApi.HEADER_PREFIX + RESTApi.READ_VIA_HEADER;

    @Nullable private final Identifier identifier;
    @Nullable private final String uuid;
    @Nullable private final CatalogLoader catalogLoader;
    @Nullable private final CatalogLockFactory lockFactory;
    @Nullable private final CatalogLockContext lockContext;
    @Nullable private final CatalogContext catalogContext;
    private final boolean supportsVersionManagement;
    private final boolean supportsPartitionModification;

    public CatalogEnvironment(
            @Nullable Identifier identifier,
            @Nullable String uuid,
            @Nullable CatalogLoader catalogLoader,
            @Nullable CatalogLockFactory lockFactory,
            @Nullable CatalogLockContext lockContext,
            @Nullable CatalogContext catalogContext,
            boolean supportsVersionManagement,
            boolean supportsPartitionModification) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.catalogLoader = catalogLoader;
        this.lockFactory = lockFactory;
        this.lockContext = lockContext;
        this.catalogContext = catalogContext;
        this.supportsVersionManagement = supportsVersionManagement;
        this.supportsPartitionModification = supportsPartitionModification;
    }

    public static CatalogEnvironment empty() {
        return new CatalogEnvironment(null, null, null, null, null, null, false, false);
    }

    @Nullable
    public Identifier identifier() {
        return identifier;
    }

    @Nullable
    public String uuid() {
        return uuid;
    }

    @Nullable
    public PartitionModification partitionModification() {
        if (catalogLoader == null) {
            return null;
        }
        if (!supportsPartitionModification) {
            return null;
        }
        Catalog catalog = catalogLoader.load();
        return PartitionModification.create(catalog, identifier);
    }

    @Nullable
    public PartitionMarkDone partitionMarkDone() {
        if (catalogLoader == null) {
            return null;
        }
        Catalog catalog = catalogLoader.load();
        return PartitionMarkDone.create(catalog, identifier);
    }

    public boolean supportsVersionManagement() {
        return supportsVersionManagement;
    }

    @Nullable
    public SchemaModification schemaModification() {
        if (catalogLoader == null) {
            return null;
        }
        Catalog catalog = catalogLoader.load();
        return SchemaModification.create(catalog, identifier);
    }

    @Nullable
    public SnapshotCommit snapshotCommit(SnapshotManager snapshotManager) {
        if (catalogLoader != null && supportsVersionManagement) {
            return snapshotCommit(snapshotManager, Lock.empty());
        }
        return snapshotCommit(
                snapshotManager,
                Lock.fromCatalog(
                        lockFactory == null ? null : lockFactory.createLock(lockContext),
                        identifier));
    }

    /** Use a supplied publication lock when the writer manages the complete operation scope. */
    public SnapshotCommit snapshotCommit(SnapshotManager snapshotManager, Lock publicationLock) {
        if (catalogLoader != null && supportsVersionManagement) {
            return new CatalogSnapshotCommit(catalogLoader.load(), identifier, uuid);
        }
        return new RenamingSnapshotCommit(snapshotManager, publicationLock);
    }

    /** Create a writer's lock independently of the snapshot publication mechanism. */
    public Lock createLock(CoreOptions options, String commitUser) {
        Identifier lockIdentifier =
                identifier == null
                        ? null
                        : new Identifier(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                options.branch());
        if (catalogLoader != null) {
            Catalog catalog = catalogLoader.load();
            Lock delegate;
            try {
                Lock catalogLock =
                        catalog.createLock(
                                lockIdentifier, uuid, commitUser, options.toConfiguration());
                // Keep serialized environments from third-party catalogs compatible with the
                // legacy factory SPI until their catalogs implement createLock.
                delegate =
                        catalogLock instanceof Lock.EmptyLock && lockFactory != null
                                ? Lock.fromCatalog(
                                        lockFactory.createLock(lockContext),
                                        lockIdentifier,
                                        uuid,
                                        commitUser)
                                : catalogLock;
            } catch (RuntimeException e) {
                IOUtils.closeQuietly(catalog);
                throw e;
            }
            return new Lock() {
                @Override
                public <T> T runWithLock(Callable<T> callable) throws Exception {
                    return delegate.runWithLock(callable);
                }

                @Override
                public void ensureValid() {
                    delegate.ensureValid();
                }

                @Override
                public void close() throws Exception {
                    try {
                        delegate.close();
                    } finally {
                        catalog.close();
                    }
                }
            };
        }
        return Lock.fromCatalog(
                lockFactory == null ? null : lockFactory.createLock(lockContext),
                lockIdentifier,
                uuid,
                commitUser);
    }

    @Nullable
    public TableRollback catalogTableRollback() {
        if (catalogLoader != null && supportsVersionManagement) {
            Catalog catalog = catalogLoader.load();
            return (instant, fromSnapshot) -> {
                try {
                    catalog.rollbackTo(identifier, instant, fromSnapshot);
                } catch (Catalog.TableNotExistException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        return null;
    }

    @Nullable
    public LongConsumer catalogSchemaRollback() {
        if (catalogLoader != null && supportsVersionManagement) {
            Catalog catalog = catalogLoader.load();
            return schemaId -> {
                try {
                    catalog.rollbackSchema(identifier, schemaId);
                } catch (Catalog.TableNotExistException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        return null;
    }

    @Nullable
    public SnapshotLoader snapshotLoader() {
        if (catalogLoader == null) {
            return null;
        }
        return new SnapshotLoaderImpl(catalogLoader, identifier);
    }

    @Nullable
    public CatalogLockFactory lockFactory() {
        return lockFactory;
    }

    @Nullable
    public CatalogLockContext lockContext() {
        return lockContext;
    }

    @Nullable
    public CatalogLoader catalogLoader() {
        return catalogLoader;
    }

    @Nullable
    public CatalogContext catalogContext() {
        return catalogContext;
    }

    /**
     * Returns a context for loading tables referenced while reading this table.
     *
     * <p>For REST catalogs, the outermost table identifier is attached as an optional request
     * header. A context which already carries the header is returned unchanged so nested
     * dependencies preserve the original table.
     */
    @Nullable
    CatalogContext dependencyReadContext() {
        if (identifier == null || catalogContext == null) {
            return catalogContext;
        }

        boolean restCatalog =
                catalogLoader instanceof RESTCatalogLoader
                        || RESTCatalogFactory.IDENTIFIER.equals(
                                catalogContext.options().get(METASTORE));
        if (!restCatalog) {
            return catalogContext;
        }

        Options options = catalogContext.options();
        if (options.containsKey(READ_VIA_OPTION)) {
            return catalogContext;
        }

        Options dependencyOptions = new Options(options.toMap());
        if (!dependencyOptions.contains(METASTORE)) {
            dependencyOptions.set(METASTORE, RESTCatalogFactory.IDENTIFIER);
        }
        dependencyOptions.set(
                READ_VIA_OPTION, RESTUtil.encodeString(JsonSerdeUtil.toFlatJson(identifier)));
        return CatalogContext.create(
                dependencyOptions,
                catalogContext.hadoopConf(),
                catalogContext.preferIO(),
                catalogContext.fallbackIO());
    }

    public CatalogEnvironment copy(Identifier identifier) {
        return new CatalogEnvironment(
                identifier,
                uuid,
                catalogLoader,
                lockFactory,
                lockContext,
                catalogContext,
                supportsVersionManagement,
                supportsPartitionModification);
    }

    public TableQueryAuth tableQueryAuth(CoreOptions options) {
        if (!options.queryAuthEnabled() || catalogLoader == null) {
            return select -> null;
        }
        return select -> {
            try (Catalog catalog = catalogLoader.load()) {
                return catalog.authTableQuery(identifier, select);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
