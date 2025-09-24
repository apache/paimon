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
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.tag.SnapshotLoaderImpl;
import org.apache.paimon.utils.SnapshotLoader;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Optional;

/** Catalog environment in table which contains log factory, metastore client factory. */
public class CatalogEnvironment implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Identifier identifier;
    @Nullable private final String uuid;
    @Nullable private final CatalogLoader catalogLoader;
    @Nullable private final CatalogLockFactory lockFactory;
    @Nullable private final CatalogLockContext lockContext;
    @Nullable private final CatalogContext catalogContext;
    private final boolean supportsVersionManagement;

    public CatalogEnvironment(
            @Nullable Identifier identifier,
            @Nullable String uuid,
            @Nullable CatalogLoader catalogLoader,
            @Nullable CatalogLockFactory lockFactory,
            @Nullable CatalogLockContext lockContext,
            @Nullable CatalogContext catalogContext,
            boolean supportsVersionManagement) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.catalogLoader = catalogLoader;
        this.lockFactory = lockFactory;
        this.lockContext = lockContext;
        this.catalogContext = catalogContext;
        this.supportsVersionManagement = supportsVersionManagement;
    }

    public static CatalogEnvironment empty() {
        return new CatalogEnvironment(null, null, null, null, null, null, false);
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
    public PartitionHandler partitionHandler() {
        if (catalogLoader == null) {
            return null;
        }
        Catalog catalog = catalogLoader.load();
        return PartitionHandler.create(catalog, identifier);
    }

    public boolean supportsVersionManagement() {
        return supportsVersionManagement;
    }

    @Nullable
    public SnapshotCommit snapshotCommit(SnapshotManager snapshotManager) {
        SnapshotCommit snapshotCommit;
        if (catalogLoader != null && supportsVersionManagement) {
            snapshotCommit = new CatalogSnapshotCommit(catalogLoader.load(), identifier, uuid);
        } else {
            Lock lock =
                    Optional.ofNullable(lockFactory)
                            .map(factory -> factory.createLock(lockContext))
                            .map(l -> Lock.fromCatalog(l, identifier))
                            .orElseGet(Lock::empty);
            snapshotCommit = new RenamingSnapshotCommit(snapshotManager, lock);
        }
        return snapshotCommit;
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

    public CatalogEnvironment copy(Identifier identifier) {
        return new CatalogEnvironment(
                identifier,
                uuid,
                catalogLoader,
                lockFactory,
                lockContext,
                catalogContext,
                supportsVersionManagement);
    }

    public TableQueryAuth tableQueryAuth(CoreOptions options) {
        if (!options.queryAuthEnabled() || catalogLoader == null) {
            return select -> Collections.emptyList();
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
