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

import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.CatalogSnapshotCommit;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.tag.SnapshotLoaderImpl;
import org.apache.paimon.utils.SnapshotLoader;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Catalog environment in table which contains log factory, metastore client factory. */
public class CatalogEnvironment implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Identifier identifier;
    @Nullable private final String uuid;
    @Nullable private final CatalogLoader catalogLoader;
    @Nullable private final CatalogLockFactory lockFactory;
    @Nullable private final CatalogLockContext lockContext;
    private final boolean supportsSnapshots;
    private final boolean supportsBranches;

    public CatalogEnvironment(
            @Nullable Identifier identifier,
            @Nullable String uuid,
            @Nullable CatalogLoader catalogLoader,
            @Nullable CatalogLockFactory lockFactory,
            @Nullable CatalogLockContext lockContext,
            boolean supportsSnapshots,
            boolean supportsBranches) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.catalogLoader = catalogLoader;
        this.lockFactory = lockFactory;
        this.lockContext = lockContext;
        this.supportsSnapshots = supportsSnapshots;
        this.supportsBranches = supportsBranches;
    }

    public static CatalogEnvironment empty() {
        return new CatalogEnvironment(null, null, null, null, null, false, false);
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
        return PartitionHandler.create(catalogLoader.load(), identifier);
    }

    @Nullable
    public SnapshotCommit snapshotCommit(SnapshotManager snapshotManager) {
        SnapshotCommit.Factory factory;
        if (catalogLoader == null || !supportsSnapshots) {
            factory = new RenamingSnapshotCommit.Factory(lockFactory, lockContext);
        } else {
            factory = new CatalogSnapshotCommit.Factory(catalogLoader);
        }
        return factory.create(identifier, snapshotManager);
    }

    @Nullable
    public SnapshotLoader snapshotLoader() {
        if (catalogLoader == null || !supportsSnapshots) {
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

    public boolean supportsBranches() {
        return supportsBranches;
    }
}
