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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Catalog environment in table which contains log factory, metastore client factory. */
public class CatalogEnvironment implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Identifier identifier;
    @Nullable private final String uuid;
    @Nullable private final CatalogLoader catalogLoader;
    @Nullable private final SnapshotCommit.Factory commitFactory;

    public CatalogEnvironment(
            @Nullable Identifier identifier,
            @Nullable String uuid,
            @Nullable CatalogLoader catalogLoader,
            @Nullable SnapshotCommit.Factory commitFactory) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.catalogLoader = catalogLoader;
        this.commitFactory = commitFactory;
    }

    public static CatalogEnvironment empty() {
        return new CatalogEnvironment(null, null, null, null);
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
    public SnapshotCommit snapshotCommit(SnapshotManager snapshotManager) {
        if (commitFactory == null) {
            return null;
        }
        return commitFactory.create(identifier, snapshotManager);
    }

    @Nullable
    public PartitionHandler partitionHandler() {
        if (catalogLoader == null) {
            return null;
        }
        return PartitionHandler.create(catalogLoader.load(), identifier);
    }

    @VisibleForTesting
    public SnapshotCommit.Factory commitFactory() {
        return commitFactory;
    }
}
