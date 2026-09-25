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

package org.apache.paimon.catalog;

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;

import javax.annotation.Nullable;

import java.util.List;

/** A {@link SnapshotCommit} using {@link Catalog} to commit. */
public class CatalogSnapshotCommit implements SnapshotCommit {

    private final Catalog catalog;
    private final Identifier identifier;
    @Nullable private final String uuid;
    @Nullable private final String storageBranch;

    public CatalogSnapshotCommit(Catalog catalog, Identifier identifier, @Nullable String uuid) {
        this(catalog, identifier, uuid, null);
    }

    public CatalogSnapshotCommit(
            Catalog catalog,
            Identifier identifier,
            @Nullable String uuid,
            @Nullable String storageBranch) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.uuid = uuid;
        this.storageBranch = storageBranch;
    }

    @Override
    public boolean commit(
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics)
            throws Exception {
        // REST resolves the original logical identifier to its physical storage branch. Keep
        // that identifier even when main is backed by a different branch after publication.
        // An explicit switch away from the loaded storage branch still selects a table branch.
        Identifier newIdentifier =
                storageBranch != null && storageBranch.equals(branch)
                        ? identifier
                        : new Identifier(
                                identifier.getDatabaseName(), identifier.getTableName(), branch);
        return catalog.commitSnapshot(newIdentifier, uuid, baseSnapshotUuid, snapshot, statistics);
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
