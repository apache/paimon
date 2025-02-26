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
import org.apache.paimon.partition.Partition;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;

/** A {@link SnapshotCommit} using {@link Catalog} to commit. */
public class CatalogSnapshotCommit implements SnapshotCommit {

    private final SupportsSnapshots supportsSnapshots;
    private final Identifier identifier;

    public CatalogSnapshotCommit(SupportsSnapshots supportsSnapshots, Identifier identifier) {
        this.supportsSnapshots = supportsSnapshots;
        this.identifier = identifier;
    }

    @Override
    public boolean commit(Snapshot snapshot, String branch, List<Partition> statistics)
            throws Exception {
        Identifier newIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch);
        return supportsSnapshots.commitSnapshot(newIdentifier, snapshot, statistics);
    }

    @Override
    public void close() throws Exception {
        supportsSnapshots.close();
    }

    /** Factory to create {@link CatalogSnapshotCommit}. */
    public static class Factory implements SnapshotCommit.Factory {

        private static final long serialVersionUID = 1L;

        private final CatalogLoader catalogLoader;

        public Factory(CatalogLoader catalogLoader) {
            this.catalogLoader = catalogLoader;
        }

        @Override
        public SnapshotCommit create(Identifier identifier, SnapshotManager snapshotManager) {
            return new CatalogSnapshotCommit((SupportsSnapshots) catalogLoader.load(), identifier);
        }
    }
}
