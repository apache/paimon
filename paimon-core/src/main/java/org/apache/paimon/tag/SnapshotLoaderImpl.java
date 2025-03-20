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

package org.apache.paimon.tag;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.SnapshotLoader;

import java.io.IOException;
import java.util.Optional;

/** Implementation of {@link SnapshotLoader}. */
public class SnapshotLoaderImpl implements SnapshotLoader {

    private final CatalogLoader catalogLoader;
    private final Identifier identifier;

    public SnapshotLoaderImpl(CatalogLoader catalogLoader, Identifier identifier) {
        this.catalogLoader = catalogLoader;
        this.identifier = identifier;
    }

    @Override
    public Optional<Snapshot> load() throws IOException {
        try (Catalog catalog = catalogLoader.load()) {
            return catalog.loadSnapshot(identifier).map(TableSnapshot::snapshot);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean rollback(long snapshotId) throws IOException {
        try (Catalog catalog = catalogLoader.load()) {
            return catalog.rollbackTableBySnapshotId(identifier, snapshotId);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean rollback(String tagName) throws IOException {
        try (Catalog catalog = catalogLoader.load()) {
            return catalog.rollbackTableByTagName(identifier, tagName);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean needCleanWhenRollback() {
        try (Catalog catalog = catalogLoader.load()) {
            return catalog.needCleanAfterRollback();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SnapshotLoader copyWithBranch(String branch) {
        return new SnapshotLoaderImpl(
                catalogLoader,
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch));
    }
}
