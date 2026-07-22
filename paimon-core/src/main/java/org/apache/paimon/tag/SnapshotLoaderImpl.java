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
import org.apache.paimon.catalog.ReadAuthorizationContext;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.SnapshotLoader;

import java.io.IOException;
import java.util.Optional;

/** Implementation of {@link SnapshotLoader}. */
public class SnapshotLoaderImpl implements SnapshotLoader {

    private static final long serialVersionUID = 3096335741107585269L;

    private final CatalogLoader catalogLoader;
    private final Identifier identifier;
    private final ReadAuthorizationContext readContext;

    public SnapshotLoaderImpl(CatalogLoader catalogLoader, Identifier identifier) {
        this(catalogLoader, identifier, ReadAuthorizationContext.direct());
    }

    public SnapshotLoaderImpl(
            CatalogLoader catalogLoader,
            Identifier identifier,
            ReadAuthorizationContext readContext) {
        this.catalogLoader = catalogLoader;
        this.identifier = identifier;
        this.readContext = readContext;
    }

    @Override
    public Optional<Snapshot> load() throws IOException {
        try (Catalog catalog = catalogLoader.load()) {
            return catalog.loadSnapshot(identifier, readContext()).map(TableSnapshot::snapshot);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollback(Instant instant) throws IOException {
        try (Catalog catalog = catalogLoader.load()) {
            catalog.rollbackTo(identifier, instant);
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
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch),
                readContext());
    }

    private ReadAuthorizationContext readContext() {
        // The field is null when deserializing instances written before read contexts existed.
        return readContext == null ? ReadAuthorizationContext.direct() : readContext;
    }
}
