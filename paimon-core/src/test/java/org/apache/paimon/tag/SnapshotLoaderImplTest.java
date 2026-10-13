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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.Instant;
import org.apache.paimon.utils.SnapshotLoader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests table incarnation propagation through catalog snapshot loaders. */
class SnapshotLoaderImplTest {
    @ParameterizedTest
    @CsvSource({"main,main-uuid", "dev,branch-uuid", "main,", "dev,"})
    void testRollbackPreservesTableUuid(String branch, String uuid) throws Exception {
        Catalog catalog = mock(Catalog.class);
        Identifier identifier = new Identifier("db", "table", branch);
        CatalogEnvironment environment = environment(catalog, identifier, uuid);
        Instant instant = Instant.tag("checkpoint");

        environment.snapshotLoader().rollback(instant);

        verify(catalog).rollbackTo(identifier, uuid, instant, null, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"main", "dev"})
    void testCopyWithinBranchKeepsTableUuid(String branch) throws Exception {
        Catalog catalog = mock(Catalog.class);
        // The main branch also has an implicit null spelling.
        Identifier identifier =
                "main".equals(branch)
                        ? Identifier.create("db", "table")
                        : new Identifier("db", "table", branch);
        SnapshotLoader loader = environment(catalog, identifier, "table-uuid").snapshotLoader();
        Instant instant = Instant.snapshot(1L);

        loader.copyWithBranch(branch).rollback(instant);

        verify(catalog)
                .rollbackTo(
                        new Identifier("db", "table", branch), "table-uuid", instant, null, null);
    }

    @ParameterizedTest
    @CsvSource({"main,dev", "dev,main"})
    void testCopyToAnotherBranchDoesNotReuseSourceUuid(String source, String target)
            throws Exception {
        Catalog catalog = mock(Catalog.class);
        SnapshotLoader loader =
                environment(catalog, new Identifier("db", "table", source), "source-uuid")
                        .snapshotLoader();
        Instant instant = Instant.snapshot(1L);

        loader.copyWithBranch(target).rollback(instant);

        verify(catalog)
                .rollbackTo(new Identifier("db", "table", target), null, instant, null, null);
    }

    private CatalogEnvironment environment(Catalog catalog, Identifier identifier, String uuid) {
        return new CatalogEnvironment(
                identifier, uuid, () -> catalog, null, null, null, true, false);
    }
}
