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
import org.apache.paimon.utils.SnapshotManagerTest;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link CatalogSnapshotCommit}. */
public class CatalogSnapshotCommitTest {

    @Test
    public void testCommitForwardsBaseSnapshotUuid() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Identifier identifier = Identifier.create("database", "table");
        Identifier branchIdentifier = new Identifier("database", "table", "branch");
        Snapshot snapshot = SnapshotManagerTest.createSnapshotWithMillis(2L, 1000L);
        when(catalog.commitSnapshot(
                        branchIdentifier,
                        "table-uuid",
                        "base-snapshot-uuid",
                        snapshot,
                        Collections.emptyList()))
                .thenReturn(true);

        CatalogSnapshotCommit commit = new CatalogSnapshotCommit(catalog, identifier, "table-uuid");

        assertThat(commit.commit("base-snapshot-uuid", snapshot, "branch", Collections.emptyList()))
                .isTrue();
        verify(catalog)
                .commitSnapshot(
                        branchIdentifier,
                        "table-uuid",
                        "base-snapshot-uuid",
                        snapshot,
                        Collections.emptyList());
    }
}
