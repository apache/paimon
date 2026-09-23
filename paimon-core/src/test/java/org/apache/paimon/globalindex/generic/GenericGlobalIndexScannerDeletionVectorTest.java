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

package org.apache.paimon.globalindex.generic;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests deletion-vector table support in {@link GenericGlobalIndexScanner}. */
class GenericGlobalIndexScannerDeletionVectorTest {

    private static FileStoreTable table(boolean deletionVectorsEnabled) {
        FileStoreTable table = mock(FileStoreTable.class);
        CoreOptions options = mock(CoreOptions.class);
        when(options.bucket()).thenReturn(-1);
        when(options.deletionVectorsEnabled()).thenReturn(deletionVectorsEnabled);
        when(table.coreOptions()).thenReturn(options);
        when(table.name()).thenReturn("T");
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.latestSnapshot()).thenReturn(null);
        when(table.snapshotManager()).thenReturn(snapshotManager);
        return table;
    }

    @Test
    void testGlobalIndexAllowedOnDeletionVectorTable() {
        assertThatCode(() -> new GenericGlobalIndexScanner(table(true)).scan())
                .doesNotThrowAnyException();
    }

    @Test
    void testGlobalIndexAllowedWithoutDeletionVectors() {
        assertThatCode(() -> new GenericGlobalIndexScanner(table(false)).scan())
                .doesNotThrowAnyException();
    }
}
