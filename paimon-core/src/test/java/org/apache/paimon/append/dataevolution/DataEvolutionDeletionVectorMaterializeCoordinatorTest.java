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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.BinaryIndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionDeletionVectorMaterializeCoordinator}. */
public class DataEvolutionDeletionVectorMaterializeCoordinatorTest {

    @Test
    public void testNoDeletionVectorSkipsDataManifestScan() {
        FileStoreTable table = mock(FileStoreTable.class);
        FileStore store = mock(FileStore.class);
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        Snapshot snapshot = mock(Snapshot.class);
        Options options = new Options();
        options.set(DATA_EVOLUTION_ENABLED, true);
        options.set(DELETION_VECTORS_ENABLED, true);

        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        when(table.store()).thenReturn(store);
        when(store.newIndexFileHandler()).thenReturn(indexFileHandler);
        when(indexFileHandler.scan(snapshot, BinaryIndexManifestEntry.FULL_PROJECTION))
                .thenReturn(CloseableIterator.empty());

        DataEvolutionDeletionVectorMaterializeCoordinator coordinator =
                new DataEvolutionDeletionVectorMaterializeCoordinator(table, null, snapshot);

        assertThatThrownBy(coordinator::plan).isInstanceOf(EndOfScanException.class);
        verify(table, never()).newSnapshotReader();
        verify(store, never()).newScan();
    }
}
