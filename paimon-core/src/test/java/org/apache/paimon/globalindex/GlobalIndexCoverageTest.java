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

package org.apache.paimon.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link GlobalIndexCoverage}. */
public class GlobalIndexCoverageTest {

    @Test
    public void testFullCoverageSkipsDataFileScan() {
        FileStoreTable table = mock(FileStoreTable.class);
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.nextRowId()).thenReturn(10L);

        IndexFileMeta indexFile = mock(IndexFileMeta.class);
        when(indexFile.globalIndexMeta()).thenReturn(new GlobalIndexMeta(0, 9, 1, null, null));

        GlobalIndexCoverage coverage =
                new GlobalIndexCoverage(
                        table, snapshot, null, Collections.singletonList(indexFile));

        assertThat(coverage.unindexedRangesForCorrectness(Collections.singleton(1))).isEmpty();
        verify(table, never()).newSnapshotReader();
    }
}
