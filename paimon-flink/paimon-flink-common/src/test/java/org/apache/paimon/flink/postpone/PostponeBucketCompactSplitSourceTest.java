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

package org.apache.paimon.flink.postpone;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PostponeBucketCompactSplitSource}. */
class PostponeBucketCompactSplitSourceTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testReadsPinnedSnapshot() throws Exception {
        SnapshotReader reader = mock(SnapshotReader.class, RETURNS_SELF);
        SnapshotReader.Plan plan = mock(SnapshotReader.Plan.class, CALLS_REAL_METHODS);
        when(plan.splits()).thenReturn(Collections.emptyList());
        when(reader.read()).thenReturn(plan);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.newSnapshotReader()).thenReturn(reader);

        PostponeBucketCompactSplitSource source =
                new PostponeBucketCompactSplitSource(table, Collections.emptyMap(), 5L);
        SourceReader sourceReader = source.createReader(mock(SourceReaderContext.class));
        sourceReader.pollNext(mock(ReaderOutput.class));

        verify(reader).withSnapshot(5L);
    }
}
