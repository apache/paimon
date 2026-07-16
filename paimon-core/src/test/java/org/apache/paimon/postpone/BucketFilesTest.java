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

package org.apache.paimon.postpone;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link BucketFiles}. */
class BucketFilesTest {

    @Test
    void testPreservesIndexChangesFromDataAndCompactIncrements() {
        IndexFileMeta dataAdd = index("data-add");
        IndexFileMeta dataDelete = index("data-delete");
        IndexFileMeta compactAdd = index("compact-add");
        IndexFileMeta compactDelete = index("compact-delete");
        DataIncrement dataIncrement =
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(dataAdd),
                        Collections.singletonList(dataDelete));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(compactAdd),
                        Collections.singletonList(compactDelete));

        BucketFiles bucketFiles =
                new BucketFiles(mock(DataFilePathFactory.class), mock(FileIO.class));
        bucketFiles.update(new CommitMessageImpl(EMPTY_ROW, 0, 1, dataIncrement, compactIncrement));

        CompactIncrement result = bucketFiles.makeMessage(EMPTY_ROW, 0).compactIncrement();
        assertThat(result.newIndexFiles()).containsExactly(dataAdd, compactAdd);
        assertThat(result.deletedIndexFiles()).containsExactly(dataDelete, compactDelete);
    }

    private static IndexFileMeta index(String name) {
        return new IndexFileMeta(name, "ann", 1, 1, null, null, null);
    }
}
