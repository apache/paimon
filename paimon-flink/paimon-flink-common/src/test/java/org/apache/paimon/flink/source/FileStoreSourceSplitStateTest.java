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

package org.apache.paimon.flink.source;

import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link FileStoreSourceSplitState}. */
public class FileStoreSourceSplitStateTest {

    @Test
    public void testRoundTripWithoutModification() {
        final FileStoreSourceSplit split = getTestSplit();
        final FileStoreSourceSplitState state = new FileStoreSourceSplitState(split);

        final FileStoreSourceSplit resultSplit = state.toSourceSplit();

        assertThat(resultSplit.recordsToSkip()).isEqualTo(split.recordsToSkip());
    }

    @Test
    public void testStateStartsWithSplitValues() {
        final FileStoreSourceSplit split = getTestSplit(456L);
        final FileStoreSourceSplitState state = new FileStoreSourceSplitState(split);

        assertThat(state.recordsToSkip()).isEqualTo(456L);
    }

    @Test
    public void testNewSplitTakesModifiedOffsetAndCount() {
        final FileStoreSourceSplit split = getTestSplit();
        final FileStoreSourceSplitState state = new FileStoreSourceSplitState(split);

        state.setPosition(new RecordAndPosition<>(null, RecordAndPosition.NO_OFFSET, 7566L));

        assertThat(state.toSourceSplit().recordsToSkip()).isEqualTo(7566L);
    }

    // ------------------------------------------------------------------------

    private static FileStoreSourceSplit getTestSplit() {
        return getTestSplit(0);
    }

    private static FileStoreSourceSplit getTestSplit(long recordsToSkip) {
        return newSourceSplit(
                "id", row(1), 2, Arrays.asList(newFile(0), newFile(1)), recordsToSkip);
    }
}
