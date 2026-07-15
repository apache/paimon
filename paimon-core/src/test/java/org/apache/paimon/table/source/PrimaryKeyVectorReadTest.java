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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests global candidate merging for primary-key vector search. */
class PrimaryKeyVectorReadTest {

    @Test
    void testMergesGlobalTopKWithDeterministicTies() {
        List<PrimaryKeyVectorRead.Candidate> candidates =
                Arrays.asList(
                        candidate(1, "file-c", 0, 2F),
                        candidate(1, "file-b", 1, 1F),
                        candidate(0, "file-a", 2, 1F));

        List<PrimaryKeyVectorRead.Candidate> result = PrimaryKeyVectorRead.topK(candidates, 2);

        assertThat(result)
                .extracting(
                        PrimaryKeyVectorRead.Candidate::bucket,
                        PrimaryKeyVectorRead.Candidate::dataFileName,
                        PrimaryKeyVectorRead.Candidate::rowPosition)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(0, "file-a", 2L),
                        org.assertj.core.groups.Tuple.tuple(1, "file-b", 1L));
    }

    private static PrimaryKeyVectorRead.Candidate candidate(
            int bucket, String fileName, long position, float distance) {
        return new PrimaryKeyVectorRead.Candidate(
                BinaryRow.EMPTY_ROW, bucket, fileName, position, distance);
    }
}
