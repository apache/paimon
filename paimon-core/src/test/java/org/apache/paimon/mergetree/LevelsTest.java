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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;

import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Levels}. */
public class LevelsTest {

    private final Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));

    @Test
    public void testNonEmptyHighestLevelNo() {
        Levels levels = new Levels(comparator, Collections.emptyList(), 3);
        assertThat(levels.nonEmptyHighestLevel()).isEqualTo(-1);
    }

    @Test
    public void testNonEmptyHighestLevel0() {
        Levels levels = new Levels(comparator, Arrays.asList(newFile(0), newFile(0)), 3);
        assertThat(levels.nonEmptyHighestLevel()).isEqualTo(0);
    }

    @Test
    public void testNonEmptyHighestLevel1() {
        Levels levels = new Levels(comparator, Arrays.asList(newFile(0), newFile(1)), 3);
        assertThat(levels.nonEmptyHighestLevel()).isEqualTo(1);
    }

    @Test
    public void testNonEmptyHighestLevel2() {
        Levels levels =
                new Levels(comparator, Arrays.asList(newFile(0), newFile(1), newFile(2)), 3);
        assertThat(levels.nonEmptyHighestLevel()).isEqualTo(2);
    }

    @Test
    public void testLevel0WithSameSequenceNumbers() {
        Levels levels = new Levels(comparator, Arrays.asList(newFile(0), newFile(0)), 3);
        assertThat(levels.allFiles()).hasSize(2);
    }

    public static DataFileMeta newFile(int level) {
        return new DataFileMeta(
                UUID.randomUUID().toString(),
                0,
                1,
                row(0),
                row(0),
                row(0),
                null,
                null,
                0,
                1,
                0,
                level,
                0L);
    }
}
