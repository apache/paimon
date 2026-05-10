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

package org.apache.paimon.io;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@code commitSnapshotId} field on {@link DataFileMeta}. */
public class DataFileMetaCommitSnapshotIdTest {

    private final DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();

    @Test
    public void testDefaultIsNull() {
        DataFileMeta meta = gen.next().meta;
        assertThat(meta.commitSnapshotId()).isNull();
    }

    @Test
    public void testAssignAndPreserve() {
        DataFileMeta original = gen.next().meta;
        DataFileMeta stamped = original.assignCommitSnapshotId(42L);
        assertThat(stamped.commitSnapshotId()).isEqualTo(42L);

        // Copy methods should preserve the stamp.
        assertThat(stamped.upgrade(stamped.level() + 1).commitSnapshotId()).isEqualTo(42L);
        assertThat(stamped.assignSequenceNumber(1L, 2L).commitSnapshotId()).isEqualTo(42L);
        assertThat(stamped.copyWithoutStats().commitSnapshotId()).isEqualTo(42L);
        assertThat(stamped.rename("renamed").commitSnapshotId()).isEqualTo(42L);
    }

    @Test
    public void testSerializerRoundTripStamped() {
        DataFileMeta stamped = gen.next().meta.assignCommitSnapshotId(1234L);
        DataFileMeta restored =
                new DataFileMetaSerializer().fromRow(new DataFileMetaSerializer().toRow(stamped));
        assertThat(restored.commitSnapshotId()).isEqualTo(1234L);
        assertThat(restored).isEqualTo(stamped);
    }

    @Test
    public void testSerializerRoundTripUnstamped() {
        DataFileMeta meta = gen.next().meta;
        DataFileMeta restored =
                new DataFileMetaSerializer().fromRow(new DataFileMetaSerializer().toRow(meta));
        assertThat(restored.commitSnapshotId()).isNull();
        assertThat(restored).isEqualTo(meta);
    }
}
