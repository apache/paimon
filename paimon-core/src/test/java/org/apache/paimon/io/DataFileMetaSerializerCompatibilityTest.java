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

/**
 * Tests that old 20-field binary data (written before {@code _COMMIT_SNAPSHOT_ID} was added) can be
 * deserialized by the legacy serializer with {@code commitSnapshotId == null}.
 */
public class DataFileMetaSerializerCompatibilityTest {

    private final DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();

    @Test
    public void testLegacyRoundTripPreservesNullCommitSnapshotId() throws Exception {
        DataFileMeta original = gen.next().meta;
        assertThat(original.commitSnapshotId()).isNull();

        DataFileMetaWriteColsLegacySerializer legacySerializer =
                new DataFileMetaWriteColsLegacySerializer();
        DataOutputSerializer output = new DataOutputSerializer(1024);
        legacySerializer.serialize(original, output);

        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
        DataFileMeta restored = legacySerializer.deserialize(input);

        assertThat(restored.commitSnapshotId()).isNull();
        assertThat(restored.fileName()).isEqualTo(original.fileName());
        assertThat(restored.fileSize()).isEqualTo(original.fileSize());
        assertThat(restored.rowCount()).isEqualTo(original.rowCount());
        assertThat(restored.level()).isEqualTo(original.level());
    }

    @Test
    public void testCurrentSerializerThenLegacyDeserializerDropsSnapshotId() throws Exception {
        DataFileMeta stamped = gen.next().meta.assignCommitSnapshotId(42L);

        DataFileMetaWriteColsLegacySerializer legacySerializer =
                new DataFileMetaWriteColsLegacySerializer();
        DataOutputSerializer output = new DataOutputSerializer(1024);
        legacySerializer.serialize(stamped, output);

        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
        DataFileMeta restored = legacySerializer.deserialize(input);

        assertThat(restored.commitSnapshotId()).isNull();
        assertThat(restored.fileName()).isEqualTo(stamped.fileName());
    }

    @Test
    public void testCurrentSerializerRoundTripPreservesSnapshotId() throws Exception {
        DataFileMeta stamped = gen.next().meta.assignCommitSnapshotId(99L);

        DataFileMetaSerializer currentSerializer = new DataFileMetaSerializer();
        DataOutputSerializer output = new DataOutputSerializer(1024);
        currentSerializer.serialize(stamped, output);

        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
        DataFileMeta restored = currentSerializer.deserialize(input);

        assertThat(restored.commitSnapshotId()).isEqualTo(99L);
        assertThat(restored.fileName()).isEqualTo(stamped.fileName());
    }
}
