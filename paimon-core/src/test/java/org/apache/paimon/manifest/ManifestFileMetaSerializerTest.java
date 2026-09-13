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

package org.apache.paimon.manifest;

import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFileMetaSerializer}. */
public class ManifestFileMetaSerializerTest extends ObjectSerializerTestBase<ManifestFileMeta> {

    private static final int NUM_ENTRIES_PER_FILE = 10;

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();

    @Test
    void testFormatIdentifier() {
        assertThat(new ManifestFileMetaSerializer().toRow(object()).getInt(0)).isEqualTo(2);
    }

    @Test
    void testExtraFiles() throws IOException {
        ManifestFileMeta original = object();
        assertThat(original.extraFiles()).isNull();

        ManifestFileMetaSerializer serializer = new ManifestFileMetaSerializer();
        for (List<String> extraFiles :
                Arrays.asList(
                        null,
                        Collections.<String>emptyList(),
                        Arrays.asList("extra-1", "extra-2"))) {
            ManifestFileMeta meta =
                    new ManifestFileMeta(
                            original.fileName(),
                            original.fileSize(),
                            original.numAddedFiles(),
                            original.numDeletedFiles(),
                            original.partitionStats(),
                            original.schemaId(),
                            original.minBucket(),
                            original.maxBucket(),
                            original.minLevel(),
                            original.maxLevel(),
                            original.minRowId(),
                            original.maxRowId(),
                            extraFiles);

            ManifestFileMeta fromRow = serializer.fromRow(serializer.toRow(meta));
            ManifestFileMeta fromBytes = serializer.deserializeFromBytes(meta.toBytes());
            assertThat(fromRow).isEqualTo(meta);
            assertThat(fromBytes).isEqualTo(meta).hasSameHashCodeAs(meta);
            assertThat(fromRow.extraFiles()).isEqualTo(extraFiles);
            assertThat(fromBytes.extraFiles()).isEqualTo(extraFiles);
            if (extraFiles == null) {
                assertThat(meta).isEqualTo(original).hasSameHashCodeAs(original);
            } else {
                assertThat(meta).isNotEqualTo(original);
            }
        }
    }

    @Override
    protected ObjectSerializer<ManifestFileMeta> serializer() {
        return new ManifestFileMetaSerializer();
    }

    @Override
    protected ManifestFileMeta object() {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < NUM_ENTRIES_PER_FILE; i++) {
            entries.add(gen.next());
        }
        return gen.createManifestFileMeta(entries);
    }
}
