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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Tests for {@link ManifestFileMetaSerializer}. */
public class ManifestFileMetaSerializerTest extends ObjectSerializerTestBase<ManifestFileMeta> {

    private static final int NUM_ENTRIES_PER_FILE = 10;

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();

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

    @Test
    public void testCompatibilityV2() throws Exception {
        RowType partitionType = RowType.of(DataTypes.INT(), DataTypes.BIGINT());
        InternalRowSerializer rowSerializer = new InternalRowSerializer(partitionType);
        BinaryRow minFields = rowSerializer.toBinaryRow(GenericRow.of(1, 10L)).copy();
        BinaryRow maxFields = rowSerializer.toBinaryRow(GenericRow.of(2, 20L)).copy();
        InternalArraySerializer arraySerializer = new InternalArraySerializer(DataTypes.BIGINT());
        BinaryArray nullCounts =
                arraySerializer.toBinaryArray(new GenericArray(new long[] {0, 1})).copy();
        SimpleStats partitionStats = new SimpleStats(minFields, maxFields, nullCounts);
        ManifestFileMeta meta =
                new ManifestFileMeta(
                        "test-manifest-file",
                        1024,
                        5,
                        2,
                        partitionStats,
                        1,
                        null,
                        null,
                        null,
                        null);

        byte[] v2Bytes =
                IOUtils.readFully(
                        ManifestFileMetaSerializerTest.class
                                .getClassLoader()
                                .getResourceAsStream("compatibility/manifest-file-meta-v2"),
                        true);
        ManifestFileMeta deserialized = serializer().deserializeFromBytes(v2Bytes);
        assertThat(deserialized).isEqualTo(meta);
    }
}
