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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.manifest.ManifestIndexTestUtils.withIndexFileName;
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
    void testIndexReferenceRoundTripAndEquality() throws Exception {
        ManifestFileMeta meta = object();
        ManifestFileMeta indexed = withIndexFileName(meta, "independent-index");
        ManifestFileMetaSerializer serializer = new ManifestFileMetaSerializer();
        assertThat(serializer.fromRow(serializer.toRow(indexed))).isEqualTo(indexed);
        assertThat(serializer.deserializeFromBytes(serializer.serializeToBytes(indexed)))
                .isEqualTo(indexed);
        assertThat(indexed).isNotEqualTo(meta);
        assertThat(indexed.hashCode())
                .isEqualTo(withIndexFileName(meta, "independent-index").hashCode());
        assertThat(indexed.toString()).contains("independent-index");
        assertThat(serializer.fromRow(serializer.toRow(meta)).indexFileName()).isNull();
    }

    @Test
    void testOldRowWithoutIndexField() {
        ManifestFileMeta meta = object();
        ManifestFileMetaSerializer serializer = new ManifestFileMetaSerializer();
        GenericRow current = (GenericRow) serializer.toRow(meta);
        GenericRow legacy = new GenericRow(current.getFieldCount() - 1);
        for (int i = 0; i < legacy.getFieldCount(); i++) {
            legacy.setField(i, current.getField(i));
        }
        assertThat(serializer.fromRow(legacy)).isEqualTo(meta);
        assertThat(serializer.fromRow(legacy).indexFileName()).isNull();
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
