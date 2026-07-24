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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DataEvolutionUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compatibility tests for the id-based {@link DataFileMeta#writtenFieldIds()}: new files carry both
 * name-based {@code writeCols} and id-based {@code writtenFieldIds}; streams in the legacy 20-field
 * layout (before {@code _WRITTEN_FIELD_IDS}) keep deserializing and resolve to the same field ids
 * via {@link DataEvolutionUtils#writtenFieldIds}.
 */
public class DataFileMetaWrittenFieldIdsCompatibilityTest {

    private static DataFileMeta file(@Nullable List<String> writeCols, @Nullable int[] ids) {
        return DataFileMeta.forAppend(
                "f0.parquet",
                100L,
                10L,
                SimpleStats.EMPTY_STATS,
                0L,
                9L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                0L,
                writeCols,
                ids);
    }

    @Test
    public void testRoundTripWithWrittenFieldIds() throws IOException {
        DataFileMeta meta = file(Arrays.asList("a", "b"), new int[] {0, 1});
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        DataOutputSerializer out = new DataOutputSerializer(128);
        serializer.serialize(meta, out);
        DataFileMeta actual =
                serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));

        assertThat(actual.writeCols()).containsExactly("a", "b");
        assertThat(actual.writtenFieldIds()).containsExactly(0, 1);
        assertThat(actual).isEqualTo(meta);
    }

    @Test
    public void testRoundTripWithoutWrittenFieldIds() throws IOException {
        DataFileMeta meta = file(Arrays.asList("a", "b"), null);
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        DataOutputSerializer out = new DataOutputSerializer(128);
        serializer.serialize(meta, out);
        DataFileMeta actual =
                serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));

        assertThat(actual.writtenFieldIds()).isNull();
        assertThat(actual).isEqualTo(meta);
    }

    @Test
    public void testLegacyLayoutStreamStillDeserializes() throws IOException {
        // a stream written by an old version in the 20-field (_WRITE_COLS era) layout
        DataFileMeta legacyMeta = file(Arrays.asList("a", "b"), null);
        DataFileMetaWriteColsLegacySerializer legacySerializer =
                new DataFileMetaWriteColsLegacySerializer();
        DataOutputSerializer out = new DataOutputSerializer(128);
        legacySerializer.serialize(legacyMeta, out);

        // the new code reads it through the legacy serializer: names preserved, ids absent
        DataFileMeta actual =
                legacySerializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertThat(actual.writeCols()).containsExactly("a", "b");
        assertThat(actual.writtenFieldIds()).isNull();
    }

    @Test
    public void testNewStreamReadByOldSerializer() throws IOException {
        // forward-compat of the binary (DataSplit/CommitMessage) row layout: a stream written by
        // the NEW 21-field serializer must still be readable by the OLD 20-field serializer, which
        // should just drop the trailing _WRITTEN_FIELD_IDS field
        DataFileMeta meta = file(Arrays.asList("a", "b"), new int[] {0, 1});
        DataOutputSerializer out = new DataOutputSerializer(128);
        new DataFileMetaSerializer().serialize(meta, out);

        DataFileMeta readByOld =
                new DataFileMetaWriteColsLegacySerializer()
                        .deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertThat(readByOld.writeCols()).containsExactly("a", "b");
        assertThat(readByOld.writtenFieldIds()).isNull();
    }

    @Test
    public void testLegacySerializerDropsWrittenFieldIds() throws IOException {
        // writing a new meta through the legacy layout (e.g. for an old consumer) must not fail
        // and simply drops the id-based representation
        DataFileMeta meta = file(Arrays.asList("a", "b"), new int[] {0, 1});
        DataFileMetaWriteColsLegacySerializer legacySerializer =
                new DataFileMetaWriteColsLegacySerializer();
        DataOutputSerializer out = new DataOutputSerializer(128);
        legacySerializer.serialize(meta, out);

        DataFileMeta actual =
                legacySerializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertThat(actual.writeCols()).containsExactly("a", "b");
        assertThat(actual.writtenFieldIds()).isNull();
    }

    @Test
    public void testResolveWrittenFieldIds() {
        Function<Long, TableSchema> schemaFetcher =
                schemaId ->
                        TableSchema.create(
                                0L,
                                new Schema(
                                        Arrays.asList(
                                                new DataField(0, "a", DataTypes.INT()),
                                                new DataField(1, "b", DataTypes.STRING()),
                                                new DataField(2, "c", DataTypes.INT())),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyMap(),
                                        ""));

        // new file: ids used directly
        assertThat(
                        DataEvolutionUtils.writtenFieldIds(
                                file(Arrays.asList("b", "c"), new int[] {1, 2}), schemaFetcher))
                .containsExactly(1, 2);

        // old file: names resolved against the schema, same result
        assertThat(
                        DataEvolutionUtils.writtenFieldIds(
                                file(Arrays.asList("b", "c"), null), schemaFetcher))
                .containsExactly(1, 2);

        // old file containing row-tracking system fields resolves via the row-tracked schema
        assertThat(
                        DataEvolutionUtils.writtenFieldIds(
                                file(Arrays.asList("a", SpecialFields.ROW_ID.name()), null),
                                schemaFetcher))
                .containsExactly(0, SpecialFields.ROW_ID.id());

        // full write: null in both representations
        assertThat(DataEvolutionUtils.writtenFieldIds(file(null, null), schemaFetcher)).isNull();
    }
}
