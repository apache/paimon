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

package org.apache.paimon.data.shredding;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapSharedShreddingReader}. */
class MapSharedShreddingReaderTest {

    @Test
    void testRebuildLogicalMap() throws IOException {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        Map<String, MapSharedShreddingFieldMeta> metas = new TreeMap<>();
        metas.put(
                "tags",
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "b", 1, "c", 2),
                        fieldToColumns(
                                0, Collections.singletonList(0),
                                1, Collections.singletonList(1)),
                        new TreeSet<>(Collections.singletonList(2)),
                        2,
                        3));

        List<InternalRow> physicalRows =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                GenericRow.of(
                                        new GenericArray(new int[] {0, 1}),
                                        10L,
                                        20L,
                                        null)),
                        GenericRow.of(
                                2,
                                GenericRow.of(
                                        new GenericArray(new int[] {0, -1}),
                                        40L,
                                        null,
                                        intKeyMap(2, null))),
                        GenericRow.of(
                                3,
                                GenericRow.of(
                                        new GenericArray(new int[] {1, -1}),
                                        null,
                                        null,
                                        null)),
                        GenericRow.of(4, null));

        try (MapSharedShreddingReader reader =
                new MapSharedShreddingReader(
                        new InMemoryReader(physicalRows), logicalType, metas)) {
            FileRecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();

            InternalRow row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            assertThat(row.getMap(1)).isEqualTo(stringKeyMap("a", 10L, "b", 20L));

            row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(2);
            assertThat(row.getMap(1)).isEqualTo(stringKeyMap("a", 40L, "c", null));

            row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(3);
            assertThat(row.getMap(1)).isEqualTo(stringKeyMap("b", null));

            row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(4);
            assertThat(row.isNullAt(1)).isTrue();
            assertThat(row.getMap(1)).isNull();

            assertThat(batch.next()).isNull();
            batch.releaseBatch();
            assertThat(reader.readBatch()).isNull();
        }
    }

    @Test
    void testRebuildMultipleLogicalMaps() throws IOException {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1,
                                "tags",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                        DataTypes.FIELD(
                                2,
                                "plain",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                        DataTypes.FIELD(
                                3,
                                "attrs",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
        Map<String, MapSharedShreddingFieldMeta> metas = new TreeMap<>();
        metas.put(
                "tags",
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "b", 1, "c", 2),
                        fieldToColumns(
                                0, Collections.singletonList(0),
                                1, Collections.singletonList(1)),
                        new TreeSet<>(Collections.singletonList(2)),
                        2,
                        3));
        metas.put(
                "attrs",
                new MapSharedShreddingFieldMeta(
                        nameToId("x", 10, "y", 11),
                        fieldToColumns(
                                10, Collections.singletonList(0),
                                11, Collections.singletonList(1)),
                        new TreeSet<Integer>(),
                        2,
                        2));

        List<InternalRow> physicalRows =
                Collections.singletonList(
                        GenericRow.of(
                                1,
                                GenericRow.of(
                                        new GenericArray(new int[] {0, -1}),
                                        10L,
                                        null,
                                        intKeyMap(2, 30L)),
                                stringKeyMap("plain-key", BinaryString.fromString("plain-value")),
                                GenericRow.of(
                                        new GenericArray(new int[] {11, 10}),
                                        BinaryString.fromString("value-y"),
                                        null)));

        try (MapSharedShreddingReader reader =
                new MapSharedShreddingReader(
                        new InMemoryReader(physicalRows), logicalType, metas)) {
            FileRecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();

            InternalRow row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            assertThat(row.getMap(1)).isEqualTo(stringKeyMap("a", 10L, "c", 30L));
            assertThat(row.getMap(2))
                    .isEqualTo(
                            stringKeyMap(
                                    "plain-key", BinaryString.fromString("plain-value")));
            assertThat(row.getMap(3))
                    .isEqualTo(stringKeyMap("y", BinaryString.fromString("value-y"), "x", null));

            assertThat(batch.next()).isNull();
            batch.releaseBatch();
            assertThat(reader.readBatch()).isNull();
        }
    }

    @Test
    void testInvalidNullFieldMapping() throws IOException {
        assertInvalidPhysicalTags(
                GenericRow.of(null, 10L, 20L, null),
                "Shared-shredding field mapping cannot be null");
    }

    @Test
    void testInvalidNullFieldMappingElement() throws IOException {
        assertInvalidPhysicalTags(
                GenericRow.of(new GenericArray(new Object[] {0, null}), 10L, 20L, null),
                "Shared-shredding field mapping element cannot be null");
    }

    @Test
    void testInvalidFieldMappingSize() throws IOException {
        assertInvalidPhysicalTags(
                GenericRow.of(new GenericArray(new int[] {0}), 10L, null, null),
                "Shared-shredding field mapping size 1 does not match metadata num columns 2");
    }

    @Test
    void testInvalidUnknownFieldId() throws IOException {
        assertInvalidPhysicalTags(
                GenericRow.of(new GenericArray(new int[] {0, 99}), 10L, 20L, null),
                "Cannot find shared-shredding field id 99 in metadata");
    }

    @Test
    void testInvalidUnknownOverflowFieldId() throws IOException {
        assertInvalidPhysicalTags(
                GenericRow.of(
                        new GenericArray(new int[] {0, -1}), 10L, null, intKeyMap(99, 20L)),
                "Cannot find shared-shredding field id 99 in metadata");
    }

    private static void assertInvalidPhysicalTags(InternalRow physicalTags, String message)
            throws IOException {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        try (MapSharedShreddingReader reader =
                new MapSharedShreddingReader(
                        new InMemoryReader(
                                Collections.singletonList(GenericRow.of(1, physicalTags))),
                        logicalType,
                        metas())) {
            FileRecordIterator<InternalRow> batch = reader.readBatch();
            InternalRow row = batch.next();
            assertThatThrownBy(() -> row.getMap(1)).hasMessageContaining(message);
            batch.releaseBatch();
        }
    }

    private static Map<String, MapSharedShreddingFieldMeta> metas() {
        Map<String, MapSharedShreddingFieldMeta> metas = new TreeMap<>();
        metas.put(
                "tags",
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "b", 1, "c", 2),
                        fieldToColumns(
                                0, Collections.singletonList(0),
                                1, Collections.singletonList(1)),
                        new TreeSet<>(Collections.singletonList(2)),
                        2,
                        3));
        return metas;
    }

    private static Map<String, Integer> nameToId(Object... kvs) {
        Map<String, Integer> map = new TreeMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    private static Map<Integer, List<Integer>> fieldToColumns(
            int fieldId0, List<Integer> columns0, int fieldId1, List<Integer> columns1) {
        Map<Integer, List<Integer>> map = new TreeMap<>();
        map.put(fieldId0, columns0);
        map.put(fieldId1, columns1);
        return map;
    }

    private static InternalMap stringKeyMap(Object... kvs) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(BinaryString.fromString((String) kvs[i]), kvs[i + 1]);
        }
        return new GenericMap(map);
    }

    private static InternalMap intKeyMap(Object... kvs) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return new GenericMap(map);
    }

    private static class InMemoryReader implements FileRecordReader<InternalRow> {

        private final List<InternalRow> rows;
        private boolean read;

        private InMemoryReader(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Nullable
        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            if (read) {
                return null;
            }
            read = true;
            return new InMemoryIterator(rows);
        }

        @Override
        public void close() {}
    }

    private static class InMemoryIterator implements FileRecordIterator<InternalRow> {

        private final List<InternalRow> rows;
        private int index;

        private InMemoryIterator(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public long returnedPosition() {
            return index - 1;
        }

        @Override
        public Path filePath() {
            return new Path("memory");
        }

        @Nullable
        @Override
        public InternalRow next() {
            return index < rows.size() ? rows.get(index++) : null;
        }

        @Override
        public void releaseBatch() {}
    }
}
