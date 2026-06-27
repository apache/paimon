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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapSharedShreddingReader}. */
class MapSharedShreddingReaderTest {

    @Test
    void testReadProjectedPhysicalRowWithoutOverflowColumn() throws IOException {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "b", 1),
                        Collections.emptyMap(),
                        new TreeSet<Integer>(),
                        3,
                        2);

        GenericRow physicalMap = new GenericRow(4);
        physicalMap.setField(0, new GenericArray(new int[] {0, -1, 1}));
        physicalMap.setField(1, 10L);
        physicalMap.setField(2, null);
        physicalMap.setField(3, 20L);

        InternalMap restored = readMap(logicalType, fieldMeta, physicalMap);

        assertThat(restored.size()).isEqualTo(2);
        assertThat(restored.keyArray().getString(0)).isEqualTo(BinaryString.fromString("a"));
        assertThat(restored.keyArray().getString(1)).isEqualTo(BinaryString.fromString("b"));
        assertThat(restored.valueArray().getLong(0)).isEqualTo(10L);
        assertThat(restored.valueArray().getLong(1)).isEqualTo(20L);
    }

    @Test
    void testReadOverflowOnlyWhenOverflowColumnExists() throws IOException {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingFieldMeta fieldMeta =
                new MapSharedShreddingFieldMeta(
                        nameToId("a", 0, "overflowed", 1),
                        Collections.emptyMap(),
                        new TreeSet<Integer>(Collections.singletonList(1)),
                        1,
                        1);

        Map<Integer, Object> overflow = new LinkedHashMap<>();
        overflow.put(1, 30L);
        GenericRow physicalMap = new GenericRow(3);
        physicalMap.setField(0, new GenericArray(new int[] {-1}));
        physicalMap.setField(1, null);
        physicalMap.setField(2, new GenericMap(overflow));

        InternalMap restored = readMap(logicalType, fieldMeta, physicalMap);

        assertThat(restored.size()).isEqualTo(1);
        assertThat(restored.keyArray().getString(0))
                .isEqualTo(BinaryString.fromString("overflowed"));
        assertThat(restored.valueArray().getLong(0)).isEqualTo(30L);
    }

    private static InternalMap readMap(
            RowType logicalType, MapSharedShreddingFieldMeta fieldMeta, InternalRow physicalMap)
            throws IOException {
        GenericRow physicalRow = GenericRow.of(physicalMap);
        Map<String, MapSharedShreddingFieldMeta> fieldMetas = new LinkedHashMap<>();
        fieldMetas.put("metrics", fieldMeta);
        MapSharedShreddingReader reader =
                new MapSharedShreddingReader(
                        new SingleRowRecordReader(physicalRow), logicalType, fieldMetas);
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        InternalRow row = iterator.next();
        iterator.releaseBatch();
        reader.close();
        return row.getMap(0);
    }

    private static Map<String, Integer> nameToId(Object... pairs) {
        Map<String, Integer> result = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            result.put((String) pairs[i], (Integer) pairs[i + 1]);
        }
        return result;
    }

    private static class SingleRowRecordReader implements FileRecordReader<InternalRow> {

        private final InternalRow row;
        private boolean consumed;

        private SingleRowRecordReader(InternalRow row) {
            this.row = row;
        }

        @Nullable
        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            if (consumed) {
                return null;
            }
            consumed = true;
            return new SingleRowFileRecordIterator(row);
        }

        @Override
        public void close() {}
    }

    private static class SingleRowFileRecordIterator implements FileRecordIterator<InternalRow> {

        private final InternalRow row;
        private boolean consumed;

        private SingleRowFileRecordIterator(InternalRow row) {
            this.row = row;
        }

        @Override
        public long returnedPosition() {
            return consumed ? 0 : -1;
        }

        @Override
        public Path filePath() {
            return new Path("/tmp/shared-shredding-reader-test");
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (consumed) {
                return null;
            }
            consumed = true;
            return row;
        }

        @Override
        public void releaseBatch() {}
    }
}
