/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.mergetree.region;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.TestReusingRecordReader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test cases for {@link RecordReaderRegion}. */
public class RecordReaderRegionTest {
    @Test
    public void testIndexExceedCount() {
        final int count = 2;

        RecordReaderRegion<KeyValue> region =
                new RecordReaderRegion<>(Comparator.comparingInt(o -> o.getInt(0)), count);
        TestReusingRecordReader reader = new TestReusingRecordReader(new ArrayList<>());
        assertThatThrownBy(
                        () ->
                                region.mergeReader(
                                        new SortedRegionDataRecordReader<>(
                                                () -> reader, null, null),
                                        count))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        String.format(
                                "The index[%s] of merged reader must be < the readers count[%s]",
                                count, count));
        assertTrue(region.isEmpty());
    }

    @Test
    public void testMergeReaderWithNullKey() throws Exception {
        final int count = 2;
        final int index = count - 1;
        RecordReaderRegion<KeyValue> region =
                new RecordReaderRegion<>(Comparator.comparingInt(o -> o.getInt(0)), count);
        assertTrue(region.isEmpty());

        TestReusingRecordReader reader = new TestReusingRecordReader(new ArrayList<>());
        assertTrue(
                region.mergeReader(
                        new SortedRegionDataRecordReader<>(() -> reader, null, null), index));

        assertFalse(region.isEmpty());
        assertTrue(region.getReader(0).isEmpty());
        assertEquals(1, region.getReader(index).getSortedReaders().size());
        assertEquals(1, region.getReaders().size());
    }

    @Test
    public void testMergeReaderWithRange() throws Exception {
        final int count = 2;
        final Comparator<RowData> keyComparator = Comparator.comparingInt(o -> o.getInt(0));
        RecordReaderRegion<KeyValue> region = new RecordReaderRegion<>(keyComparator, count);

        TestReusingRecordReader reader = new TestReusingRecordReader(new ArrayList<>());

        // merge key range [0, 100] with index 0 and key range [0, 150] with index 1
        assertTrue(
                region.mergeReader(
                        new SortedRegionDataRecordReader<>(
                                () -> reader, generateKey(0), generateKey(100)),
                        0));
        assertEquals(0, keyComparator.compare(generateKey(0), region.getMinKey()));
        assertEquals(0, keyComparator.compare(generateKey(100), region.getMaxKey()));
        assertTrue(
                region.mergeReader(
                        new SortedRegionDataRecordReader<>(
                                () -> reader, generateKey(0), generateKey(150)),
                        1));
        assertEquals(0, keyComparator.compare(generateKey(0), region.getMinKey()));
        assertEquals(0, keyComparator.compare(generateKey(150), region.getMaxKey()));

        // merge key range [60, 100] with index 1
        assertTrue(
                region.mergeReader(
                        new SortedRegionDataRecordReader<>(
                                () -> reader, generateKey(60), generateKey(100)),
                        1));
        assertEquals(0, keyComparator.compare(generateKey(0), region.getMinKey()));
        assertEquals(0, keyComparator.compare(generateKey(150), region.getMaxKey()));

        // merge key range [200, 300] with index 0
        assertFalse(
                region.mergeReader(
                        new SortedRegionDataRecordReader<>(
                                () -> reader, generateKey(200), generateKey(300)),
                        0));
        assertEquals(0, keyComparator.compare(generateKey(0), region.getMinKey()));
        assertEquals(0, keyComparator.compare(generateKey(150), region.getMaxKey()));

        assertEquals(2, region.getReaders().size());

        // merge range [0, 50] with index 1
        assertThatThrownBy(
                        () ->
                                region.mergeReader(
                                        new SortedRegionDataRecordReader<>(
                                                () -> reader, generateKey(-50), generateKey(500)),
                                        1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The added minKey in reader must be equal or larger than the minKey in region");
    }

    private RowData generateKey(int value) {
        GenericRowData data = new GenericRowData(1);
        data.setField(0, value);
        return data;
    }
}
