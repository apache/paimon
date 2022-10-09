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
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.TestReusingRecordReader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test cases for {@link RecordReaderRegionManager}. */
public class RecordReaderRegionManagerTest {
    /**
     * Split two subregion list to multiple regions. In this test case we build two subregion as
     * follows:
     *
     * <ul>
     *   <li>subregion1 with key ranges: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [610, 700]
     *   <li>subregion2 with key ranges: [4, 10] [20, 30] [20, 190] [195, 500] [200, 605]
     * </ul>
     *
     * <p>All readers in the subregions will be divided into four regions:
     *
     * <ul>
     *   <li>region1: subregion1 with key ranges [1, 2]
     *   <li>region2: subregion1 with key ranges [3, 4] [5, 180] [5, 190], subregion2 with key
     *       ranges [4, 10] [20, 30] [20, 190]
     *   <li>region3: subregion1 with key ranges [200, 600], subregion2 with key ranges [195, 500]
     *       [200 605]
     *   <li>region4: subregion1 with key ranges [610, 700]
     * </ul>
     */
    @Test
    public void testSplitReaderRegions() throws Exception {
        RecordReaderSubRegion<KeyValue> subRegion1 =
                new RecordReaderSubRegion<>(
                        0,
                        new ArrayList<>(
                                Arrays.asList(
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(1),
                                                generateKey(2)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(3),
                                                generateKey(4)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(5),
                                                generateKey(180)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(5),
                                                generateKey(190)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(200),
                                                generateKey(600)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(610),
                                                generateKey(700)))));
        RecordReaderSubRegion<KeyValue> subRegion2 =
                new RecordReaderSubRegion<>(
                        1,
                        new ArrayList<>(
                                Arrays.asList(
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(4),
                                                generateKey(10)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(20),
                                                generateKey(30)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(20),
                                                generateKey(190)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(195),
                                                generateKey(500)),
                                        new SortedRegionDataRecordReader<>(
                                                () ->
                                                        new TestReusingRecordReader(
                                                                new ArrayList<>()),
                                                generateKey(200),
                                                generateKey(605)))));

        Comparator<RowData> keyComparator = Comparator.comparingInt(o -> o.getInt(0));
        RecordReaderRegionManager<KeyValue> regionManager =
                new RecordReaderRegionManager<>(
                        Arrays.asList(subRegion1, subRegion2), keyComparator);

        List<RecordReaderRegion<KeyValue>> regionList = regionManager.getRegionList();
        // Check region count
        assertEquals(4, regionList.size());

        // Validate key ranges in region1
        RecordReaderRegion<KeyValue> region1 = regionList.get(0);
        List<RecordReader<KeyValue>> region1Readers = region1.getReaders();
        assertEquals(1, region1Readers.size());
        assertEquals(1, region1.getReader(0).getSortedReaders().size());
        validateSortedReaderKeyRange(
                region1.getReader(0).getSortedReaders().get(0), 1, 2, keyComparator);

        // Validate key ranges in region2
        RecordReaderRegion<KeyValue> region2 = regionList.get(1);
        List<RecordReader<KeyValue>> region2Readers = region2.getReaders();
        assertEquals(2, region2Readers.size());

        RecordReaderSubRegion<KeyValue> region2SubRegion0 = region2.getReader(0);
        assertEquals(3, ((ConcatRecordReader<KeyValue>) region2Readers.get(0)).getReaderCount());
        validateSortedReaderKeyRange(
                region2SubRegion0.getSortedReaders().get(0), 3, 4, keyComparator);
        validateSortedReaderKeyRange(
                region2SubRegion0.getSortedReaders().get(1), 5, 180, keyComparator);
        validateSortedReaderKeyRange(
                region2SubRegion0.getSortedReaders().get(2), 5, 190, keyComparator);

        RecordReaderSubRegion<KeyValue> region2SubRegion1 = region2.getReader(1);
        assertEquals(3, ((ConcatRecordReader<KeyValue>) region2Readers.get(0)).getReaderCount());
        validateSortedReaderKeyRange(
                region2SubRegion1.getSortedReaders().get(0), 4, 10, keyComparator);
        validateSortedReaderKeyRange(
                region2SubRegion1.getSortedReaders().get(1), 20, 30, keyComparator);
        validateSortedReaderKeyRange(
                region2SubRegion1.getSortedReaders().get(2), 20, 190, keyComparator);
    }

    private void validateSortedReaderKeyRange(
            SortedRegionDataRecordReader<KeyValue> sortedReader,
            int minKey,
            int maxKey,
            Comparator<RowData> keyComparator) {
        assertEquals(0, keyComparator.compare(generateKey(minKey), sortedReader.getMinKey()));
        assertEquals(0, keyComparator.compare(generateKey(maxKey), sortedReader.getMaxKey()));
    }

    private RowData generateKey(int value) {
        GenericRowData data = new GenericRowData(1);
        data.setField(0, value);
        return data;
    }
}
