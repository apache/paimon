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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LazyFilteredBTreeReader} to read multiple files. */
@ExtendWith(ParameterizedTestExtension.class)
public class LazyFilteredBTreeIndexReaderTest extends AbstractIndexReaderTest {

    LazyFilteredBTreeIndexReaderTest(List<Object> args) {
        super(args);
    }

    @Override
    protected GlobalIndexReader prepareDataAndCreateReader() throws Exception {
        List<GlobalIndexIOMeta> written = writeData();

        return globalIndexer.createReader(fileReader, written);
    }

    private List<GlobalIndexIOMeta> writeData() throws Exception {
        int fileNum = 10;
        List<GlobalIndexIOMeta> written = new ArrayList<>(fileNum);

        int currentStart = 0;
        int recordPerFile = dataNum / fileNum;
        while (currentStart < dataNum) {
            int nextStart = Math.min(currentStart + recordPerFile, dataNum);
            written.add(writeData(data.subList(currentStart, nextStart)));
            currentStart = nextStart;
        }

        return written;
    }

    @TestTemplate
    public void testUnorderedIterator() throws Exception {
        // Set some null values
        for (int i = dataNum - 1; i >= dataNum * 0.85; i--) {
            data.get(i).setLeft(null);
        }

        List<GlobalIndexIOMeta> written = writeData();

        Comparator<Object> nullComparator = Comparator.nullsFirst(comparator);

        // Build expected map from original data
        Map<Object, Set<Long>> expectedMap = new TreeMap<>(nullComparator);
        for (Pair<Object, Long> pair : data) {
            Object key = pair.getKey();
            Long rowId = pair.getValue();

            if (!expectedMap.containsKey(key)) {
                expectedMap.put(key, new TreeSet<>());
            }
            expectedMap.get(key).add(rowId);
        }

        Map<Object, Set<Long>> actualMap = new TreeMap<>(nullComparator);

        for (GlobalIndexIOMeta index : written) {
            try (BTreeIndexReader reader =
                    new BTreeIndexReader(keySerializer, fileReader, index, CACHE_MANAGER)) {

                Iterator<Pair<Object, long[]>> iter = reader.createIterator();

                // Collect all entries from iterator
                while (iter.hasNext()) {
                    Pair<Object, long[]> entry = iter.next();
                    Object key = entry.getLeft();
                    long[] rowIds = entry.getRight();

                    if (!actualMap.containsKey(key)) {
                        actualMap.put(key, new TreeSet<>());
                    }
                    for (long rowId : rowIds) {
                        actualMap.get(key).add(rowId);
                    }
                }
            }
        }

        // Verify all keys are present
        assertThat(actualMap.keySet()).containsExactlyElementsOf(expectedMap.keySet());

        // Verify all rowIds for each key
        for (Object key : expectedMap.keySet()) {
            assertThat(actualMap.get(key))
                    .as("RowIds for key: " + key)
                    .containsExactlyElementsOf(expectedMap.get(key));
        }
    }
}
