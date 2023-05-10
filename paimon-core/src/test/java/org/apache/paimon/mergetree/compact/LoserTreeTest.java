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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ReusingTestData;
import org.apache.paimon.utils.TestReusingRecordReader;

import org.junit.jupiter.api.RepeatedTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LoserTree}. */
public class LoserTreeTest {
    private static final Comparator<KeyValue> KEY_COMPARATOR =
            (c1, c2) -> Integer.compare(c2.key().getInt(0), c1.key().getInt(0));
    private static final Comparator<KeyValue> SEQUENCE_COMPARATOR =
            (c1, c2) -> Long.compare(c2.sequenceNumber(), c1.sequenceNumber());

    @RepeatedTest(100)
    public void testLoserTreeIsOrdered() throws IOException {
        List<ReusingTestData> initialTestData = ReusingTestData.generateData(1000, false);
        List<ReusingTestData> finalDedupedTestData = new ArrayList<>();

        List<RecordReader<KeyValue>> sortedTestReaders = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numberReaders = random.nextInt(20) + 1;
        int lowerBound = 0, upperBound = initialTestData.size();
        for (int i = 0; i < numberReaders; i++) {
            int subUpperBound;
            if (i == numberReaders - 1) {
                subUpperBound = upperBound;
            } else {
                subUpperBound = random.nextInt(lowerBound, upperBound);
            }
            List<ReusingTestData> subReusingTestData =
                    new ArrayList<>(initialTestData.subList(lowerBound, subUpperBound));
            Collections.sort(subReusingTestData);
            // for each reader, there should not be two records with duplicate key
            dedup(subReusingTestData, 1);
            sortedTestReaders.add(new TestReusingRecordReader(subReusingTestData));
            finalDedupedTestData.addAll(subReusingTestData);
            lowerBound = subUpperBound;
        }
        Collections.sort(finalDedupedTestData);
        checkLoserTree(sortedTestReaders, finalDedupedTestData);
    }

    /**
     * @param list list of ReusingTestData
     * @param n max number of occurrence of record with the same key
     */
    private static List<ReusingTestData> dedup(List<ReusingTestData> list, int n) {
        if (list.size() == 0) {
            return list;
        }
        int count = 1;
        int currentKey = list.get(0).key;
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i).key == currentKey) {
                count++;
                if (count > n) {
                    list.remove(i);
                    i--;
                }
            } else {
                currentKey = list.get(i).key;
                count = 1;
            }
        }
        return list;
    }

    private void checkLoserTree(
            List<RecordReader<KeyValue>> sortedTestReaders, List<ReusingTestData> expectedData)
            throws IOException {
        try (LoserTree<KeyValue> loserTree =
                new LoserTree<>(sortedTestReaders, KEY_COMPARATOR, SEQUENCE_COMPARATOR)) {
            List<Object> popped = new ArrayList<>();
            Iterator<ReusingTestData> expectedIterator = expectedData.iterator();
            loserTree.initializeIfNeeded();
            do {
                if (loserTree.peekWinner() == null) {
                    loserTree.adjustForNextLoop();
                    assertThat(loserTree.peekWinner() != null);
                }
                for (KeyValue winner = loserTree.popWinner();
                        winner != null;
                        winner = loserTree.popWinner()) {
                    popped.add(winner);
                    assertThat(expectedIterator.hasNext());

                    ReusingTestData expected = expectedIterator.next();

                    expected.assertEquals(winner);
                }
                loserTree.adjustForNextLoop();
            } while (expectedIterator.hasNext());
        }
    }
}
