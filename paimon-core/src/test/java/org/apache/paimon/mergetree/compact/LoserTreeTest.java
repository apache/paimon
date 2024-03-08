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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LoserTree}. */
public class LoserTreeTest {
    private static final Comparator<KeyValue> KEY_COMPARATOR =
            Comparator.<KeyValue>comparingInt(o -> o.key().getInt(0)).reversed();
    private static final Comparator<KeyValue> SEQUENCE_COMPARATOR =
            Comparator.comparingLong(KeyValue::sequenceNumber).reversed();

    @RepeatedTest(100)
    public void testLoserTreeIsOrdered() throws IOException {
        List<ReusingTestData> reusingTestData = new ArrayList<>();
        List<RecordReader<KeyValue>> sortedTestReaders =
                createSortedTestReaders(reusingTestData, 0, () -> Function::identity);
        Collections.sort(reusingTestData);
        try (LoserTree<KeyValue> loserTree =
                new LoserTree<>(sortedTestReaders, KEY_COMPARATOR, SEQUENCE_COMPARATOR)) {
            Iterator<ReusingTestData> expectedIterator = reusingTestData.iterator();
            checkLoserTree(
                    loserTree,
                    kv -> {
                        assertThat(expectedIterator.hasNext()).isTrue();
                        expectedIterator.next().assertEquals(kv);
                    });
            assertThat(expectedIterator.hasNext()).isFalse();
        }
    }

    @RepeatedTest(100)
    public void testLoserTreeCloseNormally() {
        List<RecordReader<KeyValue>> sortedTestReaders =
                createSortedTestReaders(
                        new ArrayList<>(),
                        1,
                        () ->
                                new TestReusingRecordReader.ThrowingRunnable<IOException>() {
                                    private boolean initialized = false;

                                    @Override
                                    public void run() throws IOException {
                                        if (initialized) {
                                            throw new IOException("ingest exception");
                                        }
                                        initialized = true;
                                    }
                                });
        LoserTree<KeyValue> loserTree =
                new LoserTree<>(sortedTestReaders, KEY_COMPARATOR, SEQUENCE_COMPARATOR);
        assertThatThrownBy(() -> checkLoserTree(loserTree, kv -> {}))
                .satisfies(anyCauseMatches(IOException.class));
        assertThatCode(loserTree::close).doesNotThrowAnyException();
    }

    private List<RecordReader<KeyValue>> createSortedTestReaders(
            List<ReusingTestData> reusingTestData,
            int minNumberRecords,
            Supplier<TestReusingRecordReader.ThrowingRunnable<IOException>> beforeReadBatch) {
        List<RecordReader<KeyValue>> sortedTestReaders = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numberReaders = random.nextInt(20) + 1;
        for (int i = 0; i < numberReaders; i++) {
            int numberRecords = Math.max(random.nextInt(100), minNumberRecords);
            List<ReusingTestData> testData =
                    ReusingTestData.generateOrderedNoDuplicatedKeys(numberRecords, false);
            reusingTestData.addAll(testData);
            sortedTestReaders.add(new TestReusingRecordReader(testData, beforeReadBatch.get()));
        }
        return sortedTestReaders;
    }

    private void checkLoserTree(LoserTree<KeyValue> loserTree, Consumer<KeyValue> consumer)
            throws IOException {
        loserTree.initializeIfNeeded();
        do {
            for (KeyValue winner = loserTree.popWinner();
                    winner != null;
                    winner = loserTree.popWinner()) {
                consumer.accept(winner);
            }
            loserTree.adjustForNextLoop();
        } while (loserTree.peekWinner() != null);
    }
}
