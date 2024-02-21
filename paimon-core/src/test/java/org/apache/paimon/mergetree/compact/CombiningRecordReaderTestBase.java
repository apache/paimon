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

import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ReusingTestData;
import org.apache.paimon.utils.TestReusingRecordReader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecordReader}s which combines several other {@link RecordReader}s. */
public abstract class CombiningRecordReaderTestBase {

    protected static final RecordComparator KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    protected abstract boolean addOnly();

    protected abstract List<ReusingTestData> getExpected(List<ReusingTestData> input);

    protected abstract RecordReader<KeyValue> createRecordReader(
            List<TestReusingRecordReader> readers, SortEngine sortEngine);

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testRandom(SortEngine sortEngine) throws IOException {
        for (int i = 0; i < 100; i++) {
            runTest(generateRandomData(), sortEngine);
        }
    }

    protected List<List<ReusingTestData>> parseData(String... stringsData) {
        List<List<ReusingTestData>> readersData = new ArrayList<>();
        for (String string : stringsData) {
            readersData.add(ReusingTestData.parse(string));
        }
        return readersData;
    }

    protected List<List<ReusingTestData>> generateRandomData() {
        Random random = new Random();
        int numReaders = random.nextInt(20) + 1;
        List<List<ReusingTestData>> readersData = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readersData.add(
                    ReusingTestData.generateOrderedNoDuplicatedKeys(
                            random.nextInt(100) + 1, addOnly()));
        }
        return readersData;
    }

    protected void runTest(List<List<ReusingTestData>> readersData, SortEngine sortEngine)
            throws IOException {
        Iterator<ReusingTestData> expectedIterator =
                getExpected(
                                readersData.stream()
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList()))
                        .iterator();
        List<TestReusingRecordReader> readers = new ArrayList<>();
        for (List<ReusingTestData> readerData : readersData) {
            readers.add(new TestReusingRecordReader(readerData));
        }
        RecordReader<KeyValue> recordReader = createRecordReader(readers, sortEngine);

        RecordReader.RecordIterator<KeyValue> batch;
        while ((batch = recordReader.readBatch()) != null) {
            KeyValue kv;
            while ((kv = batch.next()) != null) {
                assertThat(expectedIterator.hasNext()).isTrue();
                ReusingTestData expected = expectedIterator.next();
                expected.assertEquals(kv);
            }
            batch.releaseBatch();
        }
        assertThat(expectedIterator.hasNext()).isFalse();
        recordReader.close();

        for (TestReusingRecordReader reader : readers) {
            reader.assertCleanUp();
        }
    }
}
