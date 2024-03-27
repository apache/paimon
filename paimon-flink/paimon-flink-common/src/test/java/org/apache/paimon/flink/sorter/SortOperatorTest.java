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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

/** Test for {@link SortOperator}. */
public class SortOperatorTest {

    @Test
    public void testSort() throws Exception {
        RowType keyRowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "a", new BigIntType(), "Someone's desc.")));

        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new BigIntType()),
                                new DataField(1, "b", new VarCharType(), "Someone's desc."),
                                new DataField(2, "c", new VarCharType(), "Someone's desc.")));

        SortOperator sortOperator =
                new SortOperator(
                        keyRowType,
                        rowType,
                        MemorySize.parse("10 mb").getBytes(),
                        (int) MemorySize.parse("16 kb").getBytes(),
                        128,
                        "lz4",
                        1,
                        MemorySize.MAX_VALUE) {};

        OneInputStreamOperatorTestHarness harness = createTestHarness(sortOperator);
        harness.open();

        for (int i = 0; i < 10000; i++) {
            harness.processElement(
                    new StreamRecord<>(
                            GenericRow.of(
                                    (long) 10000 - i,
                                    BinaryString.fromString(""),
                                    BinaryString.fromString(""))));
        }

        BinaryExternalSortBuffer externalSortBuffer = sortOperator.getBuffer();
        MutableObjectIterator<BinaryRow> iterator = externalSortBuffer.sortedIterator();
        BinaryRow row;
        BinaryRow reuse = new BinaryRow(3);
        long i = 1;
        while ((row = iterator.next(reuse)) != null) {
            Assertions.assertThat(row.getLong(0)).isEqualTo(i++);
        }

        harness.close();
    }

    @Test
    public void testCloseSortOperator() throws Exception {
        RowType keyRowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "a", new VarCharType(), "Someone's desc.")));

        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new VarCharType(), "Someone's desc."),
                                new DataField(0, "b", new VarCharType(), "Someone's desc."),
                                new DataField(1, "c", new BigIntType())));

        SortOperator sortOperator =
                new SortOperator(
                        keyRowType,
                        rowType,
                        MemorySize.parse("10 mb").getBytes(),
                        (int) MemorySize.parse("16 kb").getBytes(),
                        128,
                        "lz4",
                        1,
                        MemorySize.MAX_VALUE) {};
        OneInputStreamOperatorTestHarness harness = createTestHarness(sortOperator);
        harness.open();
        File[] files = harness.getEnvironment().getIOManager().getSpillingDirectories();

        char[] data = new char[1024];
        for (int i = 0; i < 1024; i++) {
            data[i] = (char) ('a' + i % 26);
        }

        for (int i = 0; i < 10000; i++) {
            harness.processElement(
                    new StreamRecord<>(
                            GenericRow.of(
                                    BinaryString.fromString(String.valueOf(data)),
                                    BinaryString.fromString(String.valueOf(data)),
                                    (long) i)));
        }

        harness.close();
        for (File file : files) {
            assertNoDataFile(file);
        }
    }

    private void assertNoDataFile(File fileDir) {
        if (fileDir.exists()) {
            Assertions.assertThat(fileDir.isDirectory()).isTrue();
            for (File file : fileDir.listFiles()) {
                assertNoDataFile(file);
            }
        }
    }

    private OneInputStreamOperatorTestHarness createTestHarness(SortOperator operator)
            throws Exception {
        OneInputStreamOperatorTestHarness testHarness =
                new OneInputStreamOperatorTestHarness(operator);
        return testHarness;
    }
}
