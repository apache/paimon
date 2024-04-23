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

package org.apache.paimon.utils;

import org.apache.paimon.data.serializer.IntSerializer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ParallelExecution.ParallelBatch;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ParallelExecution}. */
public class ParallelExecutionTest {

    @Test
    public void testNormal() {
        Supplier<Pair<RecordReader<Integer>, Integer>> supplier1 =
                () ->
                        Pair.of(
                                create(
                                        new LinkedList<>(
                                                Arrays.asList(
                                                        Arrays.asList(1, 5, 6),
                                                        Arrays.asList(2, 7)))),
                                1);
        Supplier<Pair<RecordReader<Integer>, Integer>> supplier2 =
                () ->
                        Pair.of(
                                create(
                                        new LinkedList<>(
                                                Arrays.asList(
                                                        Arrays.asList(33, 55),
                                                        Arrays.asList(22, 77)))),
                                2);
        Supplier<Pair<RecordReader<Integer>, Integer>> supplier3 =
                () ->
                        Pair.of(
                                create(
                                        new LinkedList<>(
                                                Arrays.asList(
                                                        Arrays.asList(333, 555),
                                                        Arrays.asList(222, 777)))),
                                3);

        ParallelExecution<Integer, Integer> execution =
                new ParallelExecution<>(
                        new IntSerializer(),
                        1024,
                        2,
                        Arrays.asList(supplier1, supplier2, supplier3));
        List<Pair<Integer, Integer>> result = collect(execution);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Pair.of(1, 1),
                        Pair.of(5, 1),
                        Pair.of(6, 1),
                        Pair.of(2, 1),
                        Pair.of(7, 1),
                        Pair.of(33, 2),
                        Pair.of(55, 2),
                        Pair.of(22, 2),
                        Pair.of(77, 2),
                        Pair.of(333, 3),
                        Pair.of(555, 3),
                        Pair.of(222, 3),
                        Pair.of(777, 3));
    }

    @Test
    public void testException() {
        String message = "Test Exception";

        Supplier<Pair<RecordReader<Integer>, Integer>> supplier1 =
                () ->
                        Pair.of(
                                create(
                                        new LinkedList<>(
                                                Arrays.asList(
                                                        Arrays.asList(1, 5, 6),
                                                        Arrays.asList(2, 7)))),
                                1);
        RecordReader<Integer> exReader =
                new RecordReader<Integer>() {
                    @Nullable
                    @Override
                    public RecordIterator<Integer> readBatch() {
                        throw new RuntimeException(message);
                    }

                    @Override
                    public void close() {}
                };

        ParallelExecution<Integer, Integer> execution =
                new ParallelExecution<>(
                        new IntSerializer(),
                        1024,
                        2,
                        Arrays.asList(supplier1, supplier1, () -> Pair.of(exReader, 2)));
        assertThatThrownBy(() -> collect(execution)).hasMessageContaining(message);
    }

    private RecordReader<Integer> create(Queue<List<Integer>> queue) {
        return new RecordReader<Integer>() {
            @Nullable
            @Override
            public RecordIterator<Integer> readBatch() {
                List<Integer> values = queue.poll();
                if (values == null) {
                    return null;
                }
                Queue<Integer> vQueue = new LinkedList<>(values);
                return new RecordIterator<Integer>() {
                    @Nullable
                    @Override
                    public Integer next() {
                        return vQueue.poll();
                    }

                    @Override
                    public void releaseBatch() {}
                };
            }

            @Override
            public void close() {}
        };
    }

    private List<Pair<Integer, Integer>> collect(ParallelExecution<Integer, Integer> execution) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();
        while (true) {
            try {
                ParallelBatch<Integer, Integer> batch = execution.take();
                if (batch == null) {
                    break;
                }

                while (true) {
                    Integer record = batch.next();
                    if (record == null) {
                        batch.releaseBatch();
                        break;
                    }

                    result.add(Pair.of(record, batch.extraMessage()));
                }
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return result;
    }
}
