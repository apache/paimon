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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** This test mainly test for the methods in {@link ManifestReadThreadPool}. */
public class ManifestReadThreadPoolTest {

    @Test
    public void testParallelismBatchIterable() {
        List<Integer> nums = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            nums.add(i);
        }

        Iterable<Integer> re = sequentialBatchedExecute(i -> singletonList(i + 1), nums, null);

        AtomicInteger atomicInteger = new AtomicInteger(0);
        re.forEach(
                i ->
                        Assertions.assertThat(i)
                                .isEqualTo(nums.get(atomicInteger.getAndIncrement()) + 1));
    }

    @Test
    public void testParallelismBatchIterable2() {
        List<Integer> nums = new ArrayList<>();

        for (int i = 0; i < 12345; i++) {
            nums.add(i);
        }

        Iterable<Integer> re = sequentialBatchedExecute(i -> singletonList(i + 1), nums, null);

        AtomicInteger atomicInteger = new AtomicInteger(0);
        re.forEach(
                i ->
                        Assertions.assertThat(i)
                                .isEqualTo(nums.get(atomicInteger.getAndIncrement()) + 1));
    }

    @Test
    public void testParallelismBatchIterable3() {
        List<Integer> nums = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            nums.add(i);
        }

        Iterable<Integer> re = sequentialBatchedExecute(i -> singletonList(i + 1), nums, null);

        Iterator<Integer> iterator = re.iterator();
        for (int i = 0; i < 100; i++) {
            iterator.hasNext();
        }

        AtomicInteger atomicInteger = new AtomicInteger(0);
        while (iterator.hasNext()) {
            Integer i = iterator.next();
            Assertions.assertThat(i).isEqualTo(nums.get(atomicInteger.getAndIncrement()) + 1);
        }
    }

    @Test
    public void testParallelismBatchIterable4() {
        List<Integer> nums = new ArrayList<>();

        for (int i = 0; i < 12345; i++) {
            nums.add(i);
        }

        Iterable<Integer> re = sequentialBatchedExecute(i -> singletonList(i + 1), nums, null);

        Iterator<Integer> iterator = re.iterator();
        for (int i = 0; i < 123; i++) {
            iterator.hasNext();
        }

        AtomicInteger atomicInteger = new AtomicInteger(0);
        while (iterator.hasNext()) {
            Integer i = iterator.next();
            Assertions.assertThat(i).isEqualTo(nums.get(atomicInteger.getAndIncrement()) + 1);
        }
    }

    @Test
    public void testForEmptyInput() {
        Iterable<Integer> re =
                sequentialBatchedExecute(
                        i -> singletonList(i + 1), (List<Integer>) Collections.EMPTY_LIST, null);
        Assertions.assertThat(!re.iterator().hasNext()).isTrue();
    }

    @Test
    public void testForSingletonInput() {
        Iterable<Integer> re =
                sequentialBatchedExecute(i -> singletonList(i + 1), singletonList(1), null);
        re.forEach(i -> Assertions.assertThat(i).isEqualTo(2));
    }

    @Test
    public void testDifferentQueueSizeWithFilterElement() {
        for (int queueSize = 1; queueSize < 20; queueSize++) {
            Iterable<Integer> re =
                    sequentialBatchedExecute(
                            i -> i > 5 ? singletonList(i) : emptyList(),
                            Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                            queueSize);
            Integer[] result = new Integer[] {6, 7, 8, 9, 10};

            Assertions.assertThat(re).hasSameElementsAs(Arrays.asList(result));
        }
    }
}
