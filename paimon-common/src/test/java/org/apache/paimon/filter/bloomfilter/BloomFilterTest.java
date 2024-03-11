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

package org.apache.paimon.filter.bloomfilter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Tests for {@link BloomFilter}. */
public class BloomFilterTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testAddFindByRandom() {

        BloomFilter filter = new BloomFilter();
        List<byte[]> testData = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            testData.add(random());
        }

        testData.forEach(filter::add);

        for (byte[] bytes : testData) {
            Assertions.assertThat(filter.testContains(bytes)).isTrue();
        }

        int errorCount = 0;
        int num = 1000000;
        for (int i = 0; i < num; i++) {
            byte[] ra = random();
            if (filter.testContains(random())) {
                errorCount++;
            }
        }

        System.out.println((double) errorCount / num);

        // ffp should be less than 0.03
        Assertions.assertThat((double) errorCount / num).isLessThan(0.03);
    }

    private byte[] random() {
        byte[] b = new byte[Math.abs(RANDOM.nextInt(40) + 1)];
        RANDOM.nextBytes(b);
        return b;
    }
}
