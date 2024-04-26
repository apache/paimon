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
import java.util.List;
import java.util.Random;

/** Test for {@link BloomFilter64}. */
public class BloomFilter64Test {

    private static final Random RANDOM = new Random();

    @Test
    public void testFunction() {
        BloomFilter64 bloomFilter64 = new BloomFilter64(10000, 0.02);

        List<Long> testData = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            testData.add(RANDOM.nextLong());
        }
        testData.forEach(bloomFilter64::addHash);

        for (Long value : testData) {
            Assertions.assertThat(bloomFilter64.testHash(value)).isTrue();
        }

        int errorCount = 0;
        int num = 1000000;
        for (int i = 0; i < num; i++) {
            long ra = RANDOM.nextLong();
            if (bloomFilter64.testHash(ra) && !testData.contains(ra)) {
                errorCount++;
            }
        }

        // ffp should be less than 0.03
        Assertions.assertThat((double) errorCount / num).isLessThan(0.03);
    }
}
