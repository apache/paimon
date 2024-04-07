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

package org.apache.paimon.operation.metrics;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactTimer}. */
public class CompactTimerTest {

    @Test
    public void testRandom() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int totalLength = random.nextInt(1, 1000);
        int queryLength;
        if (random.nextBoolean() && totalLength > 100) {
            queryLength = random.nextInt(1, 20);
        } else {
            queryLength = random.nextInt(1, totalLength + 1);
        }
        boolean[] running = new boolean[totalLength];
        CompactTimer timer = new CompactTimer(queryLength);

        boolean now = false;
        for (int i = 0; i < totalLength; i++) {
            if (random.nextInt(10) == 0) {
                // change state
                now = !now;
                if (now) {
                    timer.start(i);
                } else {
                    timer.finish(i);
                }
            }
            running[i] = now;

            if (random.nextInt(10) == 0) {
                // query
                int expected = 0;
                for (int j = 1; j <= queryLength; j++) {
                    if (i - j < 0) {
                        break;
                    }
                    if (running[i - j]) {
                        expected += 1;
                    }
                }
                assertThat(timer.calculateLength(i)).isEqualTo(expected);
            }
        }
    }
}
