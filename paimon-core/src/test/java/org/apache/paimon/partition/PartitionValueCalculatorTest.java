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

package org.apache.paimon.partition;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test class for {@link PartitionValueCalculator}. */
public class PartitionValueCalculatorTest {

    @Test
    public void testGeneratePartitionValues() {
        PartitionValueCalculator valueGenerator =
                new PartitionValueCalculator("$dt $hour:00:00", "yyyyMMdd HH:mm:ss");
        LinkedHashMap<String, String> partitionValues =
                valueGenerator.calPartValues(
                        LocalDateTime.of(2023, 1, 1, 12, 0, 0), Arrays.asList("dt", "hour"));
        assertEquals(
                new LinkedHashMap<String, String>() {
                    {
                        put("dt", "20230101");
                        put("hour", "12");
                    }
                },
                partitionValues);
        valueGenerator = new PartitionValueCalculator("$dt", "yyyyMMdd");
        partitionValues =
                valueGenerator.calPartValues(
                        LocalDateTime.of(2023, 1, 1, 0, 0, 0), Arrays.asList("dt"));
        assertEquals(
                new LinkedHashMap<String, String>() {
                    {
                        put("dt", "20230101");
                    }
                },
                partitionValues);
    }
}
