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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.SimpleColStats;

/** Utils for testing with {@link SimpleColStats}. */
public class SimpleColStatsTestUtils {

    public static boolean test(Predicate predicate, long rowCount, SimpleColStats[] fieldStats) {
        Object[] min = new Object[fieldStats.length];
        Object[] max = new Object[fieldStats.length];
        Long[] nullCounts = new Long[fieldStats.length];
        for (int i = 0; i < fieldStats.length; i++) {
            min[i] = fieldStats[i].min();
            max[i] = fieldStats[i].max();
            nullCounts[i] = fieldStats[i].nullCount();
        }

        return predicate.test(
                rowCount,
                GenericRow.of(min),
                GenericRow.of(max),
                BinaryArray.fromLongArray(nullCounts));
    }
}
