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

package org.apache.paimon.benchmark.lookup;

/** Abstract benchmark class for lookup. */
abstract class AbstractLookupBenchmark {
    protected static final int[] VALUE_LENGTHS = {0, 500, 1000, 2000, 4000};

    protected byte[][] generateSequenceInputs(int start, int end) {
        int count = end - start;
        byte[][] result = new byte[count][4];
        for (int i = 0; i < count; i++) {
            result[i] = intToByteArray(i);
        }
        return result;
    }

    protected byte[] intToByteArray(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }
}
