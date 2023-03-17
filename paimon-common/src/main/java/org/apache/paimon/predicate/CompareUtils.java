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

import org.apache.paimon.types.DataType;

import static java.lang.Math.min;

/** Utils for comparator. */
public class CompareUtils {
    private CompareUtils() {}

    public static int compareLiteral(DataType type, Object v1, Object v2) {
        if (v1 instanceof Comparable) {
            return ((Comparable<Object>) v1).compareTo(v2);
        } else if (v1 instanceof byte[]) {
            return compare((byte[]) v1, (byte[]) v2);
        } else {
            throw new RuntimeException("Unsupported type: " + type);
        }
    }

    private static int compare(byte[] first, byte[] second) {
        for (int x = 0; x < min(first.length, second.length); x++) {
            int cmp = first[x] - second[x];
            if (cmp != 0) {
                return cmp;
            }
        }
        return first.length - second.length;
    }
}
