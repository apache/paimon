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

package org.apache.paimon.globalindex.fmindex;

/** Immutable linear-time byte pattern used by the exact dense-occurrence fallback. */
final class FMBytePattern {

    private final byte[] needle;
    private final int[] failure;

    FMBytePattern(byte[] needle) {
        this.needle = needle;
        this.failure = new int[needle.length];
        int matched = 0;
        for (int i = 1; i < needle.length; i++) {
            while (matched > 0 && needle[i] != needle[matched]) {
                matched = failure[matched - 1];
            }
            if (needle[i] == needle[matched]) {
                matched++;
            }
            failure[i] = matched;
        }
    }

    boolean contains(byte[] value, int offset, int length) {
        if (needle.length == 0) {
            return true;
        }
        int matched = 0;
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            while (matched > 0 && value[i] != needle[matched]) {
                matched = failure[matched - 1];
            }
            if (value[i] == needle[matched]) {
                matched++;
                if (matched == needle.length) {
                    return true;
                }
            }
        }
        return false;
    }
}
