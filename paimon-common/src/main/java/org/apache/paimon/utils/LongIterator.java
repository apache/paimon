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

import java.util.NoSuchElementException;

/** Iterator for long. */
public interface LongIterator {

    boolean hasNext();

    long next();

    static LongIterator fromRange(final long startInclusive, final long endExclusive) {
        return new LongIterator() {

            private long i = startInclusive;

            @Override
            public boolean hasNext() {
                return i < endExclusive;
            }

            @Override
            public long next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return i++;
            }
        };
    }

    static LongIterator fromArray(final long[] longs) {
        return new LongIterator() {

            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < longs.length;
            }

            @Override
            public long next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return longs[i++];
            }
        };
    }
}
