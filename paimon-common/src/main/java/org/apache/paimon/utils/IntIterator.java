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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Iterator for ints. */
public interface IntIterator extends Closeable {

    int next() throws IOException;

    static int[] toInts(IntIterator input) {
        return toIntList(input).stream().mapToInt(Integer::intValue).toArray();
    }

    static List<Integer> toIntList(IntIterator input) {
        List<Integer> ints = new ArrayList<>();
        try (IntIterator iterator = input) {
            while (true) {
                try {
                    ints.add(iterator.next());
                } catch (EOFException ignored) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ints;
    }

    static IntIterator create(int[] ints) {
        return new IntIterator() {

            int pos = -1;

            @Override
            public int next() throws EOFException {
                if (pos >= ints.length - 1) {
                    throw new EOFException();
                }
                return ints[++pos];
            }

            @Override
            public void close() {}
        };
    }
}
