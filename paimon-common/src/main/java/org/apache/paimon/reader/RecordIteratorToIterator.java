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

package org.apache.paimon.reader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Wrap a {@link RecordReader.RecordIterator} as a Java {@link Iterator}. */
public class RecordIteratorToIterator<T> implements Iterator<T> {

    private final RecordReader.RecordIterator<T> recordIterator;
    private boolean advanced;
    private T currentResult;

    public RecordIteratorToIterator(RecordReader.RecordIterator<T> recordIterator) {
        this.recordIterator = recordIterator;
    }

    @Override
    public boolean hasNext() {
        advanceIfNeeded();
        return currentResult != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        advanced = false;
        return currentResult;
    }

    private void advanceIfNeeded() {
        if (advanced) {
            return;
        }
        advanced = true;
        try {
            currentResult = recordIterator.next();
            if (currentResult == null) {
                recordIterator.releaseBatch();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
