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

package org.apache.paimon.sort;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;

/** Sort buffer to sort records. */
public interface SortBuffer {

    int size();

    void clear();

    long getOccupancy();

    /** Flush memory, return false if not supported. */
    boolean flushMemory() throws IOException;

    /** @return false if the buffer is full. */
    boolean write(InternalRow record) throws IOException;

    /** @return iterator with sorting. */
    MutableObjectIterator<BinaryRow> sortedIterator() throws IOException;
}
