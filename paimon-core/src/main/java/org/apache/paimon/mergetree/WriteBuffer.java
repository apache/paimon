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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/**
 * Append only writer buffer for storing key-values. When it is full, it will be flushed to disk and
 * form a data file.
 */
public interface WriteBuffer {

    /**
     * Put a record with sequence number and value kind.
     *
     * @return True, if the record was successfully written, false, if the mem table was full.
     */
    boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException;

    /** Record size of this table. */
    int size();

    /** Memory occupancy size of this table. */
    long memoryOccupancy();

    /** Flush memory, return false if not supported. */
    boolean flushMemory() throws IOException;

    /**
     * Performs the given action for each remaining element in this buffer until all elements have
     * been processed or the action throws an exception.
     *
     * @param rawConsumer consumer to consume records without merging.
     * @param mergedConsumer consumer to consume records after merging.
     */
    void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException;

    /** Removes all records from this table. The table will be empty after this call returns. */
    void clear();

    /** A Consumer that accepts KeyValue and throw exceptions. */
    @FunctionalInterface
    interface KvConsumer {
        void accept(KeyValue kv) throws IOException;
    }
}
