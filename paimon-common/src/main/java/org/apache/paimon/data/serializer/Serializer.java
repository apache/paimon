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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/** Serializer to serialize internal data structure. */
public interface Serializer<T> extends Serializable {

    /**
     * Creates a deep copy of this serializer if it is necessary, i.e. if it is stateful. This can
     * return itself if the serializer is not stateful.
     *
     * <p>We need this because Serializers might be used in several threads. Stateless serializers
     * are inherently thread-safe while stateful serializers might not be thread-safe.
     */
    Serializer<T> duplicate();

    /**
     * Creates a deep copy of the given element in a new element.
     *
     * @param from The element reuse be copied.
     * @return A deep copy of the element.
     */
    T copy(T from);

    /**
     * Serializes the given record to the given target output view.
     *
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically
     *     raised by the output view, which may have an underlying I/O channel to which it
     *     delegates.
     */
    void serialize(T record, DataOutputView target) throws IOException;

    /**
     * De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *     Typically raised by the input view, which may have an underlying I/O channel from which
     *     it reads.
     */
    T deserialize(DataInputView source) throws IOException;

    /**
     * Serializes the given record to string.
     *
     * @param record The record to serialize.
     * @return The serialized element.
     */
    default String serializeToString(T record) {
        throw new UnsupportedOperationException(
                String.format("serialize %s to string is unsupported", record));
    }

    /**
     * De-serializes a record from string.
     *
     * @param s The string to de-serialize.
     * @return The deserialized element.
     */
    default T deserializeFromString(String s) {
        throw new UnsupportedOperationException(
                String.format("deserialize %s from string is unsupported", s));
    }
}
