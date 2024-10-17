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

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Encapsulates a logic of serialization and deserialization of a list with a delimiter. Used in the
 * savepoint format.
 */
public final class ListDelimitedSerializer {

    private static final byte DELIMITER = ',';

    private final DataInputDeserializer dataInputView = new DataInputDeserializer();
    private final DataOutputSerializer dataOutputView = new DataOutputSerializer(128);

    public synchronized <T> List<T> deserializeList(
            byte[] valueBytes, Serializer<T> elementSerializer) {
        if (valueBytes == null) {
            return null;
        }

        dataInputView.setBuffer(valueBytes);

        List<T> result = new ArrayList<>();
        T next;
        while ((next = deserializeNextElement(dataInputView, elementSerializer)) != null) {
            result.add(next);
        }
        return result;
    }

    public synchronized <T> byte[] serializeList(List<T> valueList, Serializer<T> elementSerializer)
            throws IOException {

        dataOutputView.clear();
        boolean first = true;

        for (T value : valueList) {
            checkNotNull(value, "You cannot add null to a value list.");

            if (first) {
                first = false;
            } else {
                dataOutputView.write(DELIMITER);
            }
            elementSerializer.serialize(value, dataOutputView);
        }

        return dataOutputView.getCopyOfBuffer();
    }

    public synchronized byte[] serializeList(List<byte[]> valueList) throws IOException {

        dataOutputView.clear();
        boolean first = true;

        for (byte[] value : valueList) {
            checkNotNull(value, "You cannot add null to a value list.");

            if (first) {
                first = false;
            } else {
                dataOutputView.write(DELIMITER);
            }
            dataOutputView.write(value);
        }

        return dataOutputView.getCopyOfBuffer();
    }

    /** Deserializes a single element from a serialized list. */
    public static <T> T deserializeNextElement(
            DataInputDeserializer in, Serializer<T> elementSerializer) {
        try {
            if (in.available() > 0) {
                T element = elementSerializer.deserialize(in);
                if (in.available() > 0) {
                    in.readByte();
                }
                return element;
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected list element deserialization failure", e);
        }
        return null;
    }
}
