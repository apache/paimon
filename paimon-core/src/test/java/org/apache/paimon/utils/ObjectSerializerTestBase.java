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

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ObjectSerializer}. */
public abstract class ObjectSerializerTestBase<T> {

    // we do not use @RepeatedTests in these tests to test the reuse of serializers
    private static final int TRIES = 100;

    @Test
    public void testToFromRow() {
        ObjectSerializer<T> serializer = serializer();
        for (int i = 0; i < TRIES; i++) {
            T object = object();
            checkResult(object, serializer.fromRow(serializer.toRow(object)));
        }
    }

    @Test
    public void testSerialize() throws IOException {
        ObjectSerializer<T> serializer = serializer();
        for (int i = 0; i < TRIES; i++) {
            T object = object();
            DataOutputSerializer out = new DataOutputSerializer(128);
            serializer.serialize(object, out);
            T actual = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
            checkResult(object, actual);
        }
    }

    @Test
    public void testSerializeList() throws IOException {
        ObjectSerializer<T> serializer = serializer();
        // more records than deserializeList pre-sizes its list for: the bounded capacity must not
        // become a bound on how many records are read. Only the count is checked, because a
        // serializer is free to return a reused object from fromRow.
        List<T> objects = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            objects.add(object());
        }
        assertThat(serializer.deserializeList(serializer.serializeList(objects)))
                .hasSameSizeAs(objects);
    }

    @Test
    public void testDeserializeListWithNegativeCount() {
        ObjectSerializer<T> serializer = serializer();
        byte[] negativeCount = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThatThrownBy(
                        () -> serializer.deserializeList(new DataInputDeserializer(negativeCount)))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Invalid " + serializer.getClass().getSimpleName() + " record count: -1");
    }

    @Test
    public void testDeserializeListWithHugeCount() {
        // Reading is where a count the remaining bytes cannot cover has to fail; pre-sizing the
        // list for Integer.MAX_VALUE records threw OutOfMemoryError before the first read.
        ObjectSerializer<T> serializer = serializer();
        byte[] hugeCount = {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThatThrownBy(() -> serializer.deserializeList(new DataInputDeserializer(hugeCount)))
                .isInstanceOf(EOFException.class);
    }

    protected abstract ObjectSerializer<T> serializer();

    protected abstract T object();

    protected void checkResult(T expected, T actual) {
        assertThat(actual).isEqualTo(expected);
    }
}
