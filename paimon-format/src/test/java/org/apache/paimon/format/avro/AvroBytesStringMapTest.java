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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.InternalArray;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroBytesStringMap}. */
public class AvroBytesStringMapTest {

    @Test
    public void testNullableValues() throws Exception {
        List<String> keys = Arrays.asList("apple", "banana", "cherry");
        List<String> values = Arrays.asList("green", null, "red");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        encoder.writeMapStart();
        encoder.setItemCount(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            encoder.startItem();
            encoder.writeString(keys.get(i));
            String value = values.get(i);
            if (value == null) {
                encoder.writeIndex(0);
            } else {
                encoder.writeIndex(1);
                encoder.writeString(value);
            }
        }
        encoder.writeMapEnd();
        encoder.flush();

        checkResult(keys, values, baos.toByteArray(), true);
    }

    @Test
    public void testNotNullValues() throws Exception {
        List<String> keys = Arrays.asList("apple", "banana", "cherry");
        List<String> values = Arrays.asList("green", "yellow", "red");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        encoder.writeMapStart();
        encoder.setItemCount(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            encoder.startItem();
            encoder.writeString(keys.get(i));
            encoder.writeString(values.get(i));
        }
        encoder.writeMapEnd();
        encoder.flush();

        checkResult(keys, values, baos.toByteArray(), false);
    }

    private void checkResult(
            List<String> keys, List<String> values, byte[] bytes, boolean valueNullable)
            throws Exception {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        AvroBytesStringMap map = AvroBytesStringMap.create(decoder, valueNullable);
        assertThat(map.size()).isEqualTo(keys.size());

        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            assertThat(keyArray.getString(i).toString()).isEqualTo(keys.get(i));
            String value = values.get(i);
            if (value == null) {
                assertThat(valueArray.isNullAt(i)).isTrue();
            } else {
                assertThat(valueArray.getString(i).toString()).isEqualTo(value);
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        encoder.writeMapStart();
        encoder.setItemCount(keys.size());
        encoder.writeFixed(map.bytes(), 0, map.lengthInBytes());
        encoder.writeMapEnd();
        encoder.flush();
        assertThat(baos.toByteArray()).isEqualTo(bytes);
    }
}
