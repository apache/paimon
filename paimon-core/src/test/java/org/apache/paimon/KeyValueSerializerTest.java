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

package org.apache.paimon;

import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueSerializer}. */
public class KeyValueSerializerTest extends ObjectSerializerTestBase<KeyValue> {

    private final TestKeyValueGenerator gen = new TestKeyValueGenerator();

    @Override
    protected ObjectSerializer<KeyValue> serializer() {
        return new KeyValueSerializer(
                TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.DEFAULT_ROW_TYPE);
    }

    @Override
    protected KeyValue object() {
        return gen.next();
    }

    @Override
    protected void checkResult(KeyValue expected, KeyValue actual) {
        assertThat(
                        equals(
                                expected,
                                actual,
                                TestKeyValueGenerator.KEY_SERIALIZER,
                                TestKeyValueGenerator.DEFAULT_ROW_SERIALIZER))
                .isTrue();
    }

    public static boolean equals(
            KeyValue kv1,
            KeyValue kv2,
            InternalRowSerializer keySerializer,
            InternalRowSerializer valueSerializer) {
        return kv1.sequenceNumber() == kv2.sequenceNumber()
                && kv1.valueKind() == kv2.valueKind()
                && keySerializer
                        .toBinaryRow(kv1.key())
                        .copy()
                        .equals(keySerializer.toBinaryRow(kv2.key()).copy())
                && valueSerializer
                        .toBinaryRow(kv1.value())
                        .copy()
                        .equals(valueSerializer.toBinaryRow(kv2.value()).copy());
    }
}
