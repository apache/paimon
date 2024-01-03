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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.List;

/** Test for {@link DecimalSerializer}. */
public class DecimalSerializerTest extends SerializerTestBase<Decimal> {

    @Override
    protected DecimalSerializer createSerializer() {
        return new DecimalSerializer(5, 2);
    }

    @Override
    protected boolean deepEquals(Decimal t1, Decimal t2) {
        return t1.equals(t2);
    }

    @Override
    protected Decimal[] getTestData() {
        return new Decimal[] {
            Decimal.fromUnscaledLong(1, 5, 2),
            Decimal.fromUnscaledLong(2, 5, 2),
            Decimal.fromUnscaledLong(3, 5, 2),
            Decimal.fromUnscaledLong(4, 5, 2)
        };
    }

    @Override
    protected List<Pair<Decimal, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(Decimal.fromUnscaledLong(1, 5, 2), "0.01"),
                Pair.of(Decimal.fromUnscaledLong(2, 5, 2), "0.02"),
                Pair.of(Decimal.fromUnscaledLong(3, 5, 2), "0.03"),
                Pair.of(Decimal.fromUnscaledLong(4, 5, 2), "0.04"));
    }
}
