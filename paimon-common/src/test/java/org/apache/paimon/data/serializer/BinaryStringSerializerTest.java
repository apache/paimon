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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.List;

/** Test for {@link BinaryStringSerializer}. */
public class BinaryStringSerializerTest extends SerializerTestBase<BinaryString> {

    @Override
    protected Serializer<BinaryString> createSerializer() {
        return BinaryStringSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(BinaryString t1, BinaryString t2) {
        return t1.equals(t2);
    }

    @Override
    protected BinaryString[] getTestData() {
        return Arrays.stream(
                        new String[] {
                            "a", "", "bcd", "jbmbmner8 jhk hj \n \t üäßß@µ", "", "non-empty"
                        })
                .map(BinaryString::fromString)
                .toArray(BinaryString[]::new);
    }

    @Override
    protected List<Pair<BinaryString, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(BinaryString.fromString("a"), "a"),
                Pair.of(BinaryString.fromString(""), ""),
                Pair.of(
                        BinaryString.fromString("jbmbmner8 jhk hj \n \t üäßß@µ"),
                        "jbmbmner8 jhk hj \n \t üäßß@µ"),
                Pair.of(BinaryString.fromString("non-empty"), "non-empty"));
    }
}
