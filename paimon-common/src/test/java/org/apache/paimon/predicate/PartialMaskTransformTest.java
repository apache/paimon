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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartialMaskTransformTest {

    @Test
    void testNull() {
        PartialMaskTransform transform =
                new PartialMaskTransform(
                        new FieldRef(0, "f0", DataTypes.STRING()),
                        2,
                        2,
                        BinaryString.fromString("*"));
        assertThat(transform.transform(GenericRow.of((Object) null))).isNull();
    }

    @Test
    void testNormal() {
        PartialMaskTransform transform =
                new PartialMaskTransform(
                        new FieldRef(0, "f0", DataTypes.STRING()),
                        2,
                        2,
                        BinaryString.fromString("*"));
        Object out = transform.transform(GenericRow.of(BinaryString.fromString("abcdef")));
        assertThat(out).isEqualTo(BinaryString.fromString("ab**ef"));
    }

    @Test
    void testShorterThanPrefixPlusSuffix() {
        PartialMaskTransform transform =
                new PartialMaskTransform(
                        new FieldRef(0, "f0", DataTypes.STRING()),
                        3,
                        3,
                        BinaryString.fromString("*"));
        Object out = transform.transform(GenericRow.of(BinaryString.fromString("abc")));
        assertThat(out).isEqualTo(BinaryString.fromString("***"));
    }

    @Test
    void testMultiCharMaskToken() {
        PartialMaskTransform transform =
                new PartialMaskTransform(
                        new FieldRef(0, "f0", DataTypes.STRING()),
                        1,
                        1,
                        BinaryString.fromString("xx"));
        Object out = transform.transform(GenericRow.of(BinaryString.fromString("abcd")));
        assertThat(out).isEqualTo(BinaryString.fromString("axxxxd"));
    }

    @Test
    void testIllegalArgs() {
        assertThatThrownBy(
                        () ->
                                new PartialMaskTransform(
                                        new FieldRef(0, "f0", DataTypes.STRING()),
                                        -1,
                                        0,
                                        BinaryString.fromString("*")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new PartialMaskTransform(
                                        new FieldRef(0, "f0", DataTypes.STRING()),
                                        0,
                                        0,
                                        BinaryString.fromString("")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
