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

class HashTransformTest {

    @Test
    void testNull() {
        HashTransform transform = new HashTransform(new FieldRef(0, "f0", DataTypes.STRING()));
        assertThat(transform.transform(GenericRow.of((Object) null))).isNull();
    }

    @Test
    void testSha256HexLower() {
        HashTransform transform =
                new HashTransform(new FieldRef(0, "f0", DataTypes.STRING()), "sha256", null);
        Object out = transform.transform(GenericRow.of(BinaryString.fromString("abcdef")));
        assertThat(out)
                .isEqualTo(
                        BinaryString.fromString(
                                "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721"));
    }

    @Test
    void testSha256WithSalt() {
        HashTransform transform =
                new HashTransform(
                        new FieldRef(0, "f0", DataTypes.STRING()),
                        "SHA-256",
                        BinaryString.fromString("SALT_"));
        Object out = transform.transform(GenericRow.of(BinaryString.fromString("abcdef")));
        // sha256("SALT_" + "abcdef")
        assertThat(out)
                .isEqualTo(
                        BinaryString.fromString(
                                "1aff6a6e4dc5a1bcf81b101216ae7cb85b32ec8c56e926be3e6d3ae211caf522"));
    }

    @Test
    void testIllegalAlgorithm() {
        assertThatThrownBy(
                        () ->
                                new HashTransform(
                                        new FieldRef(0, "f0", DataTypes.STRING()),
                                        "NO_SUCH_ALGO",
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
