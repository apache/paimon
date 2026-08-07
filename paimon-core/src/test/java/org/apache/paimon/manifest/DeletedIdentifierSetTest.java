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

package org.apache.paimon.manifest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DeletedIdentifierSet}. */
class DeletedIdentifierSetTest {

    @Test
    void testDeduplicatesAndIncludesPartition() {
        DeletedIdentifierSet identifiers = new DeletedIdentifierSet();
        byte[] identifier = {1, 2, 3, 4};

        identifiers.add(3, identifier, identifier.length);
        identifiers.add(3, identifier, identifier.length);

        assertThat(identifiers.size()).isOne();
        assertThat(identifiers.retainedIdentifierBytes()).isEqualTo(identifier.length);
        assertThat(identifiers.contains(3, identifier, identifier.length)).isTrue();
        assertThat(identifiers.contains(4, identifier, identifier.length)).isFalse();

        identifiers.add(4, identifier, identifier.length);
        assertThat(identifiers.size()).isEqualTo(2);
        assertThat(identifiers.retainedIdentifierBytes()).isEqualTo(identifier.length * 2);
    }

    @Test
    void testCopiesOnlyIdentifierPrefix() {
        DeletedIdentifierSet identifiers = new DeletedIdentifierSet();
        byte[] identifier = {1, 2, 3};

        identifiers.add(0, identifier, 2);
        identifier[0] = 9;

        assertThat(identifiers.contains(0, new byte[] {1, 2, 8}, 2)).isTrue();
        assertThat(identifiers.contains(0, identifier, 2)).isFalse();
        assertThat(identifiers.retainedIdentifierBytes()).isEqualTo(2);
    }

    @Test
    void testGrowsAndReleases() {
        DeletedIdentifierSet identifiers = new DeletedIdentifierSet();
        for (int i = 0; i < 1_000; i++) {
            byte[] identifier = {(byte) i, (byte) (i >>> 8)};
            identifiers.add(i % 7, identifier, identifier.length);
        }

        assertThat(identifiers.size()).isEqualTo(1_000);
        for (int i = 0; i < 1_000; i++) {
            byte[] identifier = {(byte) i, (byte) (i >>> 8)};
            assertThat(identifiers.contains(i % 7, identifier, identifier.length)).isTrue();
        }

        identifiers.release();
        assertThat(identifiers.isEmpty()).isTrue();
        assertThat(identifiers.retainedIdentifierBytes()).isZero();

        identifiers.add(1, new byte[] {1}, 1);
        assertThat(identifiers.contains(1, new byte[] {1}, 1)).isTrue();
    }

    @Test
    void testRejectsInvalidIdentifier() {
        DeletedIdentifierSet identifiers = new DeletedIdentifierSet();

        assertThatThrownBy(() -> identifiers.add(0, (byte[]) null, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> identifiers.add(0, new byte[1], -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> identifiers.contains(0, new byte[1], 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> identifiers.add(0, (BinaryManifestEntry.ReusableIdentifier) null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
