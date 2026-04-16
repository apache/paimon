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

package org.apache.paimon.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BlobReference}. */
public class BlobReferenceTest {

    @Test
    public void testSerializeAndDeserialize() {
        BlobReference reference = new BlobReference("default.source", 7, 5L);

        BlobReference deserialized = BlobReference.deserialize(reference.serialize());

        assertThat(deserialized.tableName()).isEqualTo("default.source");
        assertThat(deserialized.fieldId()).isEqualTo(7);
        assertThat(deserialized.rowId()).isEqualTo(5L);
    }

    @Test
    public void testRejectUnexpectedVersion() {
        BlobReference reference = new BlobReference("default.source", 7, 5L);
        byte[] bytes = reference.serialize();
        bytes[0] = 3;

        assertThatThrownBy(() -> BlobReference.deserialize(bytes))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Expecting BlobReference version to be 1");
    }

    @Test
    public void testEquality() {
        BlobReference a = new BlobReference("default.source", 7, 5L);
        BlobReference b = new BlobReference("default.source", 7, 5L);
        BlobReference c = new BlobReference("default.source", 8, 5L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    public void testIsBlobReference() {
        BlobReference reference = new BlobReference("default.source", 7, 5L);
        byte[] bytes = reference.serialize();

        assertThat(BlobReference.isBlobReference(bytes)).isTrue();
        assertThat(BlobReference.isBlobReference(null)).isFalse();
        assertThat(BlobReference.isBlobReference(new byte[] {1, 2, 3})).isFalse();
    }
}
