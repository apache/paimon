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

import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BlobViewStruct}. */
public class BlobViewStructTest {

    @Test
    public void testSerializeAndDeserialize() {
        BlobViewStruct viewStruct =
                new BlobViewStruct(Identifier.fromString("default.source"), 7, 5L);

        BlobViewStruct deserialized = BlobViewStruct.deserialize(viewStruct.serialize());

        assertThat(deserialized.identifier()).isEqualTo(Identifier.fromString("default.source"));
        assertThat(deserialized.fieldId()).isEqualTo(7);
        assertThat(deserialized.rowId()).isEqualTo(5L);
    }

    @Test
    public void testRejectUnexpectedVersion() {
        BlobViewStruct viewStruct =
                new BlobViewStruct(Identifier.fromString("default.source"), 7, 5L);
        byte[] bytes = viewStruct.serialize();
        bytes[0] = 3;

        assertThatThrownBy(() -> BlobViewStruct.deserialize(bytes))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Expecting BlobViewStruct version to be 1");
    }

    @Test
    public void testEquality() {
        BlobViewStruct a = new BlobViewStruct(Identifier.fromString("default.source"), 7, 5L);
        BlobViewStruct b = new BlobViewStruct(Identifier.fromString("default.source"), 7, 5L);
        BlobViewStruct c = new BlobViewStruct(Identifier.fromString("default.source"), 8, 5L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    public void testDecodeBlobView() {
        BlobViewStruct viewStruct =
                new BlobViewStruct(Identifier.fromString("default.source"), 7, 5L);
        byte[] bytes = viewStruct.serialize();

        assertThat(BlobViewStruct.isBlobViewStruct(bytes)).isTrue();
        assertThat(BlobViewStruct.isBlobViewStruct(null)).isFalse();
        assertThat(BlobViewStruct.isBlobViewStruct(new byte[] {1, 2, 3})).isFalse();
        assertThat(Blob.fromBytes(bytes, null, null)).isEqualTo(Blob.fromView(viewStruct));
    }
}
