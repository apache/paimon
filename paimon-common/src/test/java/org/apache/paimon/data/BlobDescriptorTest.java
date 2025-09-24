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

/** Test for {@link BlobDescriptor}. */
public class BlobDescriptorTest {

    @Test
    public void testEquals() {
        String uri1 = "/test/path1";
        String uri2 = "/test/path2";

        BlobDescriptor descriptor1 = new BlobDescriptor(uri1, 100L, 200L);
        BlobDescriptor descriptor2 = new BlobDescriptor(uri1, 100L, 200L);
        BlobDescriptor descriptor3 = new BlobDescriptor(uri2, 100L, 200L);
        BlobDescriptor descriptor4 = new BlobDescriptor(uri1, 150L, 200L);
        BlobDescriptor descriptor5 = new BlobDescriptor(uri1, 100L, 250L);

        assertThat(descriptor1).isEqualTo(descriptor2);
        assertThat(descriptor1).isNotEqualTo(descriptor3);
        assertThat(descriptor1).isNotEqualTo(descriptor4);
        assertThat(descriptor1).isNotEqualTo(descriptor5);
        assertThat(descriptor1).isNotEqualTo(null);
        assertThat(descriptor1).isNotEqualTo(new Object());
    }

    @Test
    public void testHashCode() {
        String uri = "/test/path";

        BlobDescriptor descriptor1 = new BlobDescriptor(uri, 100L, 200L);
        BlobDescriptor descriptor2 = new BlobDescriptor(uri, 100L, 200L);

        assertThat(descriptor1.hashCode()).isEqualTo(descriptor2.hashCode());
    }

    @Test
    public void testToString() {
        String uri = "/test/path";
        BlobDescriptor descriptor = new BlobDescriptor(uri, 100L, 200L);

        String toString = descriptor.toString();
        assertThat(toString).contains("uri='/test/path'");
        assertThat(toString).contains("offset=100");
        assertThat(toString).contains("length=200");
    }

    @Test
    public void testSerializeAndDeserialize() {
        String uri = "/test/path";
        long offset = 100L;
        long length = 200L;

        BlobDescriptor original = new BlobDescriptor(uri, offset, length);
        byte[] serialized = original.serialize();
        BlobDescriptor deserialized = BlobDescriptor.deserialize(serialized);

        assertThat(deserialized.uri()).isEqualTo(original.uri());
        assertThat(deserialized.offset()).isEqualTo(original.offset());
        assertThat(deserialized.length()).isEqualTo(original.length());
        assertThat(deserialized).isEqualTo(original);
    }
}
