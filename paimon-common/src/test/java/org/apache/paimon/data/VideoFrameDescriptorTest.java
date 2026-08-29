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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VideoFrameDescriptor}. */
public class VideoFrameDescriptorTest {

    @Test
    public void testRoundTripAndPayloadIdentity() {
        VideoFrameDescriptor frame =
                new VideoFrameDescriptor("oss://bucket/source.mp4", 17, 103, 42);

        assertThat(VideoFrameDescriptor.isVideoFrameDescriptor(frame.serialize())).isTrue();
        assertThat(BlobDescriptor.isBlobDescriptor(frame.serialize())).isFalse();
        assertThat(BlobDescriptor.deserialize(frame.serialize())).isEqualTo(frame);
        assertThat(VideoFrameDescriptor.deserialize(frame.serialize())).isEqualTo(frame);
        assertThat(frame.payloadDescriptor())
                .isEqualTo(new BlobDescriptor("oss://bucket/source.mp4", 17, 103));

        VideoFrameDescriptor next =
                new VideoFrameDescriptor("oss://bucket/source.mp4", 17, 103, 43);
        assertThat(next).isNotEqualTo(frame);
        assertThat(next.payloadDescriptor()).isEqualTo(frame.payloadDescriptor());
    }

    @Test
    public void testBlobFromBytesPreservesFrameDescriptor() {
        VideoFrameDescriptor expected = new VideoFrameDescriptor("file:/video.mp4", 0, 9, 7);
        Blob blob = Blob.fromBytes(expected.serialize(), null, null);

        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(blob.toDescriptor()).isEqualTo(expected);
    }

    @Test
    public void testRejectInvalidPayload() {
        VideoFrameDescriptor descriptor = new VideoFrameDescriptor("file:/video.mp4", 0, 9, 7);
        byte[] trailing = Arrays.copyOf(descriptor.serialize(), descriptor.serialize().length + 1);

        assertThatThrownBy(() -> VideoFrameDescriptor.deserialize(trailing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing bytes");
        assertThatThrownBy(() -> new VideoFrameDescriptor("file:/video.mp4", 0, 9, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }
}
