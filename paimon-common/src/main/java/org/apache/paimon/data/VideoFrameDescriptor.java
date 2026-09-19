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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A descriptor for one logical frame in a descriptor-backed encoded video payload. */
@Public
public class VideoFrameDescriptor extends BlobDescriptor {

    private static final long serialVersionUID = 1L;
    private static final long MAGIC = 0x564944454F46524DL; // "VIDEOFRM"
    private static final byte CURRENT_VERSION = 2;
    private static final int V1_FIXED_LENGTH =
            Byte.BYTES + Long.BYTES + Integer.BYTES + 3 * Long.BYTES;
    private static final int V2_FIXED_LENGTH = V1_FIXED_LENGTH + 2 * Long.BYTES;

    private final long frameIndex;
    private final long keyframeIndexOffset;
    private final long keyframeIndexLength;

    public VideoFrameDescriptor(String uri, long offset, long length, long frameIndex) {
        this(uri, offset, length, frameIndex, -1, 0);
    }

    public VideoFrameDescriptor(
            String uri,
            long offset,
            long length,
            long frameIndex,
            long keyframeIndexOffset,
            long keyframeIndexLength) {
        super(uri, offset, length);
        checkArgument(
                frameIndex >= 0, "Video frame index must be non-negative, but was %s.", frameIndex);
        checkArgument(
                keyframeIndexLength >= 0, "Video keyframe index length must be non-negative.");
        checkArgument(
                (keyframeIndexLength == 0 && keyframeIndexOffset == -1)
                        || (keyframeIndexLength > 0 && keyframeIndexOffset >= 0),
                "Invalid video keyframe index range.");
        this.frameIndex = frameIndex;
        this.keyframeIndexOffset = keyframeIndexOffset;
        this.keyframeIndexLength = keyframeIndexLength;
    }

    public long frameIndex() {
        return frameIndex;
    }

    /** Returns the physical video identity without the logical frame locator. */
    public BlobDescriptor payloadDescriptor() {
        return new BlobDescriptor(uri(), offset(), length());
    }

    public @Nullable BlobDescriptor keyframeIndexDescriptor() {
        return keyframeIndexLength == 0
                ? null
                : new BlobDescriptor(uri(), keyframeIndexOffset, keyframeIndexLength);
    }

    /** Returns the persisted keyframe index carried by an exact frame reference. */
    public static @Nullable Blob keyframeIndexBlob(Blob blob) {
        VideoFrameDescriptor frame = fromBlob(blob);
        BlobDescriptor mapping = frame == null ? null : frame.keyframeIndexDescriptor();
        if (mapping == null) {
            return null;
        }
        return Blob.fromDescriptor(((BlobRef) blob).uriReader(), mapping);
    }

    /** Returns the video frame carried by an exact lazy blob reference, or {@code null}. */
    public static @Nullable VideoFrameDescriptor fromBlob(@Nullable Blob blob) {
        if (blob == null || blob.getClass() != BlobRef.class) {
            return null;
        }
        BlobDescriptor descriptor = blob.toDescriptor();
        return descriptor instanceof VideoFrameDescriptor
                ? (VideoFrameDescriptor) descriptor
                : null;
    }

    /** Returns the physical video identity carried by a frame blob, or {@code null}. */
    public static @Nullable BlobDescriptor payloadDescriptor(@Nullable Blob blob) {
        VideoFrameDescriptor frame = fromBlob(blob);
        return frame == null ? null : frame.payloadDescriptor();
    }

    @Override
    public byte[] serialize() {
        byte version = keyframeIndexLength == 0 ? 1 : CURRENT_VERSION;
        byte[] uriBytes = uri().getBytes(StandardCharsets.UTF_8);
        int fixedLength = version == 1 ? V1_FIXED_LENGTH : V2_FIXED_LENGTH;
        ByteBuffer buffer =
                ByteBuffer.allocate(fixedLength + uriBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(version);
        buffer.putLong(MAGIC);
        buffer.putInt(uriBytes.length);
        buffer.put(uriBytes);
        buffer.putLong(offset());
        buffer.putLong(length());
        buffer.putLong(frameIndex);
        if (version >= 2) {
            buffer.putLong(keyframeIndexOffset);
            buffer.putLong(keyframeIndexLength);
        }
        return buffer.array();
    }

    public static VideoFrameDescriptor deserialize(byte[] bytes) {
        if (bytes == null || bytes.length < V1_FIXED_LENGTH) {
            throw invalidPayload("too short");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        if (version < 1 || version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting VideoFrameDescriptor version in [1, "
                            + CURRENT_VERSION
                            + "], but found "
                            + version
                            + ".");
        }
        long magic = buffer.getLong();
        if (magic != MAGIC) {
            throw invalidPayload("missing magic header");
        }
        int uriLength = buffer.getInt();
        // checked by comparison and subtraction: uriLength + 3 * Long.BYTES wraps negative
        // for a uriLength near Integer.MAX_VALUE
        if (uriLength < 0) {
            throw invalidPayload("negative URI length: " + uriLength);
        }
        if (uriLength > buffer.remaining()) {
            throw invalidPayload("URI length exceeds data size");
        }
        int trailingLength = version == 1 ? 3 * Long.BYTES : 5 * Long.BYTES;
        if (buffer.remaining() - uriLength < trailingLength) {
            throw invalidPayload("missing offset/length/frame index");
        }

        byte[] uriBytes = new byte[uriLength];
        buffer.get(uriBytes);
        String uri = new String(uriBytes, StandardCharsets.UTF_8);
        long offset = buffer.getLong();
        long length = buffer.getLong();
        long frameIndex = buffer.getLong();
        long keyframeIndexOffset = -1;
        long keyframeIndexLength = 0;
        if (version >= 2) {
            keyframeIndexOffset = buffer.getLong();
            keyframeIndexLength = buffer.getLong();
        }
        if (buffer.hasRemaining()) {
            throw invalidPayload("trailing bytes");
        }
        if (frameIndex < 0) {
            throw invalidPayload("negative frame index: " + frameIndex);
        }
        return new VideoFrameDescriptor(
                uri, offset, length, frameIndex, keyframeIndexOffset, keyframeIndexLength);
    }

    public static boolean isVideoFrameDescriptor(byte[] bytes) {
        if (bytes == null || bytes.length < Byte.BYTES + Long.BYTES) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        return version >= 1 && version <= CURRENT_VERSION && buffer.getLong() == MAGIC;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VideoFrameDescriptor)) {
            return false;
        }
        VideoFrameDescriptor that = (VideoFrameDescriptor) o;
        return frameIndex == that.frameIndex
                && payloadDescriptor().equals(that.payloadDescriptor())
                && Objects.equals(keyframeIndexDescriptor(), that.keyframeIndexDescriptor());
    }

    @Override
    public int hashCode() {
        return Objects.hash(payloadDescriptor(), frameIndex, keyframeIndexDescriptor());
    }

    @Override
    public String toString() {
        return "VideoFrameDescriptor{"
                + "payload="
                + payloadDescriptor()
                + ", frameIndex="
                + frameIndex
                + '}';
    }

    private static IllegalArgumentException invalidPayload(String message) {
        return new IllegalArgumentException("Invalid VideoFrameDescriptor data: " + message);
    }
}
