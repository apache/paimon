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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests reading a BLOB whose inline payload exceeds {@link Integer#MAX_VALUE} bytes. */
class BlobInlineReadOverflowTest {

    private static BlobElementSerializer.Reader inlineReader(byte[] onDisk) {
        return new RawBlobElementSerializer()
                .createReader(
                        LocalFileIO.create(),
                        new Path("/tmp/does-not-exist.blob"),
                        new ByteArraySeekableStream(onDisk),
                        false);
    }

    @Test
    void testOversizedInlineBlobThrowsInsteadOfTruncating() {
        long declaredLength = (1L << 32) + 16;

        BlobElementSerializer.Reader reader = inlineReader(new byte[16]);

        assertThatThrownBy(() -> reader.read(0, declaredLength))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Inline BLOB is too large")
                .hasMessageContaining("blob-as-descriptor");
    }

    @Test
    void testInlineBlobWithinLimitReadsFully() {
        byte[] onDisk = new byte[16];
        for (int i = 0; i < onDisk.length; i++) {
            onDisk[i] = (byte) i;
        }

        BlobElementSerializer.Reader reader = inlineReader(onDisk);

        Blob blob = (Blob) reader.read(0, onDisk.length);
        assertThat(blob.toData()).isEqualTo(onDisk);
    }
}
