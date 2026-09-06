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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.fs.ByteArraySeekableStream;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VFSInputStream}, in particular read-statistics accounting. */
public class VFSInputStreamTest {

    @Test
    public void testSingleByteReadCountsBytesOnce() throws IOException {
        byte[] content = "hello".getBytes();
        FileSystem.Statistics stats = new FileSystem.Statistics("pvfs");
        VFSInputStream stream = new VFSInputStream(new ByteArraySeekableStream(content), stats);

        byte[] read = new byte[content.length];
        int n = 0;
        int b;
        while ((b = stream.read()) != -1) {
            read[n++] = (byte) b;
        }
        stream.close();

        assertThat(n).isEqualTo(content.length);
        assertThat(read).isEqualTo(content);
        // Each byte must be counted exactly once (2N before the fix).
        assertThat(stats.getBytesRead()).isEqualTo(content.length);
    }

    @Test
    public void testBulkReadCountsBytesOnce() throws IOException {
        byte[] content = "hello".getBytes();
        FileSystem.Statistics stats = new FileSystem.Statistics("pvfs");
        VFSInputStream stream = new VFSInputStream(new ByteArraySeekableStream(content), stats);

        byte[] buffer = new byte[content.length];
        int byteRead = stream.read(buffer, 0, content.length);
        stream.close();

        assertThat(byteRead).isEqualTo(content.length);
        assertThat(buffer).isEqualTo(content);
        assertThat(stats.getBytesRead()).isEqualTo(content.length);
    }

    @Test
    public void testReadAtEofDoesNotCount() throws IOException {
        FileSystem.Statistics stats = new FileSystem.Statistics("pvfs");
        VFSInputStream stream =
                new VFSInputStream(new ByteArraySeekableStream("".getBytes()), stats);

        assertThat(stream.read()).isEqualTo(-1);
        stream.close();

        assertThat(stats.getBytesRead()).isEqualTo(0L);
    }
}
