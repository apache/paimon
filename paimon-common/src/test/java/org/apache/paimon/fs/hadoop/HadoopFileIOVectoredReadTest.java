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

package org.apache.paimon.fs.hadoop;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * A positional read is served from Hadoop's {@link org.apache.hadoop.fs.PositionedReadable}, never
 * from {@code seek}, whose skip-read would pull the whole gap off the wire before the bytes that
 * were actually asked for. A behaviour test over a real file cannot tell the two apart, because
 * both return the right bytes.
 */
@Timeout(30)
class HadoopFileIOVectoredReadTest {

    @Test
    void preadUsesThePositionalRead() throws Exception {
        FSDataInputStream in = mock(FSDataInputStream.class);
        byte[] buffer = new byte[16];
        when(in.read(100L, buffer, 0, 16)).thenReturn(16);

        assertThat(newVectoredStream(in).pread(100L, buffer, 0, 16)).isEqualTo(16);

        // no seek, no getPos, no sequential read: verifyNoMoreInteractions covers all three
        verify(in).read(100L, buffer, 0, 16);
        verifyNoMoreInteractions(in);
    }

    @Test
    void onlyABlockFileSystemGetsTheVectoredStream() throws Exception {
        assertThat(openStream("hdfs://ns/file")).isInstanceOf(VectoredReadable.class);
        assertThat(openStream("viewfs://cluster/file")).isInstanceOf(VectoredReadable.class);
        assertThat(openStream("file:/tmp/file")).isInstanceOf(VectoredReadable.class);

        // these emulate a positional read with a seek and a seek back, so they keep the plain
        // stream and the sequential fallback that goes with it
        assertThat(openStream("s3a://bucket/file")).isNotInstanceOf(VectoredReadable.class);
        assertThat(openStream("oss://bucket/file")).isNotInstanceOf(VectoredReadable.class);
        assertThat(openStream("gs://bucket/file")).isNotInstanceOf(VectoredReadable.class);
        assertThat(openStream("cosn://bucket/file")).isNotInstanceOf(VectoredReadable.class);
    }

    private static SeekableInputStream openStream(String uri) throws Exception {
        Path path = new Path(uri);
        FileSystem fs = mock(FileSystem.class);
        when(fs.open(any(org.apache.hadoop.fs.Path.class)))
                .thenReturn(new FSDataInputStream(mock(FSInputStream.class)));

        HadoopFileIO fileIO = new HadoopFileIO(path);
        fileIO.setFileSystem(fs);
        return fileIO.newInputStream(path);
    }

    private static VectoredReadable newVectoredStream(FSDataInputStream in) throws Exception {
        Class<?> clazz =
                Class.forName(
                        "org.apache.paimon.fs.hadoop.HadoopFileIO$VectoredHadoopSeekableInputStream");
        Constructor<?> constructor = clazz.getDeclaredConstructor(FSDataInputStream.class);
        constructor.setAccessible(true);
        return (VectoredReadable) constructor.newInstance(in);
    }
}
