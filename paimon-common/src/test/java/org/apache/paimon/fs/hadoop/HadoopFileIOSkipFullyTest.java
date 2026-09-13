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

import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code HadoopSeekableInputStream#skipFully} turns a short forward seek into skips. A stream that
 * returns 0 from {@code skip} used to spin forever; a 0 has to be resolved by reading, because
 * {@link java.io.InputStream#skip} may return it without being at the end.
 */
class HadoopFileIOSkipFullyTest {

    @Test
    void skipFullyThrowsWhenTheStreamReallyEnds() throws Exception {
        FSDataInputStream in = mock(FSDataInputStream.class);
        when(in.skip(anyLong())).thenReturn(0L);
        // the read probe is what distinguishes EOF from a transient zero
        when(in.read()).thenReturn(-1);

        assertThatThrownBy(() -> skipFully(in, 4096L)).hasRootCauseInstanceOf(EOFException.class);
        verify(in).read();
    }

    @Test
    void skipFullyContinuesAfterATransientZero() throws Exception {
        FSDataInputStream in = mock(FSDataInputStream.class);
        // 0 first, then progress: the old loop threw here, and before that it spun
        when(in.skip(anyLong())).thenReturn(0L, 4095L);
        when(in.read()).thenReturn(7);

        assertThatCode(() -> skipFully(in, 4096L)).doesNotThrowAnyException();
        // the probe consumed one byte, so only the remaining 4095 are skipped
        verify(in).read();
    }

    @Test
    void skipFullyIsANoOpForNothingToSkip() throws Exception {
        FSDataInputStream in = mock(FSDataInputStream.class);

        assertThatCode(() -> skipFully(in, 0L)).doesNotThrowAnyException();
        verify(in, never()).skip(anyLong());
    }

    private static void skipFully(FSDataInputStream in, long bytes) throws Exception {
        Class<?> clazz =
                Class.forName("org.apache.paimon.fs.hadoop.HadoopFileIO$HadoopSeekableInputStream");
        Constructor<?> constructor = clazz.getDeclaredConstructor(FSDataInputStream.class);
        constructor.setAccessible(true);
        Object stream = constructor.newInstance(in);
        Method skipFully = clazz.getDeclaredMethod("skipFully", long.class);
        skipFully.setAccessible(true);
        skipFully.invoke(stream, bytes);
    }
}
