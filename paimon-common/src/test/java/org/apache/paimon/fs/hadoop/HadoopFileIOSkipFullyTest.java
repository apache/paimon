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
import org.junit.jupiter.api.Timeout;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@code HadoopSeekableInputStream#skipFully} fails fast at end of stream instead of
 * spinning on a zero-byte skip.
 */
class HadoopFileIOSkipFullyTest {

    @Test
    @Timeout(10)
    void skipFullyFailsFastAtEndOfStream() throws Exception {
        FSDataInputStream in = mock(FSDataInputStream.class);
        // a blocking stream at EOF keeps returning 0 from skip; the mock rethrows after a
        // few zero-skips so the unfixed loop surfaces as the wrong exception instead of
        // spinning forever
        when(in.skip(anyLong()))
                .thenReturn(0L, 0L, 0L)
                .thenThrow(new IOException("mock exhausted"));

        Class<?> clazz =
                Class.forName("org.apache.paimon.fs.hadoop.HadoopFileIO$HadoopSeekableInputStream");
        Constructor<?> constructor = clazz.getDeclaredConstructor(FSDataInputStream.class);
        constructor.setAccessible(true);
        Object stream = constructor.newInstance(in);
        Method skipFully = clazz.getDeclaredMethod("skipFully", long.class);
        skipFully.setAccessible(true);

        assertThatThrownBy(() -> skipFully.invoke(stream, 4096L))
                .hasRootCauseInstanceOf(EOFException.class)
                .hasRootCauseMessage("Unexpected end of stream while skipping 4096 bytes.");
    }
}
