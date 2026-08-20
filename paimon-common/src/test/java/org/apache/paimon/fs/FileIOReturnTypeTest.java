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

package org.apache.paimon.fs;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests default return values and adapters in the public file I/O API. */
public class FileIOReturnTypeTest {

    @Test
    public void testFileStatusDefaultValues() {
        FileStatus status =
                new FileStatus() {
                    @Override
                    public long getLen() {
                        return 0;
                    }

                    @Override
                    public boolean isDir() {
                        return false;
                    }

                    @Override
                    public Path getPath() {
                        return new Path("file:/status");
                    }

                    @Override
                    public long getModificationTime() {
                        return 0;
                    }
                };

        assertThat(status.getAccessTime()).isZero();
        assertThat(status.getOwner()).isNull();
    }

    @Test
    public void testSeekableInputStreamWrapForwardsReadAndClose() throws IOException {
        InputStream input = mock(InputStream.class);
        byte[] buffer = new byte[4];
        when(input.read()).thenReturn(17);
        when(input.read(buffer, 1, 2)).thenReturn(2);

        SeekableInputStream wrapped = SeekableInputStream.wrap(input);
        assertThat(wrapped.read()).isEqualTo(17);
        assertThat(wrapped.read(buffer, 1, 2)).isEqualTo(2);
        wrapped.close();

        verify(input).read();
        verify(input).read(same(buffer), eq(1), eq(2));
        verify(input).close();
    }

    @Test
    public void testSeekableInputStreamWrapRejectsSeek() {
        SeekableInputStream wrapped =
                SeekableInputStream.wrap(new ByteArrayInputStream(new byte[0]));

        assertThatThrownBy(() -> wrapped.seek(0)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testSeekableInputStreamWrapRejectsGetPos() {
        SeekableInputStream wrapped =
                SeekableInputStream.wrap(new ByteArrayInputStream(new byte[0]));

        assertThatThrownBy(wrapped::getPos).isInstanceOf(UnsupportedOperationException.class);
    }
}
