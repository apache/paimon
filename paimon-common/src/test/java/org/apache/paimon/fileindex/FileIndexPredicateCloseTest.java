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

package org.apache.paimon.fileindex;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link FileIndexPredicate} always releases the stream it is handed. */
public class FileIndexPredicateCloseTest {

    private static final RowType ROW_TYPE = RowType.of(DataTypes.INT());

    /**
     * The reader rejects a file whose magic does not match, and at that point nothing else holds a
     * reference to the stream, so the constructor has to release it itself.
     */
    @Test
    public void testFailedConstructionReleasesTheStream() {
        AtomicInteger closed = new AtomicInteger();
        byte[] notAnIndexFile = new byte[64];

        assertThatThrownBy(() -> new FileIndexPredicate(tracking(notAnIndexFile, closed), ROW_TYPE))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("not file index file");

        assertThat(closed).hasValue(1);
    }

    private static SeekableInputStream tracking(byte[] bytes, AtomicInteger closed) {
        ByteArraySeekableStream delegate = new ByteArraySeekableStream(bytes);
        return new SeekableInputStream() {

            @Override
            public void seek(long desired) throws IOException {
                delegate.seek(desired);
            }

            @Override
            public long getPos() throws IOException {
                return delegate.getPos();
            }

            @Override
            public int read() throws IOException {
                return delegate.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return delegate.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
                closed.incrementAndGet();
                delegate.close();
            }
        };
    }
}
