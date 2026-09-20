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

package org.apache.paimon.globalindex.io;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexLookupDeclinedException;
import org.apache.paimon.globalindex.GlobalIndexQueryContext;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BudgetedGlobalIndexFileReader}. */
public class BudgetedGlobalIndexFileReaderTest {

    private static final byte[] DATA = new byte[] {1, 2, 3, 4, 5};
    private static final GlobalIndexIOMeta META =
            new GlobalIndexIOMeta(new Path("file:///index"), DATA.length, null);

    @Test
    public void testSequentialAndPositionalReadsShareBudget() throws Exception {
        GlobalIndexQueryContext context =
                new GlobalIndexQueryContext(Long.MAX_VALUE, Long.MAX_VALUE, 4, 4);
        GlobalIndexFileReader delegate = ignored -> new ByteArraySeekableStream(DATA);
        SeekableInputStream input =
                new BudgetedGlobalIndexFileReader(delegate, context).getInputStream(META);

        assertThat(input.read(new byte[3])).isEqualTo(3);
        assertThat(input.getPos()).isEqualTo(3);
        byte[] positioned = new byte[1];
        assertThat(((VectoredReadable) input).pread(1, positioned, 0, 1)).isEqualTo(1);
        assertThat(positioned).containsExactly(2);
        assertThat(input.getPos()).isEqualTo(3);
        assertThat(context.readBytes()).isEqualTo(4);
        assertThatThrownBy(input::read).isInstanceOf(GlobalIndexLookupDeclinedException.class);
        input.close();
    }

    @Test
    public void testVectoredReadsReserveBeforeDelegating() throws Exception {
        GlobalIndexQueryContext context =
                new GlobalIndexQueryContext(Long.MAX_VALUE, Long.MAX_VALUE, 2, 2);
        GlobalIndexFileReader delegate = ignored -> new VectoredByteArrayInput(DATA);
        SeekableInputStream input =
                new BudgetedGlobalIndexFileReader(delegate, context).getInputStream(META);
        FileRange range = FileRange.createFileRange(2, 2);

        ((VectoredReadable) input).readVectored(Collections.singletonList(range));

        assertThat(range.getData().join()).containsExactly(3, 4);
        assertThat(context.readBytes()).isEqualTo(2);
        assertThatThrownBy(
                        () ->
                                ((VectoredReadable) input)
                                        .readVectored(
                                                Collections.singletonList(
                                                        FileRange.createFileRange(0, 1))))
                .isInstanceOf(GlobalIndexLookupDeclinedException.class);
        input.close();
    }

    @Test
    public void testRejectedVectoredBatchDoesNotPartiallyReserve() throws Exception {
        GlobalIndexQueryContext context =
                new GlobalIndexQueryContext(Long.MAX_VALUE, Long.MAX_VALUE, 3, 3);
        GlobalIndexFileReader delegate = ignored -> new VectoredByteArrayInput(DATA);
        SeekableInputStream input =
                new BudgetedGlobalIndexFileReader(delegate, context).getInputStream(META);

        assertThatThrownBy(
                        () ->
                                ((VectoredReadable) input)
                                        .readVectored(
                                                java.util.Arrays.asList(
                                                        FileRange.createFileRange(0, 2),
                                                        FileRange.createFileRange(2, 2))))
                .isInstanceOf(GlobalIndexLookupDeclinedException.class);
        assertThat(context.readBytes()).isZero();
        input.close();
    }

    private static class VectoredByteArrayInput extends ByteArraySeekableStream
            implements VectoredReadable {

        private VectoredByteArrayInput(byte[] bytes) {
            super(bytes);
        }

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            long originalPosition = getPos();
            try {
                seek(position);
                return read(buffer, offset, length);
            } finally {
                seek(originalPosition);
            }
        }
    }
}
