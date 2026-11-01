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

import org.apache.paimon.io.DataInputViewStream;
import org.apache.paimon.memory.MemorySegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbstractPagedInputView}. */
class AbstractPagedInputViewTest {

    private static final int PAGE_SIZE = 4;

    @ParameterizedTest
    @CsvSource({
        "0, 0, 1",
        "3, 0, 10",
        "3, 1, 10",
        "7, 0, 10",
        "7, 5, 10",
        "8, 0, 10",
        "8, 5, 10",
        "3, 0, 0",
        "3, 0, 2",
        "3, 0, 3",
        "7, 0, 4",
        "7, 0, 6",
        "7, 1, 6",
        "7, 4, 3",
        "7, 7, 10"
    })
    void testSkipBytes(int length, int initialPosition, int bytesToSkip) throws IOException {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i + 1);
        }
        RandomAccessInputView input = createInput(data);
        try (DataInputStream reference = new DataInputStream(new ByteArrayInputStream(data))) {
            for (int i = 0; i < initialPosition; i++) {
                assertThat(input.readUnsignedByte()).isEqualTo(reference.readUnsignedByte());
            }

            assertThat(input.skipBytes(bytesToSkip)).isEqualTo(reference.skipBytes(bytesToSkip));
            while (reference.available() > 0) {
                assertThat(input.readUnsignedByte()).isEqualTo(reference.readUnsignedByte());
            }

            for (int i = 0; i < 2; i++) {
                assertThat(input.skipBytes(1)).isZero();
                assertThatThrownBy(input::readUnsignedByte).isInstanceOf(EOFException.class);
            }
            assertThat(input.read(new byte[1])).isEqualTo(-1);
            assertThat(input.skipBytes(0)).isZero();
        }

        if (data.length > 0) {
            input.setReadPosition(0);
            assertThat(input.readUnsignedByte()).isEqualTo(data[0]);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {10, 2147483648L})
    void testSkipThroughInputStream(long bytesToSkip) throws IOException {
        byte[] data = {1, 2, 3, 4, 5, 6, 7};
        try (DataInputViewStream input = new DataInputViewStream(createInput(data));
                ByteArrayInputStream reference = new ByteArrayInputStream(data)) {
            assertThat(input.skip(bytesToSkip)).isEqualTo(reference.skip(bytesToSkip));
            assertThat(input.skip(1)).isZero();
            assertThat(input.read()).isEqualTo(reference.read());
            assertThat(input.read()).isEqualTo(-1);
        }
    }

    @Test
    void testSkipPropagatesIOException() {
        IOException failure = new IOException("Cannot read next page");
        AbstractPagedInputView input =
                new AbstractPagedInputView(MemorySegment.wrap(new byte[PAGE_SIZE]), PAGE_SIZE) {
                    @Override
                    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
                        throw failure;
                    }

                    @Override
                    protected int getLimitForSegment(MemorySegment segment) {
                        return PAGE_SIZE;
                    }
                };

        assertThatThrownBy(() -> input.skipBytes(PAGE_SIZE + 1)).isSameAs(failure);
    }

    private static RandomAccessInputView createInput(byte[] data) {
        ArrayList<MemorySegment> pages = new ArrayList<>();
        int pageCount = Math.max(1, (data.length + PAGE_SIZE - 1) / PAGE_SIZE);
        for (int i = 0; i < pageCount; i++) {
            pages.add(
                    MemorySegment.wrap(
                            Arrays.copyOfRange(data, i * PAGE_SIZE, (i + 1) * PAGE_SIZE)));
        }
        return new RandomAccessInputView(
                pages, PAGE_SIZE, data.length - (pageCount - 1) * PAGE_SIZE);
    }
}
