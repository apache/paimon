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

package org.apache.paimon.reader;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RangeSkipReader}. */
public class RangeSkipReaderTest {

    /** A simple in-memory RecordReader over a list of integers, one element per batch. */
    private static final class ListRecordReader implements RecordReader<Integer> {

        private final List<Integer> data;

        private int idx;

        ListRecordReader(List<Integer> data) {
            this.data = data;
        }

        @Nullable
        @Override
        public RecordIterator<Integer> readBatch() {
            if (idx >= data.size()) {
                return null;
            }
            final int current = idx++;
            return new RecordIterator<Integer>() {
                private boolean consumed = false;

                @Nullable
                @Override
                public Integer next() {
                    if (consumed) {
                        return null;
                    }
                    consumed = true;
                    return data.get(current);
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() {}
    }

    private List<Integer> readAll(RecordReader<Integer> reader) throws IOException {
        List<Integer> result = new ArrayList<>();
        RecordReader.RecordIterator<Integer> batch;
        while ((batch = reader.readBatch()) != null) {
            Integer v;
            while ((v = batch.next()) != null) {
                result.add(v);
            }
            batch.releaseBatch();
        }
        return result;
    }

    @Test
    public void testSkipAndLimit() throws IOException {
        List<Integer> data = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        RangeSkipReader<Integer> reader = new RangeSkipReader<>(new ListRecordReader(data), 3, 4);
        // skip first 3 (0,1,2), take 4 (3,4,5,6)
        assertThat(readAll(reader)).containsExactly(3, 4, 5, 6);
    }

    @Test
    public void testSkipZero() throws IOException {
        List<Integer> data = Arrays.asList(0, 1, 2, 3, 4);
        RangeSkipReader<Integer> reader = new RangeSkipReader<>(new ListRecordReader(data), 0, 3);
        assertThat(readAll(reader)).containsExactly(0, 1, 2);
    }

    @Test
    public void testLimitToEnd() throws IOException {
        List<Integer> data = Arrays.asList(0, 1, 2, 3, 4);
        RangeSkipReader<Integer> reader =
                new RangeSkipReader<>(new ListRecordReader(data), 2, Long.MAX_VALUE);
        // skip 2, read to end
        assertThat(readAll(reader)).containsExactly(2, 3, 4);
    }

    @Test
    public void testSkipBeyondData() throws IOException {
        List<Integer> data = Arrays.asList(0, 1, 2);
        RangeSkipReader<Integer> reader = new RangeSkipReader<>(new ListRecordReader(data), 5, 3);
        // skip beyond data -> empty
        assertThat(readAll(reader)).isEmpty();
    }

    @Test
    public void testLimitBeyondData() throws IOException {
        List<Integer> data = Arrays.asList(0, 1, 2);
        RangeSkipReader<Integer> reader = new RangeSkipReader<>(new ListRecordReader(data), 1, 10);
        // skip 1, limit exceeds data -> read remaining
        assertThat(readAll(reader)).containsExactly(1, 2);
    }

    @Test
    public void testEarlyTermination() throws IOException {
        // with limit 2 and skip 1 from 10 elements, the reader must stop after 2 returned rows.
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(i);
        }
        RangeSkipReader<Integer> reader = new RangeSkipReader<>(new ListRecordReader(data), 1, 2);
        assertThat(readAll(reader)).containsExactly(1, 2);
    }
}
