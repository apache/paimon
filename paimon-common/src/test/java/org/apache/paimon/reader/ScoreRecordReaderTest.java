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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ScoreRecordReader}. */
public class ScoreRecordReaderTest {

    @Test
    public void testFilterAndTransformPreserveScoreMetadata() throws IOException {
        ScoreRecordReader<Integer> reader =
                new ScoreRecordReader<Integer>() {
                    private boolean emitted;

                    @Nullable
                    @Override
                    public ScoreRecordIterator<Integer> readBatch() {
                        if (emitted) {
                            return null;
                        }
                        emitted = true;
                        return new TestingScoreRecordIterator();
                    }

                    @Override
                    public void close() {}
                };

        ScoreRecordReader<String> transformed =
                reader.filter(value -> value == 1).transform(String::valueOf);
        ScoreRecordIterator<String> iterator = transformed.readBatch();

        assertThat(iterator).isNotNull();
        assertThat(iterator.next()).isEqualTo("1");
        assertThat(iterator.returnedRowId()).isEqualTo(11L);
        assertThat(iterator.returnedScore()).isEqualTo(0.75f);
        assertThat(iterator.next()).isNull();
        assertThat(transformed.readBatch()).isNull();
    }

    private static class TestingScoreRecordIterator implements ScoreRecordIterator<Integer> {

        private final int[] values = {0, 1};
        private final long[] rowIds = {10L, 11L};
        private final float[] scores = {0.25f, 0.75f};
        private int index = -1;

        @Nullable
        @Override
        public Integer next() {
            index++;
            return index < values.length ? values[index] : null;
        }

        @Override
        public float returnedScore() {
            return scores[index];
        }

        @Override
        public long returnedRowId() {
            return rowIds[index];
        }

        @Override
        public void releaseBatch() {}
    }
}
