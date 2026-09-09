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

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LimitRecordReader}. */
public class LimitRecordReaderTest {

    @Test
    public void testPreservesFileRecordIterator() throws Exception {
        FileRecordIterator<Integer> fileIterator =
                new FileRecordIterator<Integer>() {
                    private int position = -1;

                    @Override
                    public long returnedPosition() {
                        return position;
                    }

                    @Override
                    public Path filePath() {
                        return new Path("test-file.parquet");
                    }

                    @Nullable
                    @Override
                    public Integer next() {
                        position++;
                        return position < 3 ? position : null;
                    }

                    @Override
                    public void releaseBatch() {}
                };

        FileRecordReader<Integer> fileReader =
                new FileRecordReader<Integer>() {
                    private boolean batchReturned;

                    @Nullable
                    @Override
                    public FileRecordIterator<Integer> readBatch() {
                        if (batchReturned) {
                            return null;
                        }
                        batchReturned = true;
                        return fileIterator;
                    }

                    @Override
                    public void close() {}
                };

        try (RecordReader<Integer> reader = LimitRecordReader.limit(fileReader, 2)) {
            RecordReader.RecordIterator<Integer> batch = reader.readBatch();
            assertThat(batch).isInstanceOf(FileRecordIterator.class);

            FileRecordIterator<?> limited = (FileRecordIterator<?>) batch;
            assertThat(limited.filePath()).isEqualTo(new Path("test-file.parquet"));
            assertThat(limited.next()).isEqualTo(0);
            assertThat(limited.returnedPosition()).isEqualTo(0);
            assertThat(limited.next()).isEqualTo(1);
            assertThat(limited.returnedPosition()).isEqualTo(1);
            assertThat(limited.next()).isNull();
            limited.releaseBatch();

            assertThat(reader.readBatch()).isNull();
        }
    }
}
