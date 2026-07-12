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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVector}. */
public class DeletionVectorTest {

    @Test
    public void testApplyDeletionFileRecordIteratorUsesProvidedJudger() throws Exception {
        DeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.checkedDelete(12);
        deletionVector.checkedDelete(14);

        ApplyDeletionFileRecordIterator iterator =
                new ApplyDeletionFileRecordIterator(
                        new TestingFileRecordIterator(5),
                        position -> deletionVector.isDeleted(10 + position));

        assertThat(iterator.deletionVector().isDeleted(2)).isTrue();
        assertThat(iterator.deletionVector().isDeleted(4)).isTrue();
        assertThat(iterator.deletionVector().isDeleted(12)).isFalse();

        assertThat(iterator.next().getInt(0)).isEqualTo(0);
        assertThat(iterator.next().getInt(0)).isEqualTo(1);
        assertThat(iterator.next().getInt(0)).isEqualTo(3);
        assertThat(iterator.next()).isNull();
    }

    @Test
    public void testApplyDeletionVectorReaderUsesOffsetAwareJudger() throws Exception {
        DeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.checkedDelete(12);
        deletionVector.checkedDelete(14);

        ApplyDeletionVectorReader reader =
                new ApplyDeletionVectorReader(
                        new TestingFileRecordReader(new TestingFileRecordIterator(5)),
                        deletionVector,
                        10);

        assertThat(reader.deletionVector().isDeleted(2)).isTrue();
        assertThat(reader.deletionVector().isDeleted(4)).isTrue();
        assertThat(reader.deletionVector().isDeleted(12)).isFalse();

        FileRecordIterator<InternalRow> batch = reader.readBatch();
        assertThat(batch).isInstanceOf(ApplyDeletionFileRecordIterator.class);
        ApplyDeletionFileRecordIterator iterator = (ApplyDeletionFileRecordIterator) batch;
        assertThat(iterator.deletionVector()).isSameAs(reader.deletionVector());

        assertThat(iterator.next().getInt(0)).isEqualTo(0);
        assertThat(iterator.next().getInt(0)).isEqualTo(1);
        assertThat(iterator.next().getInt(0)).isEqualTo(3);
        assertThat(iterator.next()).isNull();
    }

    @Test
    public void testBitmapDeletionVector() {
        HashSet<Integer> toDelete = new HashSet<>();
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            toDelete.add(random.nextInt());
        }
        HashSet<Integer> notDelete = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            if (!toDelete.contains(i)) {
                notDelete.add(i);
            }
        }

        DeletionVector deletionVector = new BitmapDeletionVector();
        assertThat(deletionVector.isEmpty()).isTrue();

        for (Integer i : toDelete) {
            assertThat(deletionVector.checkedDelete(i)).isTrue();
            assertThat(deletionVector.checkedDelete(i)).isFalse();
        }
        DeletionVector deserializedDeletionVector =
                DeletionVector.deserializeFromBytes(
                        DeletionVector.serializeToBytes(deletionVector));

        assertThat(deletionVector.isEmpty()).isFalse();
        assertThat(deserializedDeletionVector.isEmpty()).isFalse();
        for (Integer i : toDelete) {
            assertThat(deletionVector.isDeleted(i)).isTrue();
            assertThat(deserializedDeletionVector.isDeleted(i)).isTrue();
        }
        for (Integer i : notDelete) {
            assertThat(deletionVector.isDeleted(i)).isFalse();
            assertThat(deserializedDeletionVector.isDeleted(i)).isFalse();
        }
    }

    @Test
    public void testBitmap64DeletionVector() {
        HashSet<Long> toDelete = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            toDelete.add(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE * 2L));
        }
        toDelete.add(1L);
        HashSet<Long> notDelete = new HashSet<>();
        for (long i = 0; i < 10000; i++) {
            if (!toDelete.contains(i)) {
                notDelete.add(i);
            }
        }

        DeletionVector deletionVector = new Bitmap64DeletionVector();
        assertThat(deletionVector.isEmpty()).isTrue();

        for (Long i : toDelete) {
            assertThat(deletionVector.checkedDelete(i)).isTrue();
            assertThat(deletionVector.checkedDelete(i)).isFalse();
        }
        DeletionVector deserializedDeletionVector =
                DeletionVector.deserializeFromBytes(
                        DeletionVector.serializeToBytes(deletionVector));

        assertThat(deletionVector.isEmpty()).isFalse();
        assertThat(deserializedDeletionVector.isEmpty()).isFalse();
        for (Long i : toDelete) {
            assertThat(deletionVector.isDeleted(i)).isTrue();
            assertThat(deserializedDeletionVector.isDeleted(i)).isTrue();
        }
        for (Long i : notDelete) {
            assertThat(deletionVector.isDeleted(i)).isFalse();
            assertThat(deserializedDeletionVector.isDeleted(i)).isFalse();
        }
    }

    @Test
    public void testBitmapDeletionVectorTo64() {
        HashSet<Integer> toDelete = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            toDelete.add(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        }
        HashSet<Integer> notDelete = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            if (!toDelete.contains(i)) {
                notDelete.add(i);
            }
        }

        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        assertThat(deletionVector.isEmpty()).isTrue();

        for (Integer i : toDelete) {
            assertThat(deletionVector.checkedDelete(i)).isTrue();
            assertThat(deletionVector.checkedDelete(i)).isFalse();
        }

        DeletionVector bitmap64DeletionVector =
                Bitmap64DeletionVector.fromBitmapDeletionVector(deletionVector);

        assertThat(bitmap64DeletionVector.isEmpty()).isFalse();

        for (Integer i : toDelete) {
            assertThat(deletionVector.isDeleted(i)).isTrue();
            assertThat(bitmap64DeletionVector.isDeleted(i)).isTrue();
        }
        for (Integer i : notDelete) {
            assertThat(deletionVector.isDeleted(i)).isFalse();
            assertThat(bitmap64DeletionVector.isDeleted(i)).isFalse();
        }
    }

    private static class TestingFileRecordIterator implements FileRecordIterator<InternalRow> {

        private final int rows;
        private int nextPosition;
        private int returnedPosition = -1;

        private TestingFileRecordIterator(int rows) {
            this.rows = rows;
        }

        @Override
        public long returnedPosition() {
            return returnedPosition;
        }

        @Override
        public Path filePath() {
            return new Path("/tmp/testing");
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            if (nextPosition >= rows) {
                return null;
            }
            returnedPosition = nextPosition++;
            return GenericRow.of(returnedPosition);
        }

        @Override
        public void releaseBatch() {}
    }

    private static class TestingFileRecordReader implements FileRecordReader<InternalRow> {

        private final FileRecordIterator<InternalRow> iterator;
        private boolean returned;

        private TestingFileRecordReader(FileRecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
        }

        @Nullable
        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            if (returned) {
                return null;
            }
            returned = true;
            return iterator;
        }

        @Override
        public void close() {}
    }
}
