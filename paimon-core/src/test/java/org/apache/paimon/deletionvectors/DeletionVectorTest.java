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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVector}. */
public class DeletionVectorTest {
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
                        deletionVector.serializeToBytes(), BitmapDeletionVector.VERSION);

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
                        deletionVector.serializeToBytes(), Bitmap64DeletionVector.VERSION);

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
}
