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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for byte-backed, repeatable commit messages. */
class CommitMessageListTest {

    @Test
    void testIteratorCannotInvalidateByteOrChildDirectory() throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        CommitMessageList bytes =
                CommitMessageList.fromBytes(
                        Arrays.asList(
                                serializer.serialize(message(1)),
                                serializer.serialize(message(2))));
        List<CommitMessage> concatenated =
                CommitMessageList.concat(Arrays.asList(Collections.emptyList(), bytes));
        for (List<CommitMessage> list :
                Arrays.asList(bytes, concatenated, CommitMessageList.map(concatenated, m -> m))) {
            Iterator<CommitMessage> iterator = list.iterator();
            assertThat(iterator.next()).isEqualTo(message(1));
            assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
            assertThat(list).containsExactly(message(1), message(2));
            assertThat(list.size()).isEqualTo(2);
        }
        List<CommitMessage> stable = bytes.materialize();
        stable.set(0, message(3));
        Iterator<CommitMessage> iterator = bytes.iterator();
        assertThat(iterator.next()).isSameAs(stable.get(0));
        assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testDecodeOnlyAccessedMessage() throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> messages =
                CommitMessageList.fromBytes(
                        Arrays.asList(serializer.serialize(message(1)), new byte[0]));
        assertThat(messages.size()).isEqualTo(2);
        assertThat(messages.get(0)).isEqualTo(message(1));
        assertThat(messages.get(0)).isNotSameAs(messages.get(0));
        assertThatThrownBy(() -> messages.get(1)).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> messages.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> messages.get(2)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testConcatAndMapRemainLazyAndReplayable() throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> first =
                CommitMessageList.fromBytes(
                        Collections.singletonList(serializer.serialize(message(1))));
        List<CommitMessage> second =
                CommitMessageList.fromBytes(
                        Collections.singletonList(serializer.serialize(message(2))));
        List<CommitMessage> joined =
                CommitMessageList.concat(Arrays.asList(Collections.emptyList(), first, second));
        List<CommitMessage> checked =
                CommitMessageList.map(
                        joined, m -> ((CommitMessageImpl) m).withCheckFromSnapshot(42));
        assertThat(checked)
                .containsExactly(
                        message(1).withCheckFromSnapshot(42), message(2).withCheckFromSnapshot(42));
        assertThat(checked)
                .containsExactly(
                        message(1).withCheckFromSnapshot(42), message(2).withCheckFromSnapshot(42));
        assertThat(checked.get(0)).isNotSameAs(checked.get(0));
        assertThatThrownBy(() -> checked.add(message(3)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(joined.get(1)).isEqualTo(message(2));
    }

    private static CommitMessageImpl message(int bucket) {
        return new CommitMessageImpl(
                BinaryRow.EMPTY_ROW,
                bucket,
                null,
                DataIncrement.emptyIncrement(),
                CompactIncrement.emptyIncrement());
    }
}
