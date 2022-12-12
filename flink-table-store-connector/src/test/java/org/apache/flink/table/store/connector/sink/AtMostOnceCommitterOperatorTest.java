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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.TableWrite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/** Tests for {@link AtMostOnceCommitterOperator}. */
public class AtMostOnceCommitterOperatorTest extends CommitterOperatorTestBase {

    private String initialCommitUser;

    @BeforeEach
    public void before() {
        super.before();
        initialCommitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testSnapshotLostWhenFailed() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(table);
        testHarness.open();

        long timestamp = 1;

        // this checkpoint is notified, should be committed
        TableWrite write = table.newWrite(initialCommitUser);
        write.write(GenericRowData.of(1, 10L));
        write.write(GenericRowData.of(2, 20L));
        for (FileCommittable committable : write.prepareCommit(false, 1)) {
            testHarness.processElement(
                    new Committable(1, Committable.Kind.FILE, committable), timestamp++);
        }
        testHarness.snapshot(1, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(1);

        // this checkpoint is not notified, should not be committed
        write.write(GenericRowData.of(3, 30L));
        write.write(GenericRowData.of(4, 40L));
        for (FileCommittable committable : write.prepareCommit(false, 2)) {
            testHarness.processElement(
                    new Committable(2, Committable.Kind.FILE, committable), timestamp++);
        }
        OperatorSubtaskState snapshot = testHarness.snapshot(2, timestamp++);

        // reopen test harness
        write.close();
        testHarness.close();

        testHarness = createTestHarness(table);
        testHarness.initializeState(snapshot);
        testHarness.open();

        // this checkpoint is notified, should be committed
        write = table.newWrite(initialCommitUser);
        write.write(GenericRowData.of(5, 50L));
        write.write(GenericRowData.of(6, 60L));
        for (FileCommittable committable : write.prepareCommit(false, 3)) {
            testHarness.processElement(
                    new Committable(3, Committable.Kind.FILE, committable), timestamp++);
        }
        testHarness.snapshot(3, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(3);

        write.close();
        testHarness.close();

        assertResults(table, "1, 10", "2, 20", "5, 50", "6, 60");
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            FileStoreTable table) throws Exception {
        AtMostOnceCommitterOperator operator =
                new AtMostOnceCommitterOperator(
                        true, initialCommitUser, user -> new StoreCommitter(table.newCommit(user)));
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Committable, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, serializer);
        harness.setup(serializer);
        return harness;
    }
}
