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

package org.apache.paimon.flink.sink;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.paimon.append.MultiTableAppendOnlyCompactionTask;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.AppendOnlySingleTableCompactionWorkerOperatorTest.packTask;

/**
 * test for {@link AppendOnlyMultiTableCompactionWorkerOperator}.
 */
public class AppendOnlyMultiTableCompactionWorkerOperatorTest extends TableTestBase {
    private final String[] tables = {
            "a", "b"
    };
    @Test
    public void testAsyncCompactionWorks() throws Exception {

        AppendOnlyMultiTableCompactionWorkerOperator workerOperator =
                new AppendOnlyMultiTableCompactionWorkerOperator(()->catalog, "user",new Options());

        List<StreamRecord<MultiTableAppendOnlyCompactionTask>> records =new ArrayList<>();
        //create table and write
        for (String table : tables) {
            Identifier identifier = identifier(table);
            createTable(identifier);

            // write 200 files
            List<CommitMessage> commitMessages = writeData(getTable(identifier), 200, 20);

            packTask(commitMessages, 5).stream().map(
                    task->new StreamRecord<>(new MultiTableAppendOnlyCompactionTask(task.partition(),task.compactBefore(),identifier))
            ).forEach(records::add);
        }

        Assertions.assertThat(records.size()).isEqualTo(8);
        workerOperator.open();

        for (StreamRecord<MultiTableAppendOnlyCompactionTask> record : records) {
            workerOperator.processElement(record);
        }

        List<MultiTableCommittable> committables = new ArrayList<>();
        Long timeStart = System.currentTimeMillis();
        long timeout = 60_000L;

        Assertions.assertThatCode(
                        () -> {
                            while (committables.size() != 8) {
                                committables.addAll(
                                        workerOperator.prepareCommit(false, Long.MAX_VALUE));

                                Long now = System.currentTimeMillis();
                                if (now - timeStart > timeout && committables.size() != 8) {
                                    throw new RuntimeException(
                                            "Timeout waiting for compaction, maybe some error happens in "
                                                    + AppendOnlySingleTableCompactionWorkerOperator
                                                    .class
                                                    .getName());
                                }
                                Thread.sleep(1_000L);
                            }
                        })
                .doesNotThrowAnyException();
        committables.forEach(
                a ->
                        Assertions.assertThat(
                                        ((CommitMessageImpl) a.wrappedCommittable())
                                                .compactIncrement()
                                                .compactAfter()
                                                .size()
                                                == 1)
                                .isTrue());
        Set<String> table = committables.stream().map(MultiTableCommittable::getTable).collect(Collectors.toSet());
        Assertions.assertThat(table).hasSameElementsAs(Arrays.asList(tables));
    }
}
