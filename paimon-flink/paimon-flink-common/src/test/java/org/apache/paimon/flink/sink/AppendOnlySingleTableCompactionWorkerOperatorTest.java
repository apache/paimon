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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Tests for {@link AppendOnlySingleTableCompactionWorkerOperator}. */
public class AppendOnlySingleTableCompactionWorkerOperatorTest extends TableTestBase {

    @Test
    public void testAsyncCompactionWorks() throws Exception {
        createTableDefault();
        AppendOnlySingleTableCompactionWorkerOperator workerOperator =
                new AppendOnlySingleTableCompactionWorkerOperator(getTableDefault(), "user");

        // write 200 files
        List<CommitMessage> commitMessages = writeDataDefault(200, 20);

        List<AppendOnlyCompactionTask> tasks = packTask(commitMessages, 5);
        List<StreamRecord<AppendOnlyCompactionTask>> records =
                tasks.stream().map(StreamRecord::new).collect(Collectors.toList());
        Assertions.assertThat(tasks.size()).isEqualTo(4);

        workerOperator.open();

        for (StreamRecord<AppendOnlyCompactionTask> record : records) {
            workerOperator.processElement(record);
        }

        List<Committable> committables = new ArrayList<>();
        Long timeStart = System.currentTimeMillis();
        long timeout = 60_000L;

        Assertions.assertThatCode(
                        () -> {
                            while (committables.size() != 4) {
                                committables.addAll(
                                        workerOperator.prepareCommit(false, Long.MAX_VALUE));

                                Long now = System.currentTimeMillis();
                                if (now - timeStart > timeout && committables.size() != 4) {
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
    }

    @Test
    public void testAsyncCompactionFileDeletedWhenShutdown() throws Exception {
        createTableDefault();
        AppendOnlySingleTableCompactionWorkerOperator workerOperator =
                new AppendOnlySingleTableCompactionWorkerOperator(getTableDefault(), "user");

        // write 200 files
        List<CommitMessage> commitMessages = writeDataDefault(200, 40);

        List<AppendOnlyCompactionTask> tasks = packTask(commitMessages, 5);
        List<StreamRecord<AppendOnlyCompactionTask>> records =
                tasks.stream().map(StreamRecord::new).collect(Collectors.toList());
        Assertions.assertThat(tasks.size()).isEqualTo(8);

        workerOperator.open();

        for (StreamRecord<AppendOnlyCompactionTask> record : records) {
            workerOperator.processElement(record);
        }

        // wait compaction
        Thread.sleep(500);

        LocalFileIO localFileIO = LocalFileIO.create();
        DataFilePathFactory dataFilePathFactory =
                getTableDefault()
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        int i = 0;
        for (Future<CommitMessage> f : workerOperator.result()) {
            if (!f.isDone()) {
                break;
            }
            CommitMessage commitMessage = f.get();
            List<DataFileMeta> fileMetas =
                    ((CommitMessageImpl) commitMessage).compactIncrement().compactAfter();
            for (DataFileMeta fileMeta : fileMetas) {
                Assertions.assertThat(
                                localFileIO.exists(dataFilePathFactory.toPath(fileMeta.fileName())))
                        .isTrue();
            }
            if (i++ > 2) {
                break;
            }
        }

        // shut down worker operator
        workerOperator.close();

        // wait the last runnable in thread pool to stop
        Thread.sleep(2_000);

        for (Future<CommitMessage> f : workerOperator.result()) {
            try {
                if (!f.isDone()) {
                    try {
                        f.get(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        break;
                    }
                }
                CommitMessage commitMessage = f.get();
                List<DataFileMeta> fileMetas =
                        ((CommitMessageImpl) commitMessage).compactIncrement().compactAfter();
                for (DataFileMeta fileMeta : fileMetas) {
                    Assertions.assertThat(
                                    localFileIO.exists(
                                            dataFilePathFactory.toPath(fileMeta.fileName())))
                            .isFalse();
                }
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.BIGINT());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.BUCKET.key(), "-1");
        schemaBuilder.option(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "5");
        return schemaBuilder.build();
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(RANDOM.nextInt(), RANDOM.nextLong(), randomString());
    }

    private List<AppendOnlyCompactionTask> packTask(List<CommitMessage> messages, int fileSize) {
        List<AppendOnlyCompactionTask> result = new ArrayList<>();
        List<DataFileMeta> metas =
                messages.stream()
                        .flatMap(
                                m ->
                                        ((CommitMessageImpl) m)
                                                .newFilesIncrement().newFiles().stream())
                        .collect(Collectors.toList());
        for (int i = 0; i < metas.size(); i += fileSize) {
            if (i < metas.size() - fileSize) {
                result.add(
                        new AppendOnlyCompactionTask(
                                BinaryRow.EMPTY_ROW, metas.subList(i, i + fileSize)));
            } else {
                result.add(
                        new AppendOnlyCompactionTask(
                                BinaryRow.EMPTY_ROW, metas.subList(i, metas.size())));
            }
        }
        return result;
    }
}
