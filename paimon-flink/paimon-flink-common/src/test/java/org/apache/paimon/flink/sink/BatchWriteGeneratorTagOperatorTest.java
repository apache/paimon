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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BatchWriteGeneratorTagOperator}. */
public class BatchWriteGeneratorTagOperatorTest extends CommitterOperatorTest {

    @Test
    public void testBatchWriteGeneratorTag() throws Exception {
        FileStoreTable table = createFileStoreTable();
        // set tag.automatic-creation = batch
        HashMap<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("tag.automatic-creation", "batch");
        dynamicOptions.put("tag.num-retained-max", "2");
        table = table.copy(dynamicOptions);

        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        OneInputStreamOperatorFactory<Committable, Committable> committerOperatorFactory =
                createCommitterOperator(
                        table,
                        initialCommitUser,
                        new RestoreAndFailCommittableStateManager<>(
                                ManifestCommittableSerializer::new));

        OneInputStreamOperator<Committable, Committable> committerOperator =
                committerOperatorFactory.createStreamOperator(
                        new StreamOperatorParameters<>(
                                new SourceOperatorStreamTask<Integer>(new DummyEnvironment()),
                                new MockStreamConfig(new Configuration(), 1),
                                new MockOutput<>(new ArrayList<>()),
                                null,
                                null,
                                null));

        committerOperator.open();

        TableCommitImpl tableCommit = table.newCommit(initialCommitUser);

        write.write(GenericRow.of(1, 10L));
        tableCommit.commit(write.prepareCommit(false, 1));

        SnapshotManager snapshotManager = table.newSnapshotReader().snapshotManager();
        TagManager tagManager = table.tagManager();

        // No tag is generated before the finish method
        assertThat(table.tagManager().tagCount()).isEqualTo(0);
        committerOperator.finish();
        // After the finish method, a tag is generated
        assertThat(table.tagManager().tagCount()).isEqualTo(1);
        // Get tagName from tagManager.
        String tagName = tagManager.allTagNames().get(0);
        // The tag is consistent with the latest snapshot
        assertThat(tagManager.taggedSnapshot(tagName)).isEqualTo(snapshotManager.latestSnapshot());

        // test tag expiration
        table.createTag("many-tags-test1");
        Thread.sleep(1_000);
        table.createTag("many-tags-test2");
        assertThat(tagManager.tagCount()).isEqualTo(3);

        write.write(GenericRow.of(2, 20L));
        tableCommit = table.newCommit(initialCommitUser);
        tableCommit.commit(write.prepareCommit(false, 2));
        // note that this tag has the same name with previous tag
        // so the previous tag will be deleted
        committerOperator.finish();
        // If tagName does not exist, it happened across the day.
        if (!tagManager.tagExists(tagName)) {
            //  Generate tag name
            String prefix = "batch-write-";
            Instant instant =
                    Instant.ofEpochMilli(
                            Objects.requireNonNull(snapshotManager.latestSnapshot()).timeMillis());
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            tagName = prefix + localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }
        assertThat(tagManager.allTagNames()).containsOnly("many-tags-test2", tagName);
    }

    @Override
    protected OneInputStreamOperatorFactory<Committable, Committable> createCommitterOperator(
            FileStoreTable table,
            String commitUser,
            CommittableStateManager<ManifestCommittable> committableStateManager) {
        return new BatchWriteGeneratorTagOperatorFactory<>(
                (CommitterOperatorFactory<Committable, ManifestCommittable>)
                        super.createCommitterOperator(table, commitUser, committableStateManager),
                table);
    }
}
