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

package org.apache.paimon.crosspartition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Pair;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GlobalIndexAssigner}. */
public class GlobalIndexAssignerTest extends TableTestBase {

    private GlobalIndexAssigner createAssigner(MergeEngine mergeEngine) throws Exception {
        return createAssigner(mergeEngine, false);
    }

    private GlobalIndexAssigner createAssigner(MergeEngine mergeEngine, boolean enableTtl)
            throws Exception {
        Identifier identifier = identifier("T");
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, mergeEngine);
        if (mergeEngine == MergeEngine.FIRST_ROW) {
            options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        }
        options.set(CoreOptions.DYNAMIC_BUCKET_TARGET_ROW_NUM, 3L);
        options.set(CoreOptions.BUCKET, -1);
        if (enableTtl) {
            options.set(CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL, Duration.ofSeconds(1000));
        }
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk")
                        .options(options.toMap())
                        .build();
        catalog.createTable(identifier, schema, true);
        return new GlobalIndexAssigner(catalog.getTable(identifier));
    }

    @Test
    public void testBucketAssign() throws Exception {
        innerTestBucketAssign(false);
    }

    @Test
    public void testEnableTtl() throws Exception {
        innerTestBucketAssign(true);
    }

    private IOManager ioManager() {
        return IOManager.create(new File(tempPath.toFile(), "io").getPath());
    }

    private void innerTestBucketAssign(boolean enableTtl) throws Exception {
        GlobalIndexAssigner assigner = createAssigner(MergeEngine.DEDUPLICATE, enableTtl);
        List<Integer> output = new ArrayList<>();
        assigner.open(0, ioManager(), 2, 0, (row, bucket) -> output.add(bucket));
        assigner.endBoostrap(false);

        // assign
        assigner.processInput(GenericRow.of(1, 1, 1));
        assigner.processInput(GenericRow.of(1, 2, 2));
        assigner.processInput(GenericRow.of(1, 3, 3));
        assertThat(output).containsExactly(0, 0, 0);
        output.clear();

        // full
        assigner.processInput(GenericRow.of(1, 4, 4));
        assertThat(output).containsExactly(2);
        output.clear();

        // another partition
        assigner.processInput(GenericRow.of(2, 5, 5));
        assertThat(output).containsExactly(0);
        output.clear();

        // read assigned
        assigner.processInput(GenericRow.of(1, 4, 4));
        assigner.processInput(GenericRow.of(1, 2, 2));
        assigner.processInput(GenericRow.of(1, 3, 3));
        assertThat(output).containsExactly(2, 0, 0);
        output.clear();

        assigner.close();
    }

    @Test
    public void testUpsert() throws Exception {
        GlobalIndexAssigner assigner = createAssigner(MergeEngine.DEDUPLICATE);
        List<Pair<InternalRow, Integer>> output = new ArrayList<>();
        assigner.open(0, ioManager(), 2, 0, (row, bucket) -> output.add(Pair.of(row, bucket)));
        assigner.endBoostrap(false);

        // change partition
        assigner.processInput(GenericRow.of(1, 1, 1));
        assigner.processInput(GenericRow.of(2, 1, 2));
        Assertions.assertThat(output)
                .containsExactly(
                        Pair.of(GenericRow.of(1, 1, 1), 0),
                        Pair.of(GenericRow.ofKind(RowKind.DELETE, 1, 1, 2), 0),
                        Pair.of(GenericRow.of(2, 1, 2), 0));
        output.clear();

        // test partition 1 deleted
        assigner.processInput(GenericRow.of(1, 2, 2));
        assigner.processInput(GenericRow.of(1, 3, 3));
        assigner.processInput(GenericRow.of(1, 4, 4));
        assertThat(output.stream().map(Pair::getRight)).containsExactly(0, 0, 0);
        output.clear();

        // move from full bucket
        assigner.processInput(GenericRow.of(2, 4, 4));
        Assertions.assertThat(output)
                .containsExactly(
                        Pair.of(GenericRow.ofKind(RowKind.DELETE, 1, 4, 4), 0),
                        Pair.of(GenericRow.of(2, 4, 4), 0));
        output.clear();

        // test partition 1 deleted
        assigner.processInput(GenericRow.of(1, 5, 5));
        assertThat(output.stream().map(Pair::getRight)).containsExactly(0);
        output.clear();

        assigner.close();
    }

    @Test
    public void testUseOldPartition() throws Exception {
        MergeEngine mergeEngine =
                ThreadLocalRandom.current().nextBoolean()
                        ? MergeEngine.PARTIAL_UPDATE
                        : MergeEngine.AGGREGATE;
        GlobalIndexAssigner assigner = createAssigner(mergeEngine);
        List<Pair<InternalRow, Integer>> output = new ArrayList<>();
        assigner.open(0, ioManager(), 2, 0, (row, bucket) -> output.add(Pair.of(row, bucket)));
        assigner.endBoostrap(false);

        // change partition
        assigner.processInput(GenericRow.of(1, 1, 1));
        assigner.processInput(GenericRow.of(2, 1, 2));
        Assertions.assertThat(output)
                .containsExactly(
                        Pair.of(GenericRow.of(1, 1, 1), 0), Pair.of(GenericRow.of(1, 1, 2), 0));
        output.clear();

        // test partition 2 no effect
        assigner.processInput(GenericRow.of(2, 2, 2));
        assigner.processInput(GenericRow.of(2, 3, 3));
        assigner.processInput(GenericRow.of(2, 4, 4));
        assertThat(output.stream().map(Pair::getRight)).containsExactly(0, 0, 0);
        output.clear();
        assigner.close();
    }

    @Test
    public void testFirstRow() throws Exception {
        GlobalIndexAssigner assigner = createAssigner(MergeEngine.FIRST_ROW);
        List<Pair<InternalRow, Integer>> output = new ArrayList<>();
        assigner.open(0, ioManager(), 2, 0, (row, bucket) -> output.add(Pair.of(row, bucket)));
        assigner.endBoostrap(false);

        // change partition
        assigner.processInput(GenericRow.of(1, 1, 1));
        assigner.processInput(GenericRow.of(2, 1, 2));
        Assertions.assertThat(output).containsExactly(Pair.of(GenericRow.of(1, 1, 1), 0));
        output.clear();

        // test partition 2 no effect
        assigner.processInput(GenericRow.of(2, 2, 2));
        assigner.processInput(GenericRow.of(2, 3, 3));
        assigner.processInput(GenericRow.of(2, 4, 4));
        assertThat(output.stream().map(Pair::getRight)).containsExactly(0, 0, 0);
        output.clear();
        assigner.close();
    }

    @Test
    public void testBootstrapRecords() throws Exception {
        GlobalIndexAssigner assigner = createAssigner(MergeEngine.DEDUPLICATE);
        List<List<Integer>> output = new ArrayList<>();
        assigner.open(
                0,
                ioManager(),
                2,
                0,
                (row, bucket) ->
                        output.add(
                                Arrays.asList(
                                        row.getInt(0), row.getInt(1), row.getInt(2), bucket)));

        assigner.processInput(GenericRow.of(1, 1, 1));
        assigner.processInput(GenericRow.of(2, 1, 2));
        assigner.processInput(GenericRow.of(2, 2, 2));
        assigner.processInput(GenericRow.of(2, 3, 3));
        assigner.processInput(GenericRow.of(2, 4, 4));
        assigner.endBoostrap(true);

        assertThat(output)
                .containsExactlyInAnyOrder(
                        Arrays.asList(2, 1, 2, 0),
                        Arrays.asList(2, 2, 2, 0),
                        Arrays.asList(2, 3, 3, 0),
                        Arrays.asList(2, 4, 4, 2));
        output.clear();
        assigner.close();
    }

    @Test
    public void testBootstrapWithTTL() throws Exception {
        // enableTtl is true
        GlobalIndexAssigner assigner = createAssigner(MergeEngine.DEDUPLICATE, true);
        List<List<Integer>> output = new ArrayList<>();
        assigner.open(
                0,
                ioManager(),
                2,
                0,
                (row, bucket) ->
                        output.add(
                                Arrays.asList(
                                        row.getInt(0), row.getInt(1), row.getInt(2), bucket)));

        // assigner.bootstrapKey can trigger the problem
        assigner.bootstrapKey(GenericRow.of(1, 1, 1));
        assigner.processInput(GenericRow.of(1, 1, 1));
        assigner.endBoostrap(true);

        assertThat(output).containsExactlyInAnyOrder(Arrays.asList(1, 1, 1, 1));
    }
}
