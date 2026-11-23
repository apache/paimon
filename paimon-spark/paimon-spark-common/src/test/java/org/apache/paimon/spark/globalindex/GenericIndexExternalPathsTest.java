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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests path allocation across serialized shards in one Spark partition. */
class GenericIndexExternalPathsTest extends TableTestBase {

    @Test
    void testRoundRobinAcrossShards() throws Exception {
        List<Path> roots = Arrays.asList(new Path(warehouse, "a"), new Path(warehouse, "b"));
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option("bucket", "-1")
                        .option("file.format", "avro")
                        .option("row-tracking.enabled", "true")
                        .option("data-evolution.enabled", "true")
                        .option("global-index.external-paths", roots.get(0) + "," + roots.get(1))
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < 20; i++) {
                write.write(GenericRow.of(i));
            }
            commit.commit(write.prepareCommit());
        }
        List<IndexedSplit> splits =
                GlobalIndexBuilderUtils.createShardIndexedSplits(
                        table, table.store().newScan().plan().files(), 1);
        assertThat(splits).hasSize(20);
        RowType readType = SpecialFields.rowTypeWithRowId(table.rowType());
        List<Pair<byte[], byte[]>> tasks = new ArrayList<>();
        for (IndexedSplit split : splits) {
            DefaultGlobalIndexBuilder builder =
                    new DefaultGlobalIndexBuilder(
                            table,
                            split.dataSplit().partition(),
                            readType,
                            table.rowType().getField("id"),
                            "bitmap",
                            split.rowRanges().get(0),
                            new Options());
            tasks.add(
                    Pair.of(
                            InstantiationUtil.serializeObject(builder),
                            InstantiationUtil.serializeObject(split)));
        }
        Iterator<byte[]> messages = DefaultGlobalIndexTopoBuilder.buildIndexes(tasks.iterator());
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<Path> parents = new ArrayList<>();
        while (messages.hasNext()) {
            CommitMessageImpl message =
                    (CommitMessageImpl)
                            serializer.deserialize(serializer.getVersion(), messages.next());
            List<IndexFileMeta> files = message.newFilesIncrement().newIndexFiles();
            assertThat(files).hasSize(1);
            Path file = new Path(files.get(0).externalPath());
            assertThat(table.fileIO().exists(file)).isTrue();
            assertThat(files.get(0).fileName()).isEqualTo(file.getName());
            parents.add(file.getParent());
        }
        assertThat(parents).hasSize(20).containsOnlyElementsOf(roots);
        for (int i = 1; i < parents.size(); i++) {
            assertThat(parents.get(i)).isNotEqualTo(parents.get(i - 1));
        }
    }
}
