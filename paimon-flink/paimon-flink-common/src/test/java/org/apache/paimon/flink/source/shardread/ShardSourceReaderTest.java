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

package org.apache.paimon.flink.source.shardread;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.DynamicPartitionFilteringInfo;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceReaderTest;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.StaticFileStoreSplitEnumeratorTestBase;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link ShardSourceReader}. */
public class ShardSourceReaderTest extends FileStoreSourceReaderTest {

    private static final String COMMIT_USER = "commit_user";

    private FileStoreTable table;

    @BeforeEach
    @Override
    public void beforeEach() throws Exception {
        super.beforeEach();
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema = schemaManager.latest().get();
        this.table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }

    @Test
    public void testPlanResultIsEmpty() {
        final TestingReaderContext context = new TestingReaderContext();
        final ShardSourceReader reader = (ShardSourceReader) createReader(context);

        reader.start();
        assertThat(context.getNumSplitRequests()).isEqualTo(1);

        reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));

        // splits are only requested when a checkpoint is ready to be triggered
        assertThat(context.getNumSplitRequests()).isEqualTo(2);
    }

    @Test
    public void testPlanResultIsNotEmpty() throws Exception {
        writeTable();
        final TestingReaderContext context = new TestingReaderContext();
        final ShardSourceReader reader = (ShardSourceReader) createReader(context);

        reader.start();
        assertThat(context.getNumSplitRequests()).isEqualTo(1);

        reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));

        // splits are only requested when a checkpoint is ready to be triggered
        assertThat(context.getNumSplitRequests()).isEqualTo(1);
    }

    @Test
    public void testNoSplitRequestWhenSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final ShardSourceReader reader = (ShardSourceReader) createReader(context);

        reader.addSplits(Collections.singletonList(createTestFileSplit("id1")));
        reader.start();
        reader.close();

        assertThat(context.getNumSplitRequests()).isEqualTo(2);
    }

    @Test
    public void testFilterSplitsIfDynamicPartitionPruning()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final TestingReaderContext context = new TestingReaderContext();
        final ShardSourceReader reader = (ShardSourceReader) createReader(context);

        DynamicFilteringData dynamicFilteringData =
                new StaticFileStoreSplitEnumeratorTestBase.MockDynamicFilteringData(
                        org.apache.flink.table.types.logical.RowType.of(
                                new IntType(), new IntType()),
                        new RowData[] {GenericRowData.of(1, 1)});

        RowType partitionRowProjection = RowType.of(DataTypes.INT(), DataTypes.INT());
        List<String> dynamicPartitionFilteringFields = Arrays.asList("f0", "f1");
        DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo =
                new DynamicPartitionFilteringInfo(
                        partitionRowProjection, dynamicPartitionFilteringFields);

        FileStoreSourceSplit splitFromEnumerator =
                FileStoreSourceSplitWithDpp.fromFileStoreSourceSplit(
                        newSourceSplit("id1", row(0, 0), 0, Collections.emptyList()),
                        dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                        dynamicFilteringData);

        List<FileStoreSourceSplit> splitsBelongToThisReader =
                Arrays.asList(
                        newSourceSplit("id1", row(0, 0), 0, Collections.emptyList()),
                        newSourceSplit("id2", row(0, 1), 0, Collections.emptyList()),
                        newSourceSplit("id1", row(1, 0), 0, Collections.emptyList()),
                        newSourceSplit("id2", row(1, 1), 0, Collections.emptyList()));

        Method method =
                reader.getClass()
                        .getDeclaredMethod(
                                "filterSplitsIfDynamicPartitionPruning",
                                FileStoreSourceSplit.class,
                                List.class);
        method.setAccessible(true);
        List<FileStoreSourceSplit> finalSplits =
                (List<FileStoreSourceSplit>)
                        method.invoke(reader, splitFromEnumerator, splitsBelongToThisReader);

        assertThat(finalSplits.size()).isEqualTo(1);
        assertThat(finalSplits).containsExactly(splitsBelongToThisReader.get(3));
    }

    @Test
    public void testFilterSplitsIfDynamicPartitionPruningWithProjection()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final TestingReaderContext context = new TestingReaderContext();
        final ShardSourceReader reader = (ShardSourceReader) createReader(context);

        DynamicFilteringData dynamicFilteringData =
                new StaticFileStoreSplitEnumeratorTestBase.MockDynamicFilteringData(
                        org.apache.flink.table.types.logical.RowType.of(new IntType()),
                        new RowData[] {GenericRowData.of(1)});

        RowType partitionRowProjection = RowType.of(DataTypes.INT(), DataTypes.INT());
        List<String> dynamicPartitionFilteringFields = Collections.singletonList("f0");
        DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo =
                new DynamicPartitionFilteringInfo(
                        partitionRowProjection, dynamicPartitionFilteringFields);

        FileStoreSourceSplit splitFromEnumerator =
                FileStoreSourceSplitWithDpp.fromFileStoreSourceSplit(
                        newSourceSplit("id1", row(0, 0), 0, Collections.emptyList()),
                        dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                        dynamicFilteringData);

        List<FileStoreSourceSplit> splitsBelongToThisReader =
                Arrays.asList(
                        newSourceSplit("id0", row(0, 0), 0, Collections.emptyList()),
                        newSourceSplit("id1", row(0, 1), 0, Collections.emptyList()),
                        newSourceSplit("id2", row(1, 0), 0, Collections.emptyList()),
                        newSourceSplit("id3", row(1, 1), 0, Collections.emptyList()));

        Method method =
                reader.getClass()
                        .getDeclaredMethod(
                                "filterSplitsIfDynamicPartitionPruning",
                                FileStoreSourceSplit.class,
                                List.class);
        method.setAccessible(true);
        List<FileStoreSourceSplit> finalSplits =
                (List<FileStoreSourceSplit>)
                        method.invoke(reader, splitFromEnumerator, splitsBelongToThisReader);

        assertThat(finalSplits.size()).isEqualTo(2);
        assertThat(finalSplits)
                .containsExactlyInAnyOrder(
                        splitsBelongToThisReader.get(2), splitsBelongToThisReader.get(3));
    }

    @Override
    protected FileStoreSourceReader createReader(
            TestingReaderContext context, TableRead tableRead) {

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan tableScan = readBuilder.withShard(0, 3).newScan();

        return new ShardSourceReader(
                context,
                tableRead,
                tableScan,
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                IOManager.create(tempDir.toString()),
                null,
                null);
    }

    private void writeTable() throws Exception {
        try (StreamTableWrite write = table.newWrite(COMMIT_USER);
                StreamTableCommit commit = table.newCommit(COMMIT_USER)) {

            write.write(GenericRow.of(1L, 11L, 101), 0);
            write.write(GenericRow.of(2L, 22L, 202), 1);
            write.write(GenericRow.of(3L, 33L, 303), 2);
            commit.commit(1, write.prepareCommit(true, 0));
        }
    }

    @Test
    public void testReaderOnSplitFinished() throws Exception {}

    @Test
    public void testAddMultipleSplits() throws Exception {}

    @Test
    public void testIOManagerIsSet() throws Exception {}
}
