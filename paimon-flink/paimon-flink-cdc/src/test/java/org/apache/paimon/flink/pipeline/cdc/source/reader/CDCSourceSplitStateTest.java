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

package org.apache.paimon.flink.pipeline.cdc.source.reader;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CDCSourceSplitState}. */
public class CDCSourceSplitStateTest {

    @Test
    public void testSchemaChangeEventAdvancesOnlySchemaProgress() {
        CDCSourceSplitState state = new CDCSourceSplitState(newSplit(3L, 1L));

        state.setPosition(
                new RecordAndPosition<Event>(schemaChangeEvent(), RecordAndPosition.NO_OFFSET, 3L));

        assertThat(state.recordsToSkip()).isEqualTo(3L);
        assertThat(state.schemaChangeEventsToSkip()).isEqualTo(2L);
    }

    @Test
    public void testDataChangeEventAdvancesOnlyDataProgress() {
        CDCSourceSplitState state = new CDCSourceSplitState(newSplit(3L, 1L));

        state.setPosition(
                new RecordAndPosition<Event>(dataChangeEvent(), RecordAndPosition.NO_OFFSET, 4L));

        assertThat(state.recordsToSkip()).isEqualTo(4L);
        assertThat(state.schemaChangeEventsToSkip()).isEqualTo(1L);
    }

    @Test
    public void testToSourceSplitPreservesProgress() {
        CDCSourceSplitState state = new CDCSourceSplitState(newSplit(3L, 1L));

        state.setPosition(
                new RecordAndPosition<Event>(schemaChangeEvent(), RecordAndPosition.NO_OFFSET, 3L));
        state.setPosition(
                new RecordAndPosition<Event>(dataChangeEvent(), RecordAndPosition.NO_OFFSET, 4L));

        TableAwareFileStoreSourceSplit split = state.toSourceSplit();
        assertThat(split.recordsToSkip()).isEqualTo(4L);
        assertThat(split.schemaChangeEventsToSkip()).isEqualTo(2L);
    }

    private static TableAwareFileStoreSourceSplit newSplit(
            long recordsToSkip, long schemaChangeEventsToSkip) {
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(row(1))
                        .withBucket(2)
                        .withDataFiles(Arrays.asList(newFile(0), newFile(1)))
                        .isStreaming(false)
                        .rawConvertible(false)
                        .withBucketPath("/temp/2") // not used
                        .build();
        return new TableAwareFileStoreSourceSplit(
                "split-1",
                dataSplit,
                recordsToSkip,
                Identifier.create("test_database", "test_table"),
                1L,
                2L,
                schemaChangeEventsToSkip);
    }

    private static AddColumnEvent schemaChangeEvent() {
        return new AddColumnEvent(
                TableId.tableId("test_database", "test_table"),
                Collections.singletonList(
                        AddColumnEvent.last(Column.physicalColumn("extra", DataTypes.BIGINT()))));
    }

    private static DataChangeEvent dataChangeEvent() {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        new org.apache.flink.cdc.common.types.DataType[] {DataTypes.BIGINT()});
        return DataChangeEvent.insertEvent(
                TableId.tableId("test_database", "test_table"),
                generator.generate(new Object[] {1L}));
    }
}
