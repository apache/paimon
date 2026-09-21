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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.flink.source.FileSplitEnumeratorTestBase;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link AlignedContinuousFileSplitEnumerator} over {@link QueryAuthSplit}. */
public class AlignedContinuousFileSplitEnumeratorQueryAuthTest
        extends FileSplitEnumeratorTestBase<FileStoreSourceSplit> {

    @Test
    public void testWrappedSplitsGroupedBySnapshotLikeTheSplitsTheyWrap() throws Exception {
        List<FileStoreSourceSplit> splits =
                Arrays.asList(snapshotSplit(2, 0), snapshotSplit(1, 1), snapshotSplit(1, 0));

        List<FileStoreSourceSplit> plain = assignFirstSnapshot(splits);
        List<FileStoreSourceSplit> authorized = assignFirstSnapshot(withQueryAuth(splits));

        assertThat(underlying(plain)).containsExactly(splits.get(2).split(), splits.get(1).split());
        assertThat(underlying(authorized)).isEqualTo(underlying(plain));
    }

    @Test
    public void testWrappedSplitIsAssignedStillWrapped() throws Exception {
        List<FileStoreSourceSplit> wrapped =
                withQueryAuth(Collections.singletonList(snapshotSplit(1, 0)));

        List<FileStoreSourceSplit> assigned = assignFirstSnapshot(wrapped);

        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0)).isSameAs(wrapped.get(0));
        assertThat(assigned.get(0).split()).isInstanceOf(QueryAuthSplit.class);
    }

    private List<FileStoreSourceSplit> assignFirstSnapshot(List<FileStoreSourceSplit> splits) {
        TestingSplitEnumeratorContext<FileStoreSourceSplit> context = getSplitEnumeratorContext(2);
        AlignedContinuousFileSplitEnumerator enumerator = enumerator(context);
        enumerator.addSplits(splits);
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(1, "test-host");

        List<FileStoreSourceSplit> assigned = new ArrayList<>();
        new TreeMap<>(context.getSplitAssignments())
                .values()
                .forEach(state -> assigned.addAll(state.getAssignedSplits()));
        return assigned;
    }

    private static AlignedContinuousFileSplitEnumerator enumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context) {
        return new AlignedContinuousFileSplitEnumerator(
                context,
                Collections.emptyList(),
                null,
                Long.MAX_VALUE,
                null,
                false,
                30000L,
                10,
                false,
                -1,
                10);
    }

    private FileStoreSourceSplit snapshotSplit(int snapshotId, int bucket) {
        return createSnapshotSplit(snapshotId, bucket, Collections.emptyList());
    }

    private static FileStoreSourceSplit withQueryAuth(FileStoreSourceSplit split) {
        return new FileStoreSourceSplit(
                split.splitId(),
                new QueryAuthSplit(split.split(), new TableQueryAuthResult(null, null)),
                split.recordsToSkip());
    }

    private static List<FileStoreSourceSplit> withQueryAuth(List<FileStoreSourceSplit> splits) {
        return splits.stream()
                .map(AlignedContinuousFileSplitEnumeratorQueryAuthTest::withQueryAuth)
                .collect(Collectors.toList());
    }

    private static List<Split> underlying(Collection<FileStoreSourceSplit> splits) {
        List<Split> underlying = new ArrayList<>();
        for (FileStoreSourceSplit split : splits) {
            Split inner = split.split();
            underlying.add(
                    inner instanceof QueryAuthSplit ? ((QueryAuthSplit) inner).split() : inner);
        }
        return underlying;
    }

    @Override
    protected FileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files, int... partitions) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(row(partitions))
                        .withBucket(bucket)
                        .withDataFiles(files)
                        .isStreaming(true)
                        .withBucketPath("/temp/xxx")
                        .build(),
                0);
    }
}
