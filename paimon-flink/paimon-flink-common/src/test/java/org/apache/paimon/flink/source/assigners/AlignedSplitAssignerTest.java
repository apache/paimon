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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.align.PlaceholderSplit;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link AlignedSplitAssigner} over {@link QueryAuthSplit}. */
public class AlignedSplitAssignerTest {

    @Test
    public void testWrappedSplitsAssignedLikeTheSplitsTheyWrap() {
        List<FileStoreSourceSplit> splits =
                Arrays.asList(snapshotSplit(1, 0), snapshotSplit(1, 1), snapshotSplit(2, 0));

        AlignedSplitAssigner plain = new AlignedSplitAssigner();
        AlignedSplitAssigner authorized = new AlignedSplitAssigner();
        for (int i = 0; i < splits.size(); i++) {
            plain.addSplit(i % 2, splits.get(i));
            authorized.addSplit(i % 2, withQueryAuth(splits.get(i)));
        }

        assertThat(authorized.remainingSnapshots()).isEqualTo(plain.remainingSnapshots());
        assertThat(authorized.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());
        assertThat(authorized.getNextSnapshotId(0)).isEqualTo(plain.getNextSnapshotId(0));
        assertThat(authorized.isAligned()).isEqualTo(plain.isAligned());
        assertThat(underlying(authorized.remainingSplits()))
                .isEqualTo(underlying(plain.remainingSplits()));

        assertThat(underlying(authorized.getNext(0, null)))
                .isEqualTo(underlying(plain.getNext(0, null)));
        assertThat(underlying(authorized.getNext(1, null)))
                .isEqualTo(underlying(plain.getNext(1, null)));
        assertThat(authorized.isAligned()).isEqualTo(plain.isAligned());
        assertThat(authorized.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());
    }

    @Test
    public void testWrappedSplitIsAssignedStillWrapped() {
        FileStoreSourceSplit wrapped = withQueryAuth(snapshotSplit(1, 0));

        AlignedSplitAssigner assigner = new AlignedSplitAssigner();
        assigner.addSplit(0, wrapped);

        assertThat(assigner.remainingSplits()).containsExactly(wrapped);
        List<FileStoreSourceSplit> assigned = assigner.getNext(0, null);
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0)).isSameAs(wrapped);
        assertThat(assigned.get(0).split()).isInstanceOf(QueryAuthSplit.class);
    }

    @Test
    public void testWrappedPlaceholderStillCountsAsAligned() {
        FileStoreSourceSplit placeholder = placeholderSplit(1);

        AlignedSplitAssigner plain = new AlignedSplitAssigner();
        plain.addSplit(0, placeholder);
        AlignedSplitAssigner authorized = new AlignedSplitAssigner();
        authorized.addSplit(0, withQueryAuth(placeholder));

        assertThat(plain.isAligned()).isTrue();
        assertThat(authorized.isAligned()).isEqualTo(plain.isAligned());
        assertThat(authorized.getNext(0, null)).isEqualTo(plain.getNext(0, null));
    }

    @Test
    public void testWrappedSplitsAddedBackIntoTheSnapshotAlreadyPending() {
        FileStoreSourceSplit pending = snapshotSplit(4, 0);
        List<FileStoreSourceSplit> back = Arrays.asList(snapshotSplit(4, 1), snapshotSplit(4, 2));

        AlignedSplitAssigner plain = new AlignedSplitAssigner();
        plain.addSplit(0, pending);
        plain.addSplitsBack(1, back);
        AlignedSplitAssigner authorized = new AlignedSplitAssigner();
        authorized.addSplit(0, withQueryAuth(pending));
        authorized.addSplitsBack(1, withQueryAuth(back));

        assertThat(plain.remainingSnapshots()).isEqualTo(1);
        assertThat(authorized.remainingSnapshots()).isEqualTo(plain.remainingSnapshots());
        assertThat(authorized.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());
        assertThat(underlying(authorized.getNext(1, null)))
                .isEqualTo(underlying(plain.getNext(1, null)));
    }

    @Test
    public void testWrappedSplitsFromDifferentSnapshotsAreStillRejected() {
        List<FileStoreSourceSplit> mixed =
                withQueryAuth(Arrays.asList(snapshotSplit(1, 0), snapshotSplit(2, 0)));

        assertThatThrownBy(() -> new AlignedSplitAssigner().addSplitsBack(0, mixed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SnapshotId not equal");
    }

    @Test
    public void testWrappedPlaceholderAddedBackStillCountsAsAligned() {
        FileStoreSourceSplit placeholder = placeholderSplit(5);

        AlignedSplitAssigner plain = new AlignedSplitAssigner();
        plain.addSplitsBack(0, Collections.singletonList(placeholder));
        AlignedSplitAssigner authorized = new AlignedSplitAssigner();
        authorized.addSplitsBack(0, Collections.singletonList(withQueryAuth(placeholder)));

        assertThat(plain.isAligned()).isTrue();
        assertThat(authorized.isAligned()).isEqualTo(plain.isAligned());
    }

    @Test
    public void testFallbackWrappedSplitsAssignedLikeTheSplitsTheyWrap() {
        List<FileStoreSourceSplit> splits =
                Arrays.asList(snapshotSplit(1, 0), snapshotSplit(1, 1), snapshotSplit(2, 0));

        AlignedSplitAssigner plain = new AlignedSplitAssigner();
        AlignedSplitAssigner nested = new AlignedSplitAssigner();
        for (int i = 0; i < splits.size(); i++) {
            plain.addSplit(i % 2, splits.get(i));
            nested.addSplit(i % 2, withFallback(withQueryAuth(splits.get(i))));
        }

        assertThat(nested.remainingSnapshots()).isEqualTo(plain.remainingSnapshots());
        assertThat(nested.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());
        assertThat(nested.getNextSnapshotId(0)).isEqualTo(plain.getNextSnapshotId(0));
        assertThat(underlying(nested.remainingSplits()))
                .isEqualTo(underlying(plain.remainingSplits()));

        assertThat(underlying(nested.getNext(0, null)))
                .isEqualTo(underlying(plain.getNext(0, null)));
        assertThat(underlying(nested.getNext(1, null)))
                .isEqualTo(underlying(plain.getNext(1, null)));
    }

    @Test
    public void testFallbackWrappedSplitIsAssignedStillFallbackWrapped() {
        FileStoreSourceSplit nested = withFallback(withQueryAuth(snapshotSplit(1, 0)));

        AlignedSplitAssigner assigner = new AlignedSplitAssigner();
        assigner.addSplit(0, nested);

        assertThat(assigner.remainingSplits()).containsExactly(nested);
        List<FileStoreSourceSplit> assigned = assigner.getNext(0, null);
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0)).isSameAs(nested);
        assertThat(assigned.get(0).split()).isInstanceOf(FallbackSplitImpl.class);
    }

    @Test
    public void testFallbackWrappedPlaceholderStillCountsAsAligned() {
        FileStoreSourceSplit placeholder = placeholderSplit(1);

        AlignedSplitAssigner plain = new AlignedSplitAssigner();
        plain.addSplit(0, placeholder);
        AlignedSplitAssigner nested = new AlignedSplitAssigner();
        nested.addSplit(0, withFallback(withQueryAuth(placeholder)));

        assertThat(plain.isAligned()).isTrue();
        assertThat(nested.isAligned()).isEqualTo(plain.isAligned());
    }

    private static FileStoreSourceSplit snapshotSplit(long snapshotId, int bucket) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(row(1))
                        .withBucket(bucket)
                        .withDataFiles(Collections.emptyList())
                        .isStreaming(true)
                        .withBucketPath("/temp/xxx") // not used
                        .build());
    }

    private static FileStoreSourceSplit placeholderSplit(long snapshotId) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(), new PlaceholderSplit(snapshotId));
    }

    private static FileStoreSourceSplit withQueryAuth(FileStoreSourceSplit split) {
        return new FileStoreSourceSplit(
                split.splitId(),
                new QueryAuthSplit(split.split(), new TableQueryAuthResult(null, null)),
                split.recordsToSkip());
    }

    private static List<FileStoreSourceSplit> withQueryAuth(List<FileStoreSourceSplit> splits) {
        return splits.stream()
                .map(AlignedSplitAssignerTest::withQueryAuth)
                .collect(Collectors.toList());
    }

    private static FileStoreSourceSplit withFallback(FileStoreSourceSplit split) {
        return new FileStoreSourceSplit(
                split.splitId(),
                FallbackReadFileStoreTable.toFallbackSplit(split.split(), true),
                split.recordsToSkip());
    }

    private static List<Split> underlying(Collection<FileStoreSourceSplit> splits) {
        List<Split> underlying = new ArrayList<>();
        for (FileStoreSourceSplit split : splits) {
            Split inner = split.split();
            if (inner instanceof FallbackSplitImpl) {
                inner = ((FallbackSplitImpl) inner).wrapped();
            }
            underlying.add(
                    inner instanceof QueryAuthSplit ? ((QueryAuthSplit) inner).split() : inner);
        }
        return underlying;
    }
}
