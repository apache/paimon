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
import org.apache.paimon.flink.source.DynamicPartitionFilteringInfo;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.predicate.NullTransform;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link PreAssignSplitAssigner} pruning over {@link QueryAuthSplit}. */
public class PreAssignSplitAssignerTest {

    @Test
    public void testWrappedSplitsPrunedLikeTheSplitsTheyWrap() {
        FileStoreSourceSplit keptOne = partitionSplit(1);
        FileStoreSourceSplit pruned = partitionSplit(2);
        FileStoreSourceSplit keptThree = partitionSplit(3);
        List<FileStoreSourceSplit> splits = Arrays.asList(keptOne, pruned, keptThree);

        SplitAssigner plain = pruningAssigner(splits);
        SplitAssigner authorized = pruningAssigner(withQueryAuth(splits));

        assertThat(underlying(plain.remainingSplits()))
                .containsExactly(keptOne.split(), keptThree.split());
        assertThat(underlying(authorized.remainingSplits()))
                .isEqualTo(underlying(plain.remainingSplits()));
        assertThat(authorized.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());

        List<FileStoreSourceSplit> authorizedNext = authorized.getNext(0, null);
        assertThat(underlying(authorizedNext)).isEqualTo(underlying(plain.getNext(0, null)));
        assertThat(underlying(authorizedNext)).containsExactly(keptOne.split(), keptThree.split());

        assertThat(authorized.getNext(0, null)).isEqualTo(plain.getNext(0, null)).isEmpty();
        assertThat(authorized.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());
    }

    @Test
    public void testWrappedSplitIsKeptStillWrapped() {
        FileStoreSourceSplit wrapped = withQueryAuth(partitionSplit(1));

        SplitAssigner assigner = pruningAssigner(Collections.singletonList(wrapped));

        assertThat(assigner.remainingSplits()).containsExactly(wrapped);
        List<FileStoreSourceSplit> assigned = assigner.getNext(0, null);
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0)).isSameAs(wrapped);
        assertThat(assigned.get(0).split()).isInstanceOf(QueryAuthSplit.class);
    }

    @Test
    public void testWrappedSplitOutsideTheFilterIsNotKept() {
        FileStoreSourceSplit pruned = partitionSplit(2);

        SplitAssigner plain = pruningAssigner(Collections.singletonList(pruned));
        SplitAssigner authorized =
                pruningAssigner(Collections.singletonList(withQueryAuth(pruned)));

        assertThat(plain.remainingSplits()).isEmpty();
        assertThat(authorized.remainingSplits()).isEmpty();
        assertThat(authorized.numberOfRemainingSplits()).isEqualTo(plain.numberOfRemainingSplits());
        assertThat(authorized.getNext(0, null)).isEqualTo(plain.getNext(0, null)).isEmpty();
    }

    @Test
    public void testSplitIsKeptWhenAFilteringFieldIsMasked() {
        FileStoreSourceSplit masked = withColumnMasking(partitionSplit(2), "f0");

        SplitAssigner assigner = pruningAssigner(Collections.singletonList(masked));

        assertThat(assigner.remainingSplits()).containsExactly(masked);
        assertThat(assigner.numberOfRemainingSplits()).isEqualTo(1);
        assertThat(assigner.getNext(0, null)).containsExactly(masked);
    }

    @Test
    public void testSplitIsPrunedWhenTheMaskedColumnIsNotAFilteringField() {
        FileStoreSourceSplit masked = withColumnMasking(partitionSplit(2), "other");

        SplitAssigner assigner = pruningAssigner(Collections.singletonList(masked));

        assertThat(assigner.remainingSplits()).isEmpty();
        assertThat(assigner.numberOfRemainingSplits()).isZero();
        assertThat(assigner.getNext(0, null)).isEmpty();
    }

    private static SplitAssigner pruningAssigner(Collection<FileStoreSourceSplit> splits) {
        DynamicPartitionFilteringInfo filteringInfo =
                new DynamicPartitionFilteringInfo(
                        RowType.of(DataTypes.INT()), Collections.singletonList("f0"));
        DynamicFilteringData filteringData =
                new MockDynamicFilteringData(
                        org.apache.flink.table.types.logical.RowType.of(new IntType()),
                        new RowData[] {GenericRowData.of(1), GenericRowData.of(3)});
        return new PreAssignSplitAssigner(10, 1, splits)
                .ofDynamicPartitionPruning(filteringInfo, filteringData);
    }

    private static FileStoreSourceSplit partitionSplit(int partition) {
        return new FileStoreSourceSplit(
                UUID.randomUUID().toString(),
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(row(partition))
                        .withBucket(0)
                        .withDataFiles(Collections.emptyList())
                        .isStreaming(false)
                        .withBucketPath("/temp/xxx")
                        .build());
    }

    private static FileStoreSourceSplit withQueryAuth(FileStoreSourceSplit split) {
        return new FileStoreSourceSplit(
                split.splitId(),
                new QueryAuthSplit(split.split(), new TableQueryAuthResult(null, null)),
                split.recordsToSkip());
    }

    private static FileStoreSourceSplit withColumnMasking(
            FileStoreSourceSplit split, String column) {
        return new FileStoreSourceSplit(
                split.splitId(),
                new QueryAuthSplit(
                        split.split(),
                        new TableQueryAuthResult(
                                null,
                                Collections.singletonMap(
                                        column, JsonSerdeUtil.toJson(NullTransform.INSTANCE)))),
                split.recordsToSkip());
    }

    private static List<FileStoreSourceSplit> withQueryAuth(List<FileStoreSourceSplit> splits) {
        return splits.stream()
                .map(PreAssignSplitAssignerTest::withQueryAuth)
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

    private static class MockDynamicFilteringData extends DynamicFilteringData {

        private final org.apache.flink.table.types.logical.RowType rowType;
        private final RowData[] neededPartitions;

        public MockDynamicFilteringData(
                org.apache.flink.table.types.logical.RowType rowType, RowData[] neededPartitions) {
            super(new GenericTypeInfo<>(RowData.class), rowType, Collections.emptyList(), true);
            this.rowType = rowType;
            this.neededPartitions = neededPartitions;
        }

        public boolean contains(RowData row) {
            checkArgument(rowType.getFieldCount() == row.getArity());
            for (RowData mayMatch : neededPartitions) {
                if (matchRow(row, mayMatch)) {
                    return true;
                }
            }
            return false;
        }

        private boolean matchRow(RowData row, RowData mayMatch) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (row.getInt(i) != mayMatch.getInt(i)) {
                    return false;
                }
            }
            return true;
        }
    }
}
