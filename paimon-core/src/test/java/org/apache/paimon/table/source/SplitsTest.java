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

package org.apache.paimon.table.source;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackDataSplit;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplit;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplitImpl;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Splits}. */
public class SplitsTest {

    @Test
    public void testDataSplitIsItsOwnUnderlyingSplit() {
        DataSplit split = dataSplit();

        assertThat(Splits.underlying(split)).isSameAs(split);
    }

    @Test
    public void testAuthorizationWrapperIsLookedThrough() {
        DataSplit split = dataSplit();

        assertThat(Splits.underlying(withQueryAuth(split))).isSameAs(split);
    }

    @Test
    public void testFallbackWrapperAroundAuthorizationWrapperIsLookedThrough() {
        DataSplit split = dataSplit();
        Split nested = toFallbackSplit(withQueryAuth(split));

        assertThat(nested).isInstanceOf(FallbackSplitImpl.class);
        assertThat(((FallbackSplit) nested).wrapped()).isInstanceOf(QueryAuthSplit.class);
        assertThat(Splits.underlying(nested)).isSameAs(split);
    }

    @Test
    public void testFallbackDataSplitIsItsOwnUnderlyingSplit() {
        DataSplit split = dataSplit();
        Split fallback = toFallbackSplit(split);

        assertThat(fallback).isInstanceOf(FallbackDataSplit.class);
        assertThat(Splits.underlying(fallback)).isSameAs(fallback);
    }

    @Test
    public void testSplitThatIsNeitherADataSplitNorAWrapperIsItsOwnUnderlyingSplit() {
        ChainSplit split = chainSplit();

        assertThat(Splits.underlying(split)).isSameAs(split);
    }

    @Test
    public void testFallbackWrapperAroundASplitThatIsNotADataSplitIsLookedThrough() {
        ChainSplit split = chainSplit();

        assertThat(Splits.underlying(toFallbackSplit(split))).isSameAs(split);
    }

    @Test
    public void testAuthorizationWrapperAroundASplitThatIsNotADataSplitIsLookedThrough() {
        ChainSplit split = chainSplit();

        assertThat(Splits.underlying(withQueryAuth(split))).isSameAs(split);
    }

    @Test
    public void testAuthorizationWrapperAroundAFallbackDataSplitStopsAtTheFallbackDataSplit() {
        Split fallback = toFallbackSplit(dataSplit());

        assertThat(Splits.underlying(withQueryAuth(fallback))).isSameAs(fallback);
    }

    @Test
    public void testSplitCarryingNoAuthorizationHasNoAuthResult() {
        assertThat(Splits.authResult(dataSplit())).isNull();
        assertThat(Splits.authResult(chainSplit())).isNull();
        assertThat(Splits.authResult(toFallbackSplit(dataSplit()))).isNull();
        assertThat(Splits.authResult(toFallbackSplit(chainSplit()))).isNull();
    }

    @Test
    public void testAuthResultIsReadThroughTheAuthorizationWrapper() {
        TableQueryAuthResult authResult = authResult();

        assertThat(Splits.authResult(new QueryAuthSplit(dataSplit(), authResult)))
                .isSameAs(authResult);
    }

    @Test
    public void testAuthResultIsReadThroughAFallbackWrapper() {
        TableQueryAuthResult authResult = authResult();
        Split nested = toFallbackSplit(new QueryAuthSplit(dataSplit(), authResult));

        assertThat(Splits.authResult(nested)).isSameAs(authResult);
    }

    @Test
    public void testAuthResultIsReadFromAWrapperAroundAFallbackDataSplit() {
        TableQueryAuthResult authResult = authResult();
        Split split = new QueryAuthSplit(toFallbackSplit(dataSplit()), authResult);

        assertThat(Splits.authResult(split)).isSameAs(authResult);
    }

    @Test
    public void testAuthorizationWrapperWithoutRulesHasNoAuthResult() {
        assertThat(Splits.authResult(new QueryAuthSplit(dataSplit(), null))).isNull();
    }

    private static TableQueryAuthResult authResult() {
        return new TableQueryAuthResult(
                null, Collections.singletonMap("name", "{\"name\":\"NULL\"}"));
    }

    private static DataSplit dataSplit() {
        return DataSplit.builder()
                .withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Collections.singletonList(newFile(0L, 1L)))
                .build();
    }

    private static ChainSplit chainSplit() {
        return new ChainSplit(
                BinaryRow.EMPTY_ROW,
                Collections.singletonList(newFile(0L, 1L)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null);
    }

    private static Split withQueryAuth(Split split) {
        return new QueryAuthSplit(split, new TableQueryAuthResult(null, null));
    }

    private static Split toFallbackSplit(Split split) {
        return FallbackReadFileStoreTable.toFallbackSplit(split, true);
    }
}
