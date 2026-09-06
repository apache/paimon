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

package org.apache.paimon.globalindex;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BTreeTopNIndexFileSelector}. */
public class BTreeTopNIndexFileSelectorTest {

    private static final DataField FIELD = new DataField(1, "score", DataTypes.INT());
    private static final KeySerializer KEY_SERIALIZER = KeySerializer.create(FIELD.type());

    @Test
    public void testSelectDescendingNullsLast() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("all-null", null, null, true),
                        file("max-20", 10, 20, false),
                        file("max-40", 30, 40, false),
                        file("max-30", 20, 30, true));

        assertThat(fileNames(select(files, NULLS_LAST, 2))).containsExactly("max-40", "max-30");
    }

    @Test
    public void testSelectDescendingNullsFirst() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("nonnull", 100, 1000, false),
                        file("null-b", null, null, true),
                        file("null-a", 10, 20, true));

        assertThat(fileNames(select(files, NULLS_FIRST, 2))).containsExactly("null-a", "null-b");
    }

    @Test
    public void testSelectAscendingNullsLast() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("all-null", null, null, true),
                        file("min-10", 10, 20, false),
                        file("min-30", 30, 40, false),
                        file("min-20", 20, 30, true));

        assertThat(fileNames(select(files, ASCENDING, NULLS_LAST, 2)))
                .containsExactly("min-10", "min-20");
    }

    @Test
    public void testSelectAscendingNullsFirst() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("nonnull", 1, 100, false),
                        file("null-b", null, null, true),
                        file("null-a", 10, 20, true));

        assertThat(fileNames(select(files, ASCENDING, NULLS_FIRST, 2)))
                .containsExactly("null-a", "null-b");
    }

    @Test
    public void testAscendingSingleFileCoversTopNAtEqualBoundary() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("bottom", 0, 10, false, 100),
                        file("next", 10, 20, false, 100),
                        file("upper", 20, 30, false, 100));

        assertThat(fileNames(select(files, ASCENDING, NULLS_LAST, 100))).containsExactly("bottom");
    }

    @Test
    public void testMissingMetadataFailsFast() {
        assertThatThrownBy(
                        () -> select(Arrays.asList(fileWithoutMetadata("missing")), NULLS_LAST, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("BTree index file 'missing' is missing sorted metadata.");
    }

    @Test
    public void testCorruptMetadataFailsFast() {
        assertThatThrownBy(
                        () ->
                                select(
                                        Arrays.asList(
                                                fileWithMetadata(
                                                        "corrupt", new byte[] {-1, -1, -1, -1})),
                                        NULLS_LAST,
                                        1))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testSingleFileCoversTopNAtEqualBoundary() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("top", 90, 100, false, 100),
                        file("next", 80, 90, false, 100),
                        file("lower", 70, 80, false, 100));

        assertThat(fileNames(select(files, NULLS_LAST, 100))).containsExactly("top");
    }

    @Test
    public void testDoesNotStopWhenSingleFileHasTooFewRows() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("top", 90, 100, false, 99),
                        file("next", 80, 89, false, 100),
                        file("lower", 70, 79, false, 100));

        assertThat(fileNames(select(files, NULLS_LAST, 100))).containsExactly("top", "next");
    }

    @Test
    public void testDoesNotStopAtOverlappingRange() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("top", 0, 100, false, 100),
                        file("next", 90, 99, false, 99),
                        file("lower", 80, 89, false, 99));

        assertThat(fileNames(select(files, NULLS_LAST, 100)))
                .containsExactly("top", "next", "lower");
    }

    @Test
    public void testNullsLastPreventsWholeFileCoverage() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        file("top", 90, 100, true, 100),
                        file("next", 80, 89, false, 100),
                        file("lower", 70, 79, false, 100));

        assertThat(fileNames(select(files, NULLS_LAST, 100))).containsExactly("top", "next");
    }

    @Test
    public void testLimitBoundaries() {
        List<IndexFileMeta> files =
                Arrays.asList(file("max-10", 0, 10, false), file("max-20", 10, 20, false));

        assertThat(select(files, NULLS_LAST, 0)).isEmpty();
        assertThat(fileNames(select(files, NULLS_LAST, files.size())))
                .containsExactly("max-20", "max-10");
    }

    private List<IndexFileMeta> select(
            List<IndexFileMeta> files, SortValue.NullOrdering nullOrdering, int limit) {
        return select(files, DESCENDING, nullOrdering, limit);
    }

    private List<IndexFileMeta> select(
            List<IndexFileMeta> files,
            SortValue.SortDirection direction,
            SortValue.NullOrdering nullOrdering,
            int limit) {
        FieldRef fieldRef = new FieldRef(FIELD.id(), FIELD.name(), FIELD.type());
        TopN topN = new TopN(fieldRef, direction, nullOrdering, limit);
        return BTreeTopNIndexFileSelector.select(files, FIELD, topN);
    }

    private IndexFileMeta file(
            String fileName, Integer firstKey, Integer lastKey, boolean hasNulls) {
        return file(fileName, firstKey, lastKey, hasNulls, 1);
    }

    private IndexFileMeta file(
            String fileName, Integer firstKey, Integer lastKey, boolean hasNulls, long rowCount) {
        SortedIndexFileMeta sortedMeta =
                new SortedIndexFileMeta(serialize(firstKey), serialize(lastKey), hasNulls);
        return new IndexFileMeta(
                "btree",
                fileName,
                1,
                rowCount,
                new GlobalIndexMeta(0, 0, FIELD.id(), null, sortedMeta.serialize()),
                null);
    }

    private IndexFileMeta fileWithoutMetadata(String fileName) {
        return fileWithMetadata(fileName, null);
    }

    private IndexFileMeta fileWithMetadata(String fileName, byte[] metadata) {
        return new IndexFileMeta(
                "btree",
                fileName,
                1,
                1,
                new GlobalIndexMeta(0, 0, FIELD.id(), null, metadata),
                null);
    }

    private byte[] serialize(Integer value) {
        return value == null ? null : KEY_SERIALIZER.serialize(value);
    }

    private List<String> fileNames(List<IndexFileMeta> files) {
        return files.stream().map(IndexFileMeta::fileName).collect(Collectors.toList());
    }
}
