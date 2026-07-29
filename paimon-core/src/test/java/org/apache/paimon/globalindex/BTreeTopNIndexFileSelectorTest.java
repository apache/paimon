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
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.assertj.core.api.Assertions.assertThat;

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
    public void testUnknownMetadataIsRetained() {
        List<IndexFileMeta> files =
                Arrays.asList(
                        fileWithoutMetadata("unknown"),
                        file("empty", 100, 200, false, 0),
                        file("max-10", 0, 10, false),
                        file("max-20", 10, 20, false));

        assertThat(fileNames(select(files, NULLS_LAST, 1)))
                .containsExactly("unknown", "empty", "max-20");
    }

    @Test
    public void testLimitBoundaries() {
        List<IndexFileMeta> files =
                Arrays.asList(file("max-10", 0, 10, false), file("max-20", 10, 20, false));

        assertThat(select(files, NULLS_LAST, 0)).isEmpty();
        assertThat(select(files, NULLS_LAST, files.size())).containsExactlyElementsOf(files);
    }

    private List<IndexFileMeta> select(
            List<IndexFileMeta> files, SortValue.NullOrdering nullOrdering, int limit) {
        FieldRef fieldRef = new FieldRef(FIELD.id(), FIELD.name(), FIELD.type());
        TopN topN = new TopN(fieldRef, DESCENDING, nullOrdering, limit);
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
        return new IndexFileMeta(
                "btree", fileName, 1, 1, new GlobalIndexMeta(0, 0, FIELD.id(), null, null), null);
    }

    private byte[] serialize(Integer value) {
        return value == null ? null : KEY_SERIALIZER.serialize(value);
    }

    private List<String> fileNames(List<IndexFileMeta> files) {
        return files.stream().map(IndexFileMeta::fileName).collect(Collectors.toList());
    }
}
