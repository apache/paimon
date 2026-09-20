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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DeleteRowFieldsProjector}. */
class DeleteRowFieldsProjectorTest {

    @Test
    void testRequiredFieldProjection() {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "seq");
        options.set(CoreOptions.RECORD_LEVEL_TIME_FIELD, "expire");
        options.set(CoreOptions.CLUSTERING_COLUMNS, "cluster");
        options.set(CoreOptions.PK_VECTOR_INDEX_COLUMNS, "vector_index");
        options.set(CoreOptions.PK_BTREE_INDEX_COLUMNS, "btree_index");
        options.set(CoreOptions.PK_BITMAP_INDEX_COLUMNS, "bitmap_index");
        options.set(CoreOptions.PK_MULTIVALUE_INDEX_COLUMNS, "multivalue_index");
        options.set(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS, "fulltext_index");
        options.set(CoreOptions.PK_FM_INDEX_COLUMNS, "fm_index");

        assertThat(
                        DeleteRowFieldsProjector.createProjection(
                                tableSchema(), new CoreOptions(options)))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1);
    }

    @Test
    void testOnlyWholeRecordDeletesAreProjected() {
        assertThat(createProjection(new Options())).isNotNull();

        Options partialUpdate = new Options();
        partialUpdate.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        assertThat(createProjection(partialUpdate)).isNull();
        partialUpdate.set(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE, true);
        assertThat(createProjection(partialUpdate)).isNotNull();

        Options aggregation = new Options();
        aggregation.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.AGGREGATE);
        assertThat(createProjection(aggregation)).isNull();
        aggregation.set(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE, true);
        assertThat(createProjection(aggregation)).isNotNull();

        Options firstRow = new Options();
        firstRow.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.FIRST_ROW);
        assertThat(createProjection(firstRow)).isNull();
    }

    @Test
    void testProjectionDoesNotChangeInput() {
        GenericRow value =
                GenericRow.of(
                        1,
                        "required",
                        2L,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        "payload");
        KeyValue input = new KeyValue().replace(GenericRow.of(1), 2L, RowKind.DELETE, value);

        KeyValue projected =
                new DeleteRowFieldsProjector(createProjection(new Options())).project(input);

        assertThat(projected.key()).isSameAs(input.key());
        assertThat(projected.sequenceNumber()).isEqualTo(2L);
        assertThat(projected.valueKind()).isEqualTo(RowKind.DELETE);
        assertThat(projected.value().isNullAt(1)).isFalse();
        assertThat(projected.value().isNullAt(11)).isTrue();
        assertThat(value.isNullAt(11)).isFalse();
    }

    private int[] createProjection(Options options) {
        return DeleteRowFieldsProjector.createProjection(tableSchema(), new CoreOptions(options));
    }

    private TableSchema tableSchema() {
        return new TableSchema(
                0,
                Arrays.asList(
                        new DataField(0, "key", DataTypes.INT()),
                        new DataField(1, "not_null", DataTypes.STRING().notNull()),
                        new DataField(2, "seq", DataTypes.BIGINT()),
                        new DataField(3, "expire", DataTypes.BIGINT()),
                        new DataField(4, "cluster", DataTypes.STRING()),
                        new DataField(5, "vector_index", DataTypes.STRING()),
                        new DataField(6, "btree_index", DataTypes.STRING()),
                        new DataField(7, "bitmap_index", DataTypes.STRING()),
                        new DataField(8, "multivalue_index", DataTypes.STRING()),
                        new DataField(9, "fulltext_index", DataTypes.STRING()),
                        new DataField(10, "fm_index", DataTypes.STRING()),
                        new DataField(11, "payload", DataTypes.STRING())),
                11,
                Collections.emptyList(),
                Collections.singletonList("key"),
                Collections.emptyMap(),
                "");
    }
}
