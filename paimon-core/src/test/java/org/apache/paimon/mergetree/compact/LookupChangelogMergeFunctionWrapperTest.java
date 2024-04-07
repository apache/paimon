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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.FieldSumAgg;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.types.RowKind.DELETE;
import static org.apache.paimon.types.RowKind.INSERT;
import static org.apache.paimon.types.RowKind.UPDATE_AFTER;
import static org.apache.paimon.types.RowKind.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LookupChangelogMergeFunctionWrapper}. */
public class LookupChangelogMergeFunctionWrapperTest {

    private static final RecordEqualiser EQUALISER =
            (row1, row2) -> row1.getInt(0) == row2.getInt(0);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDeduplicate(boolean changelogRowDeduplicate) {
        Map<InternalRow, KeyValue> highLevel = new HashMap<>();
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                RowType.of(DataTypes.INT()),
                                RowType.of(DataTypes.INT())),
                        highLevel::get,
                        EQUALISER,
                        changelogRowDeduplicate,
                        LookupStrategy.CHANGELOG_ONLY,
                        null);

        // Without level-0
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(1));
        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.changelogs()).isEmpty();
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 record, with level-x (x > 0) record
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(1);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 record, without level-x record, query fail
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, UPDATE_AFTER, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(1);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(INSERT);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 record, without level-x record, query success
        function.reset();
        highLevel.put(row(1), new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(1);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 record, without level-x record, query success but is 'delete'
        function.reset();
        highLevel.put(row(1), new KeyValue().replace(row(1), 1, DELETE, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(1);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(INSERT);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 'delete' record, without level-x record, query fail
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, UPDATE_BEFORE, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.changelogs()).isEmpty();
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 'delete' record, without level-x record, query success
        function.reset();
        highLevel.put(row(1), new KeyValue().replace(row(1), 1, DELETE, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, DELETE, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.changelogs()).isEmpty();
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.valueKind()).isEqualTo(DELETE);
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 'insert' record, with level-x (x > 0) same record
        function.reset();
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(2));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        if (changelogRowDeduplicate) {
            assertThat(changelogs).isEmpty();
        } else {
            assertThat(changelogs).hasSize(2);
            assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
            assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
            assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
            assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        }
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.valueKind()).isEqualTo(INSERT);
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 'insert' record, without level-x (x > 0) record, query success
        function.reset();
        highLevel.put(row(1), new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        if (changelogRowDeduplicate) {
            assertThat(changelogs).isEmpty();
        } else {
            assertThat(changelogs).hasSize(2);
            assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
            assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
            assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
            assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        }
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.valueKind()).isEqualTo(INSERT);
        assertThat(kv.value().getInt(0)).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSum(boolean changelogRowDeduplicate) {
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                projection ->
                                        new AggregateMergeFunction(
                                                new FieldGetter[] {
                                                    row -> row.isNullAt(0) ? null : row.getInt(0)
                                                },
                                                new FieldAggregator[] {
                                                    new FieldSumAgg(DataTypes.INT())
                                                }),
                                RowType.of(DataTypes.INT()),
                                RowType.of(DataTypes.INT())),
                        key -> null,
                        EQUALISER,
                        changelogRowDeduplicate,
                        LookupStrategy.CHANGELOG_ONLY,
                        null);

        // Without level-0
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(1));
        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.changelogs()).isEmpty();
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 record, with level-x (x > 0) record
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(1);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(3);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(3);

        // With level-0 record, with multiple level-x (x > 0) record
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(3));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 3, INSERT, row(2)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 4, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(4);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(4);

        // With level-0 record, with the result is not changed
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(0)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        if (changelogRowDeduplicate) {
            assertThat(changelogs).isEmpty();
        } else {
            assertThat(changelogs).hasSize(2);
            assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
            assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
            assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
            assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        }
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);
    }

    @Test
    public void testFirstRow() {
        Set<InternalRow> highLevel = new HashSet<>();
        FirstRowMergeFunctionWrapper function =
                new FirstRowMergeFunctionWrapper(
                        projection ->
                                new FirstRowMergeFunction(
                                        new RowType(
                                                Lists.list(new DataField(0, "f0", new IntType()))),
                                        new RowType(
                                                Lists.list(new DataField(1, "f1", new IntType())))),
                        highLevel::contains);

        // Without level-0
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(1));
        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.changelogs()).isEmpty();
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(1);

        // With level-0 record, with level-x (x > 0) record
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).isEmpty();
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(1);

        // With level-0 record, with multiple level-x (x > 0) record
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(3));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 3, INSERT, row(2)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 4, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).isEmpty();
        assertThat(kv.value().getInt(0)).isEqualTo(1);

        // Without high level value
        function.reset();
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(0)).setLevel(0));

        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(1);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(INSERT);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(0);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(0);

        // with high level value
        function.reset();
        highLevel.add(row(1));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(0)).setLevel(0));

        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(0);
        kv = result.result();
        assertThat(kv).isNull();
    }
}
