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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldSumAggFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
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
                                DeduplicateMergeFunction.factory(), null, null, null),
                        highLevel::get,
                        changelogRowDeduplicate ? EQUALISER : null,
                        LookupStrategy.from(false, true, false, false),
                        null,
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
        highLevel.clear();
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

    @Test
    public void testDeduplicateWithIgnoreFields() {
        Map<InternalRow, KeyValue> highLevel = new HashMap<>();
        RowType valueType =
                RowType.builder()
                        .fields(
                                new DataType[] {DataTypes.INT(), DataTypes.INT()},
                                new String[] {"f0", "f1"})
                        .build();
        UserDefinedSeqComparator userDefinedSeqComparator =
                UserDefinedSeqComparator.create(
                        valueType, CoreOptions.fromMap(ImmutableMap.of("sequence.field", "f1")));
        assert userDefinedSeqComparator != null;
        List<String> ignoreFields = Collections.singletonList("f1");
        ValueEqualiserSupplier logDedupEqualSupplier =
                ValueEqualiserSupplier.fromIgnoreFields(valueType, ignoreFields);
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(), null, null, null),
                        highLevel::get,
                        logDedupEqualSupplier.get(),
                        LookupStrategy.from(false, true, false, false),
                        null,
                        userDefinedSeqComparator);

        // With level-0 'insert' record, with level-x (x > 0) same record. Notice that the specified
        // ignored
        // fields in records are different.
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1, 1)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(1, 2)).setLevel(0));
        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).isEmpty();
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.valueKind()).isEqualTo(INSERT);
        assertThat(kv.value().getInt(0)).isEqualTo(1);
        assertThat(kv.value().getInt(1)).isEqualTo(2);

        // With level-0 'insert' record, with level-x (x > 0) different record.
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1, 1)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2, 2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(1);
        assertThat(changelogs.get(0).value().getInt(1)).isEqualTo(1);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        assertThat(changelogs.get(1).value().getInt(1)).isEqualTo(2);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.valueKind()).isEqualTo(INSERT);
        assertThat(kv.value().getInt(0)).isEqualTo(2);
        assertThat(kv.value().getInt(1)).isEqualTo(2);
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
                                                    new FieldSumAggFactory()
                                                            .create(DataTypes.INT(), null, null)
                                                },
                                                false,
                                                null),
                                null,
                                null,
                                null),
                        key -> null,
                        changelogRowDeduplicate ? EQUALISER : null,
                        LookupStrategy.from(false, true, false, false),
                        null,
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
    public void testMergeHighLevelOrder() {
        Map<InternalRow, KeyValue> highLevel = new HashMap<>();
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                projection ->
                                        new AggregateMergeFunction(
                                                new FieldGetter[] {
                                                    row -> row.isNullAt(0) ? null : row.getInt(0)
                                                },
                                                new FieldAggregator[] {
                                                    new FieldLastValueAggFactory()
                                                            .create(DataTypes.INT(), null, null)
                                                },
                                                false,
                                                null),
                                null,
                                null,
                                null),
                        highLevel::get,
                        null,
                        LookupStrategy.from(false, true, false, false),
                        null,
                        UserDefinedSeqComparator.create(
                                RowType.builder().field("f0", DataTypes.INT()).build(),
                                CoreOptions.fromMap(ImmutableMap.of("sequence.field", "f0"))));

        // Only level-0 record and find record of higher sequence.field value in high level.
        highLevel.put(row(1), new KeyValue().replace(row(1), 1, INSERT, row(3)).setLevel(3));
        function.reset();
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();
        List<KeyValue> changelogs = result.changelogs();
        // 3 -> 3
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(3);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(3);
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(3);

        // Only level-0 record and find record of lower sequence.field value in high level.
        highLevel.put(row(1), new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(3));
        function.reset();
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(2)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        // 1 -> 2
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(1);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(2);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // Only level-0 record and find record of middle sequence.field value in high level.
        // 1 2 3
        highLevel.put(row(1), new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(3));
        function.reset();
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(1)).setLevel(0));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(3)).setLevel(0));
        result = function.getResult();
        assertThat(result).isNotNull();
        changelogs = result.changelogs();
        // 2 -> 3
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(2);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(3);
        kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(3);
    }

    @Test
    public void testFirstRow() {
        Set<InternalRow> highLevel = new HashSet<>();
        FirstRowMergeFunctionWrapper function =
                new FirstRowMergeFunctionWrapper(
                        projection -> new FirstRowMergeFunction(false), highLevel::contains);

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

    @Test
    public void testKeepLowestHighLevel() {
        Map<InternalRow, KeyValue> highLevel = new HashMap<>();
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(), null, null, null),
                        highLevel::get,
                        null,
                        LookupStrategy.from(false, true, false, false),
                        null,
                        null);

        // Without level-0
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.changelogs()).isEmpty();
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);

        // With level-0 record, with multiple level-x (x > 0) record
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(3)).setLevel(0));
        result = function.getResult();
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(1);
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(3);
        kv = result.result();
        assertThat(kv.value().getInt(0)).isEqualTo(3);
    }

    /**
     * Test that sequence.field is correctly used to pick the high level record with the highest
     * sequence value, even when it's at a higher level number.
     *
     * <p>Scenario: L1 has older sequence (7), L2 has newer sequence (8), L0 has oldest (6). The
     * correct behavior should pick L2 (sequence=8) as the high level record.
     */
    @Test
    public void testSequenceFieldWithMultipleLevels() {
        // Define value type with sequence field as the second column
        RowType valueType =
                RowType.builder()
                        .fields(
                                new DataType[] {DataTypes.INT(), DataTypes.INT()},
                                new String[] {"value", "sequence"})
                        .build();

        // Create user-defined sequence comparator on the second field
        UserDefinedSeqComparator userDefinedSeqComparator =
                UserDefinedSeqComparator.create(
                        valueType,
                        CoreOptions.fromMap(ImmutableMap.of("sequence.field", "sequence")));
        assertThat(userDefinedSeqComparator).isNotNull();

        Map<InternalRow, KeyValue> highLevel = new HashMap<>();

        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(), null, null, null),
                        highLevel::get,
                        null,
                        LookupStrategy.from(false, true, false, false),
                        null,
                        userDefinedSeqComparator);

        // Test scenario:
        // L1: (key=1, value=100, sequence=7)  <- Level 1, but older sequence
        // L2: (key=1, value=200, sequence=8)  <- Level 2, but newer sequence (should be picked!)
        // L0: (key=1, value=50,  sequence=6)  <- Level 0, oldest sequence

        function.reset();
        function.add(
                new KeyValue()
                        .replace(row(1), 1, INSERT, row(100, 7))
                        .setLevel(1)); // Level 1, seq=7
        function.add(
                new KeyValue()
                        .replace(row(1), 1, INSERT, row(200, 8))
                        .setLevel(2)); // Level 2, seq=8
        function.add(
                new KeyValue()
                        .replace(row(1), 2, INSERT, row(50, 6))
                        .setLevel(0)); // Level 0, seq=6

        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();

        KeyValue kv = result.result();
        assertThat(kv).isNotNull();

        // Should return the record with highest sequence (seq=8 from L2)
        int actualSequence = kv.value().getInt(1);
        int actualValue = kv.value().getInt(0);

        assertThat(actualSequence)
                .as("Should return record with highest sequence field (8)")
                .isEqualTo(8);
        assertThat(actualValue).isEqualTo(200);

        // Verify changelog: before should be L2 (seq=8), after should be merged result
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).valueKind()).isEqualTo(UPDATE_BEFORE);
        assertThat(changelogs.get(0).value().getInt(1)).isEqualTo(8); // before is L2
        assertThat(changelogs.get(1).valueKind()).isEqualTo(UPDATE_AFTER);
    }

    /**
     * Test that without sequence.field, the original behavior is preserved: pick the record with
     * the lowest level number.
     */
    @Test
    public void testWithoutSequenceFieldPreservesOriginalBehavior() {
        Map<InternalRow, KeyValue> highLevel = new HashMap<>();

        // No userDefinedSeqComparator (null)
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(), null, null, null),
                        highLevel::get,
                        null,
                        LookupStrategy.from(false, true, false, false),
                        null,
                        null); // No sequence comparator

        // L1: value=100, L2: value=200
        // Without sequence.field, should pick L1 (level 1 < level 2)
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(100)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(200)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(50)).setLevel(0));

        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();

        // Without sequence.field, L1 is picked as highLevel, and L0 is the latest
        // So the result should be L0's value (50) since DeduplicateMergeFunction keeps the last
        KeyValue kv = result.result();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(50);

        // Changelog: before=L1(100), after=L0(50)
        List<KeyValue> changelogs = result.changelogs();
        assertThat(changelogs).hasSize(2);
        assertThat(changelogs.get(0).value().getInt(0)).isEqualTo(100); // before is L1
        assertThat(changelogs.get(1).value().getInt(0)).isEqualTo(50); // after is L0
    }

    /**
     * Test sequence.field with descending sort order. When sort-order=descending, smaller sequence
     * values are considered "newer".
     *
     * <p>Note: We use a 3-field schema with sequence at index 2 to avoid cache collision with other
     * tests, because CodeGenUtils caches comparators by field types and indices without considering
     * sort order.
     */
    @Test
    public void testSequenceFieldWithDescendingSortOrder() {
        RowType valueType =
                RowType.builder()
                        .fields(
                                new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                                new String[] {"value", "extra", "sequence"})
                        .build();

        // Create comparator with descending order
        UserDefinedSeqComparator userDefinedSeqComparator =
                UserDefinedSeqComparator.create(
                        valueType,
                        CoreOptions.fromMap(
                                ImmutableMap.of(
                                        "sequence.field", "sequence",
                                        "sequence.field.sort-order", "descending")));
        assertThat(userDefinedSeqComparator).isNotNull();

        Map<InternalRow, KeyValue> highLevel = new HashMap<>();

        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(), null, null, null),
                        highLevel::get,
                        null,
                        LookupStrategy.from(false, true, false, false),
                        null,
                        userDefinedSeqComparator);

        // With descending order, smaller sequence = newer
        // L1: (key=1, value=100, extra=0, sequence=7)  <- Level 1, newer (7 < 8)
        // L2: (key=1, value=200, extra=0, sequence=8)  <- Level 2, older (8 > 7)
        // L0: (key=1, value=50,  extra=0, sequence=9)  <- Level 0, oldest (9 > 8 > 7)

        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(100, 0, 7)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(200, 0, 8)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 2, INSERT, row(50, 0, 9)).setLevel(0));

        ChangelogResult result = function.getResult();
        assertThat(result).isNotNull();

        KeyValue kv = result.result();
        assertThat(kv).isNotNull();

        int actualSequence = kv.value().getInt(2);
        int actualValue = kv.value().getInt(0);

        // With descending order, L1 (seq=7) is the newest high-level record
        // The result should be L1's value (100) since it's the newest
        assertThat(actualSequence)
                .as("With descending order, should return record with smallest sequence (7)")
                .isEqualTo(7);
        assertThat(actualValue).isEqualTo(100);
    }
}
