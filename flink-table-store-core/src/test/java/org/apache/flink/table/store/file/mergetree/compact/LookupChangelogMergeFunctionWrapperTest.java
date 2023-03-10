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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.InternalRow.FieldGetter;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.aggregate.FieldAggregator;
import org.apache.flink.table.store.file.mergetree.compact.aggregate.FieldSumAgg;
import org.apache.flink.table.store.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.store.file.io.DataFileTestUtils.row;
import static org.apache.flink.table.store.types.RowKind.DELETE;
import static org.apache.flink.table.store.types.RowKind.INSERT;
import static org.apache.flink.table.store.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.table.store.types.RowKind.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LookupChangelogMergeFunctionWrapper}. */
public class LookupChangelogMergeFunctionWrapperTest {

    @Test
    public void testDeduplicate() {
        Map<InternalRow, KeyValue> highLevel = new HashMap<>();
        LookupChangelogMergeFunctionWrapper function =
                new LookupChangelogMergeFunctionWrapper(
                        LookupMergeFunction.wrap(DeduplicateMergeFunction.factory()),
                        highLevel::get);

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
    }

    @Test
    public void testSum() {
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
                                                })),
                        key -> null);

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
    }
}
