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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test that each {@link MergeFunction} engine preserves the per-record snapshotId of its
 * inputs in {@link MergeFunction#getResult()}. This invariant is required by the snapshot-based
 * sequence ordering path: {@code MergeTreeCompactRewriter.stampSequenceWithSnapshotId} writes the
 * result's snapshotId into the per-record {@code _SEQUENCE_NUMBER} of compacted files; if a merge
 * function returns a result whose snapshotId is {@link KeyValue#UNKNOWN_SNAPSHOT_ID}, downstream
 * reads/compactions of that file treat the records as unknown-snapshot and ordering breaks.
 */
class MergeFunctionSnapshotIdTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .fields(
                            new org.apache.paimon.types.DataType[] {
                                DataTypes.INT(), DataTypes.INT()
                            },
                            new String[] {"k", "v"})
                    .build();
    private static final List<String> PRIMARY_KEYS = Collections.singletonList("k");

    enum Engine {
        DEDUPLICATE,
        FIRST_ROW,
        AGGREGATE,
        PARTIAL_UPDATE
    }

    @ParameterizedTest
    @EnumSource(Engine.class)
    void getResultPreservesLatestSnapshotId(Engine engine) {
        MergeFunction<KeyValue> func = createMergeFunction(engine);
        func.reset();

        // first kv: snapshotId=10
        func.add(kv(1, 1, 100L, RowKind.INSERT, 10L));
        // last kv: snapshotId=20
        func.add(kv(1, 2, 200L, RowKind.INSERT, 20L));

        // FIRST_ROW keeps the first input, all others keep / aggregate-up-to the latest.
        long expected = engine == Engine.FIRST_ROW ? 10L : 20L;
        KeyValue result = func.getResult();
        assertThat(result).as("merge function %s returned null result", engine).isNotNull();
        assertThat(result.snapshotId())
                .as(
                        "merge function %s must preserve snapshotId of the input record it kept;"
                                + " a value of -1 (UNKNOWN_SNAPSHOT_ID) means reused.replace(...)"
                                + " cleared it, which corrupts compaction sequence stamping",
                        engine)
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @EnumSource(Engine.class)
    void getResultPreservesUnknownSnapshotIdWhenInputsHaveNone(Engine engine) {
        MergeFunction<KeyValue> func = createMergeFunction(engine);
        func.reset();

        // Inputs without setSnapshotId — snapshotId defaults to UNKNOWN_SNAPSHOT_ID.
        func.add(kvNoSnapshot(1, 1, 100L, RowKind.INSERT));
        func.add(kvNoSnapshot(1, 2, 200L, RowKind.INSERT));

        KeyValue result = func.getResult();
        assertThat(result).isNotNull();
        assertThat(result.snapshotId())
                .as(
                        "merge function %s should propagate UNKNOWN_SNAPSHOT_ID when no input"
                                + " carries a snapshotId (snapshot-ordering disabled path)",
                        engine)
                .isEqualTo(KeyValue.UNKNOWN_SNAPSHOT_ID);
    }

    private MergeFunction<KeyValue> createMergeFunction(Engine engine) {
        Options options = new Options();
        switch (engine) {
            case DEDUPLICATE:
                return DeduplicateMergeFunction.factory(options).create();
            case FIRST_ROW:
                return FirstRowMergeFunction.factory(options).create();
            case AGGREGATE:
                return AggregateMergeFunction.factory(options, ROW_TYPE, PRIMARY_KEYS).create();
            case PARTIAL_UPDATE:
                return PartialUpdateMergeFunction.factory(options, ROW_TYPE, PRIMARY_KEYS).create();
            default:
                throw new IllegalArgumentException("Unknown engine: " + engine);
        }
    }

    private static KeyValue kv(int key, int value, long sequenceNumber, RowKind kind, long snap) {
        return new KeyValue()
                .replace(GenericRow.of(key), sequenceNumber, kind, GenericRow.of(key, value))
                .setSnapshotId(snap);
    }

    private static KeyValue kvNoSnapshot(int key, int value, long sequenceNumber, RowKind kind) {
        return new KeyValue()
                .replace(GenericRow.of(key), sequenceNumber, kind, GenericRow.of(key, value));
    }
}
