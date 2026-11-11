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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.types.RowKind.DELETE;
import static org.apache.paimon.types.RowKind.INSERT;
import static org.assertj.core.api.Assertions.assertThat;

class LookupMergeFunctionTest {

    private static final RowType KEY_TYPE =
            DataTypes.ROW(DataTypes.FIELD(0, "_KEY_id", DataTypes.INT()));

    @TempDir Path tempDir;

    @Test
    public void testKeepLowestHighLevel() {
        LookupMergeFunction function =
                (LookupMergeFunction)
                        LookupMergeFunction.wrap(
                                        DeduplicateMergeFunction.factory(), null, null, null)
                                .create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        KeyValue kv = function.getResult();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);
    }

    @Test
    public void testLevelNegative() {
        LookupMergeFunction function =
                (LookupMergeFunction)
                        LookupMergeFunction.wrap(
                                        DeduplicateMergeFunction.factory(), null, null, null)
                                .create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(-1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(-1));
        KeyValue kv = function.getResult();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(1);
    }

    /**
     * Same scenario as {@link #testKeepLowestHighLevel()}, but with the candidates spilled: every
     * iteration over a spilled buffer deserializes fresh instances, so an identity check against
     * the record picked by a previous iteration can never match.
     */
    @Test
    public void testKeepLowestHighLevelWhenCandidatesHaveSpilled() {
        for (boolean withIoManager : new boolean[] {false, true}) {
            LookupMergeFunction function = spillingFunction(withIoManager);
            function.reset();
            function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(1));
            function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
            KeyValue kv = function.getResult();
            assertThat(kv).as("spilled, ioManager=%s", withIoManager).isNotNull();
            assertThat(kv.value().getInt(0)).isEqualTo(2);
        }
    }

    /**
     * The lowest high level record is not simply the first or the last one, so this also covers the
     * position bookkeeping rather than only "some high level record was merged".
     */
    @Test
    public void testPicksTheLowestHighLevelFromTheMiddleWhenCandidatesHaveSpilled() {
        for (boolean withIoManager : new boolean[] {false, true}) {
            LookupMergeFunction function = spillingFunction(withIoManager);
            function.reset();
            function.add(new KeyValue().replace(row(1), 1, INSERT, row(30)).setLevel(3));
            function.add(new KeyValue().replace(row(1), 2, INSERT, row(10)).setLevel(1));
            function.add(new KeyValue().replace(row(1), 3, INSERT, row(20)).setLevel(2));
            KeyValue kv = function.getResult();
            assertThat(kv).as("spilled, ioManager=%s", withIoManager).isNotNull();
            assertThat(kv.value().getInt(0)).isEqualTo(10);
        }
    }

    private LookupMergeFunction spillingFunction(boolean withIoManager) {
        Options options = new Options();
        // spill as soon as there is more than one candidate for the key
        options.set(CoreOptions.LOOKUP_MERGE_RECORDS_THRESHOLD, 1);
        RowType keyType = RowType.builder().field("k", DataTypes.INT()).build();
        RowType valueType = RowType.builder().field("v", DataTypes.INT()).build();
        LookupMergeFunction.Factory factory =
                (LookupMergeFunction.Factory)
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                new CoreOptions(options),
                                keyType,
                                valueType);
        @Nullable IOManager ioManager = withIoManager ? IOManager.create(tempDir.toString()) : null;
        factory.withIOManager(ioManager);
        return (LookupMergeFunction) factory.create();
    }

    @Test
    public void testSpillWithProjectedReadType() throws Exception {
        RowType valueType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "payload", DataTypes.INT()),
                        DataTypes.FIELD(2, "items", DataTypes.ARRAY(DataTypes.INT())));
        InternalRow result =
                mergeWithSpill(
                        valueType,
                        valueType.project("items", "id"),
                        GenericRow.of(new GenericArray(new int[] {10, 20}), 1),
                        INSERT);

        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getArray(0).toIntArray()).containsExactly(10, 20);
        assertThat(result.getInt(1)).isEqualTo(1);
    }

    @Test
    public void testSpillWithNotNullReadType() throws Exception {
        RowType valueType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(1, "payload", DataTypes.INT()));
        RowType readType = DataTypes.ROW(valueType.getField(0), valueType.getField(1).copy(false));

        InternalRow result = mergeWithSpill(valueType, readType, GenericRow.of(1, null), DELETE);

        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(readType.getTypeAt(1).isNullable()).isFalse();
    }

    @Test
    public void testSpillWithNestedReadType() throws Exception {
        RowType nestedType =
                DataTypes.ROW(
                        DataTypes.FIELD(2, "items", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD(3, "payload", DataTypes.INT().notNull()));
        RowType valueType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "nested", nestedType));
        RowType readType =
                DataTypes.ROW(
                        valueType.getField(0),
                        DataTypes.FIELD(1, "nested", nestedType.project("payload").notNull()));

        InternalRow result =
                mergeWithSpill(valueType, readType, GenericRow.of(1, GenericRow.of(42)), INSERT);

        assertThat(result.getFieldCount()).isEqualTo(2);
        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getRow(1, 1).getInt(0)).isEqualTo(42);
    }

    private InternalRow mergeWithSpill(
            RowType valueType, RowType readType, InternalRow value, RowKind valueKind)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.LOOKUP_MERGE_RECORDS_THRESHOLD, 1);
        LookupMergeFunction.Factory factory =
                (LookupMergeFunction.Factory)
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                new CoreOptions(options),
                                KEY_TYPE,
                                valueType);
        try (IOManager ioManager = new IOManagerImpl(tempDir.toString())) {
            factory.withIOManager(ioManager);
            MergeFunction<KeyValue> function = factory.create(readType);
            function.reset();
            try {
                function.add(new KeyValue().replace(row(1), 1, valueKind, value).setLevel(0));
                function.add(new KeyValue().replace(row(1), 2, valueKind, value).setLevel(0));
                KeyValue result = function.getResult();
                assertThat(result).isNotNull();
                assertThat(result.key().getInt(0)).isEqualTo(1);
                assertThat(result.sequenceNumber()).isEqualTo(2);
                assertThat(result.valueKind()).isEqualTo(valueKind);
                return new InternalRowSerializer(readType).copy(result.value());
            } finally {
                function.reset();
            }
        }
    }
}
