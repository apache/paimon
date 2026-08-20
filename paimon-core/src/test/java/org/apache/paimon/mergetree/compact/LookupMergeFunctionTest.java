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
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.types.RowKind.INSERT;
import static org.assertj.core.api.Assertions.assertThat;

class LookupMergeFunctionTest {

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

    @TempDir Path tempDir;

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
}
