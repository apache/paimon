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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.LOOKUP_MERGE_RECORDS_THRESHOLD;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.types.RowKind.INSERT;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link LookupMergeFunction}. */
public class LookupMergeFunctionUnitTest {

    @TempDir java.nio.file.Path tempDir;

    private IOManager ioManager;

    @BeforeEach
    public void beforeEach() {
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (ioManager != null) {
            ioManager.close();
        }
    }

    // Test that the function correctly identifies when level 0 records are present
    @Test
    public void testContainLevel0() {
        LookupMergeFunction function = create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));
        assertThat(function.containLevel0()).isFalse();

        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(0));
        assertThat(function.containLevel0()).isTrue();
    }

    // Test that the function correctly picks the highest level record (lowest level number)
    @Test
    public void testPickHighLevel() {
        LookupMergeFunction function = create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(3)).setLevel(3));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(2));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));

        KeyValue highLevel = function.pickHighLevel();
        assertThat(highLevel).isNotNull();
        assertThat(highLevel.level()).isEqualTo(1);
        assertThat(highLevel.value().getInt(0)).isEqualTo(1);
    }

    // Test that level 0 and negative level records are ignored when picking high level records
    @Test
    public void testPickHighLevelIgnoreLevel0AndNegative() {
        LookupMergeFunction function = create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(4)).setLevel(0));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(3)).setLevel(-1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(2));

        KeyValue highLevel = function.pickHighLevel();
        assertThat(highLevel).isNotNull();
        assertThat(highLevel.level()).isEqualTo(2);
        assertThat(highLevel.value().getInt(0)).isEqualTo(2);
    }

    // Test getResult with a mix of level records including level 0
    @Test
    public void testGetResultWithLevel0() {
        LookupMergeFunction function = create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(0));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(3)).setLevel(2));

        KeyValue result = function.getResult();
        assertThat(result).isNotNull();
        // Should merge level 0 and level 1 (highest level record)
        assertThat(result.value().getInt(0)).isEqualTo(2); // Value from level 0
    }

    // Test getResult with only negative level records
    @Test
    public void testGetResultWithNegativeLevels() {
        LookupMergeFunction function = create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(-1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(-1));

        KeyValue result = function.getResult();
        assertThat(result).isNotNull();
        // Should merge all negative level records
        assertThat(result.value().getInt(0)).isEqualTo(1); // Value from the last added record
    }

    // Test that the function correctly stores and returns the current key
    @Test
    public void testKey() {
        LookupMergeFunction function = create();
        function.reset();
        BinaryRow key = row(1);
        function.add(new KeyValue().replace(key, 1, INSERT, row(1)).setLevel(1));

        BinaryRow currentKey = (BinaryRow) function.key();
        assertThat(currentKey).isNotNull();
        assertThat(currentKey.getInt(0)).isEqualTo(1);
    }

    @Test
    public void testBinaryBuffer() {
        innerTestBinaryBuffer(false);
    }

    @Test
    public void testSpillBinaryBuffer() {
        innerTestBinaryBuffer(true);
    }

    private void innerTestBinaryBuffer(boolean spill) {
        Options options = new Options();
        options.set(LOOKUP_MERGE_RECORDS_THRESHOLD, 3);
        LookupMergeFunction function = create(options, spill ? ioManager : null);
        function.reset();
        for (int i = 5; i >= 0; i--) {
            function.add(new KeyValue().replace(row(1), 1, INSERT, row(i)).setLevel(i));
        }
        KeyValue result = function.getResult();
        assertThat(result).isNotNull();
        assertThat(result.value().getInt(0)).isEqualTo(0);
    }

    private LookupMergeFunction create() {
        return create(new Options(), null);
    }

    private LookupMergeFunction create(Options options, IOManager ioManager) {
        LookupMergeFunction.Factory factory =
                (LookupMergeFunction.Factory)
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                new CoreOptions(options),
                                new RowType(singletonList(new DataField(0, "k", DataTypes.INT()))),
                                new RowType(singletonList(new DataField(0, "v", DataTypes.INT()))));
        factory.withIOManager(ioManager);
        return (LookupMergeFunction) factory.create();
    }
}
