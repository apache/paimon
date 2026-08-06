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

import org.apache.paimon.utils.ObjectSerializerTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataFileMetaSerializer}. */
public class DataFileMetaSerializerTest extends ObjectSerializerTestBase<DataFileMeta> {

    private final DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();

    @Override
    protected DataFileMetaSerializer serializer() {
        return new DataFileMetaSerializer();
    }

    @Override
    protected DataFileMeta object() {
        return gen.next()
                .meta
                .copy(Arrays.asList("extra1", "extra2"))
                .withColumnMaxSequenceNumbers(Collections.singletonMap(3, 42L));
    }

    @Test
    void testCopyOperationsPreserveColumnSequences() {
        DataFileMeta file = object();
        assertColumnSequences(file.upgrade(file.level() + 1));
        assertColumnSequences(file.rename("renamed.parquet"));
        assertColumnSequences(file.copyWithoutStats());
        assertColumnSequences(file.assignSequenceNumber(1L, 2L));
        assertColumnSequences(file.assignFirstRowId(1L));
        assertColumnSequences(file.newFirstRowId(null));
        assertColumnSequences(file.copy(Collections.emptyList()));
        assertColumnSequences(file.newExternalPath("external/renamed.parquet"));
        assertColumnSequences(file.copy(new byte[] {1}));
    }

    @Test
    void testLegacySerializerDropsColumnSequences() {
        DataFileMetaWriteColsLegacySerializer legacy = new DataFileMetaWriteColsLegacySerializer();
        DataFileMeta file = legacy.fromRow(legacy.toRow(object()));
        assertThat(file.columnMaxSequenceNumbers()).isNull();
    }

    private void assertColumnSequences(DataFileMeta file) {
        assertThat(file.columnMaxSequenceNumbers()).containsEntry(3, 42L);
    }
}
