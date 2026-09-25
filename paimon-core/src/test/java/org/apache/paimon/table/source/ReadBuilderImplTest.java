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

package org.apache.paimon.table.source;

import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.ObjectStreamClass;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link ReadBuilderImpl}. */
public class ReadBuilderImplTest {

    @Test
    public void testLongLimitCompatibilityBridge() {
        ReadBuilder legacyBuilder = mock(ReadBuilder.class, CALLS_REAL_METHODS);

        legacyBuilder.withLimit((long) Integer.MAX_VALUE);
        verify(legacyBuilder).withLimit(Integer.MAX_VALUE);

        clearInvocations(legacyBuilder);
        assertThat(legacyBuilder.withLimit(4294967297L)).isSameAs(legacyBuilder);
        verify(legacyBuilder, never()).withLimit(anyInt());
    }

    @Test
    public void testSerializedLimitFieldCompatibility() {
        assertThat(ObjectStreamClass.lookup(ReadBuilderImpl.class).getField("limit").getType())
                .isEqualTo(Integer.class);
    }

    @Test
    public void testLongLimitForwardedToScanAndRead() {
        InnerTable table = mock(InnerTable.class);
        when(table.name()).thenReturn("table");
        when(table.options()).thenReturn(Collections.emptyMap());
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.rowType()).thenReturn(RowType.of(DataTypes.INT()));

        InnerTableScan scan = mock(InnerTableScan.class, RETURNS_SELF);
        InnerTableRead read = mock(InnerTableRead.class, RETURNS_SELF);
        when(table.newScan()).thenReturn(scan);
        when(table.newRead()).thenReturn(read);

        long limit = 4294967297L;
        ReadBuilderImpl builder = new ReadBuilderImpl(table);
        builder.withLimit(limit);
        builder.newScan();
        builder.newRead();

        verify(scan).withLimit(limit);
        verify(read).withLimit(limit);
    }
}
