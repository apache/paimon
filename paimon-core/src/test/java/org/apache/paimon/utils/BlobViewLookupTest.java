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

package org.apache.paimon.utils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.ReadAuthorizationContext;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BlobViewLookup}. */
class BlobViewLookupTest {

    @Test
    void testUpstreamTablesUseOriginatingReadAuthorization() throws Exception {
        Identifier root = Identifier.create("db", "blob_table");
        Identifier upstream = Identifier.create("db", "upstream");
        ReadAuthorizationContext readContext = ReadAuthorizationContext.forTable(root);
        Catalog catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        ReadBuilder readBuilder = mock(ReadBuilder.class);
        TableScan scan = mock(TableScan.class);
        TableScan.Plan plan = mock(TableScan.Plan.class);
        TableRead read = mock(TableRead.class);
        @SuppressWarnings("unchecked")
        RecordReader<InternalRow> reader = mock(RecordReader.class);

        when(catalog.getTable(upstream, readContext)).thenReturn(table);
        when(table.rowType())
                .thenReturn(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(7, "blob", DataTypes.BLOB()))));
        when(table.copy(any())).thenReturn(table);
        when(table.newReadBuilder()).thenReturn(readBuilder);
        when(readBuilder.withReadType(any())).thenReturn(readBuilder);
        when(readBuilder.withRowRanges(any())).thenReturn(readBuilder);
        when(readBuilder.newScan()).thenReturn(scan);
        when(scan.plan()).thenReturn(plan);
        when(readBuilder.newRead()).thenReturn(read);
        when(read.createReader(plan)).thenReturn(reader);
        when(reader.readBatch()).thenReturn(null);

        BlobViewLookup.createResolver(
                CatalogContext.create(new Options()),
                Collections.singletonList(new BlobViewStruct(upstream, 7, 1L)),
                readContext,
                ignored -> catalog);

        verify(catalog, atLeast(2)).getTable(upstream, readContext);
        verify(catalog, never()).getTable(upstream);
    }
}
