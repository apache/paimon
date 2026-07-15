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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.BlobViewResolver;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.RESTReadVia;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link BlobViewLookup}. */
public class BlobViewLookupTest {

    @Test
    public void testUseReadRootForAllReferencedTableLoads() throws Exception {
        Identifier upstream = Identifier.create("db_upstream", "upstream_table");
        Identifier root = Identifier.create("db_downstream", "downstream_table");
        Identifier expected = RESTReadVia.withReadVia(upstream, root);
        int fieldId = 5;
        long rowId = 7L;
        BlobViewStruct viewStruct = new BlobViewStruct(upstream, fieldId, rowId);
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(fieldId, "blob_field", DataTypes.BLOB())));

        BlobDescriptor descriptor = new BlobDescriptor("file:///blob", 0L, 1L);
        Blob blob = mock(Blob.class);
        when(blob.toDescriptor()).thenReturn(descriptor);
        InternalRow row = mock(InternalRow.class);
        when(row.getLong(1)).thenReturn(rowId);
        when(row.isNullAt(0)).thenReturn(false);
        when(row.getBlob(0)).thenReturn(blob);

        RecordReader.RecordIterator<InternalRow> iterator = mock(RecordReader.RecordIterator.class);
        when(iterator.next()).thenReturn(row, null);
        RecordReader<InternalRow> reader = mock(RecordReader.class);
        when(reader.readBatch()).thenReturn(iterator, null);
        TableScan.Plan plan = mock(TableScan.Plan.class);
        TableScan scan = mock(TableScan.class);
        when(scan.plan()).thenReturn(plan);
        TableRead read = mock(TableRead.class);
        when(read.createReader(plan)).thenReturn(reader);
        ReadBuilder readBuilder = mock(ReadBuilder.class);
        when(readBuilder.withReadType(any(RowType.class))).thenReturn(readBuilder);
        when(readBuilder.withRowRanges(any())).thenReturn(readBuilder);
        when(readBuilder.newRead()).thenReturn(read);
        when(readBuilder.newScan()).thenReturn(scan);

        Table table = mock(Table.class);
        when(table.rowType()).thenReturn(rowType);
        when(table.copy(any())).thenReturn(table);
        when(table.newReadBuilder()).thenReturn(readBuilder);
        when(table.fileIO()).thenReturn(mock(FileIO.class));
        List<Identifier> loadedIdentifiers = Collections.synchronizedList(new ArrayList<>());
        Catalog catalog = mock(Catalog.class);
        when(catalog.getTable(any(Identifier.class)))
                .thenAnswer(
                        invocation -> {
                            loadedIdentifiers.add(invocation.getArgument(0));
                            return table;
                        });

        BlobViewResolver resolver =
                BlobViewLookup.createResolver(
                        CatalogContext.create(new Options()),
                        Collections.singletonList(viewStruct),
                        root,
                        ignored -> catalog);
        BlobView blobView = new BlobView(viewStruct);
        resolver.resolve(blobView);

        assertThat(blobView.isResolved()).isTrue();
        assertThat(loadedIdentifiers).containsExactly(expected, expected, expected);
    }
}
