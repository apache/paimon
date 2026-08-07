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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.BlobViewResolver;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlobViewLookup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

/** Shared helpers for resolving {@link org.apache.paimon.CoreOptions#BLOB_VIEW_FIELD} on read. */
final class BlobViewTableReadSupport {

    private BlobViewTableReadSupport() {}

    static int[] blobViewFieldIndexes(RowType rowType, CoreOptions options) {
        if (!options.blobViewResolveEnabled()) {
            return new int[0];
        }

        Set<String> blobViewFieldNames = options.blobViewField();
        if (blobViewFieldNames.isEmpty()) {
            return new int[0];
        }

        return rowType.getFields().stream()
                .filter(
                        field ->
                                field.type().is(DataTypeRoot.BLOB)
                                        && blobViewFieldNames.contains(field.name()))
                .mapToInt(field -> rowType.getFieldIndex(field.name()))
                .toArray();
    }

    static RecordReader<InternalRow> createBlobViewReader(
            CatalogContext catalogContext,
            Split split,
            @Nullable TableQueryAuthResult authResult,
            int[] blobViewFields,
            RowType readType,
            @Nullable Predicate predicate,
            @Nullable TopN topN,
            @Nullable Integer limit,
            boolean executeFilter,
            RecordReaderSupplier dataReaderSupplier,
            Supplier<InnerTableRead> prescanReadSupplier)
            throws IOException {
        RowType prescanType = executeFilter ? readType : readType.project(blobViewFields);
        int[] prescanBlobViewFields = new int[blobViewFields.length];
        if (executeFilter) {
            System.arraycopy(blobViewFields, 0, prescanBlobViewFields, 0, blobViewFields.length);
        } else {
            for (int i = 0; i < prescanBlobViewFields.length; i++) {
                prescanBlobViewFields[i] = i;
            }
        }
        InnerTableRead prescanRead = prescanReadSupplier.get();
        prescanRead.withReadType(prescanType);
        if (predicate != null) {
            prescanRead.withFilter(predicate);
        }
        if (topN != null) {
            prescanRead.withTopN(topN);
        }
        if (limit != null) {
            prescanRead.withLimit(limit);
        }

        Split prescanSplit = authResult != null ? new QueryAuthSplit(split, authResult) : split;
        LinkedHashSet<BlobViewStruct> viewStructs = new LinkedHashSet<>();
        RecordReader<InternalRow> prescanReader = prescanRead.createReader(prescanSplit);
        try {
            prescanReader.forEachRemaining(
                    row -> {
                        for (int field : prescanBlobViewFields) {
                            if (row.isNullAt(field)) {
                                continue;
                            }
                            Blob blob = row.getBlob(field);
                            if (!(blob instanceof BlobView)) {
                                throw new IllegalArgumentException(
                                        "blob-view-field requires blob field value to be a "
                                                + "serialized BlobViewStruct.");
                            }
                            viewStructs.add(((BlobView) blob).viewStruct());
                        }
                    });
        } finally {
            prescanReader.close();
        }

        BlobViewResolver resolver =
                BlobViewLookup.createResolver(catalogContext, new ArrayList<>(viewStructs));

        RecordReader<InternalRow> reader = dataReaderSupplier.get();
        Set<Integer> blobViewFieldSet = new HashSet<>();
        for (int field : blobViewFields) {
            blobViewFieldSet.add(field);
        }
        return reader.transform(row -> new BlobViewResolvingRow(row, blobViewFieldSet, resolver));
    }

    @FunctionalInterface
    interface RecordReaderSupplier {

        RecordReader<InternalRow> get() throws IOException;
    }
}
