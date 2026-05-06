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
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlobViewLookup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/** A {@link TableRead} for data-evolution enabled append-only tables. */
public class DataEvolutionTableRead extends AppendTableRead {

    @Nullable private final CatalogContext catalogContext;
    @Nullable private final Supplier<InnerTableRead> readFactory;

    public DataEvolutionTableRead(
            List<Function<SplitReadConfig, SplitReadProvider>> providerFactories,
            TableSchema schema,
            @Nullable CatalogContext catalogContext,
            @Nullable Supplier<InnerTableRead> readFactory) {
        super(providerFactories, schema);
        this.catalogContext = catalogContext;
        this.readFactory = readFactory;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        QueryAuthContext queryAuthContext = unwrapQueryAuthSplit(split);
        if (catalogContext != null) {
            int[] blobViewFields = blobViewFields(currentReadType());
            if (blobViewFields.length > 0) {
                if (readFactory == null) {
                    throw new IllegalStateException(
                            "Cannot read blob-view-field fields without a readFactory.");
                }
                return createBlobViewReader(
                        queryAuthContext.split(), queryAuthContext.authResult(), blobViewFields);
            }
        }
        return createDataReader(queryAuthContext.split(), queryAuthContext.authResult());
    }

    private int[] blobViewFields(RowType rowType) {
        Set<String> blobViewFieldNames = CoreOptions.fromMap(schema().options()).blobViewField();
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

    private RecordReader<InternalRow> createBlobViewReader(
            Split split, @Nullable TableQueryAuthResult authResult, int[] blobViewFields)
            throws IOException {
        RowType blobViewOnlyType = currentReadType().project(blobViewFields);
        InnerTableRead prescanRead = readFactory.get();
        prescanRead.withReadType(blobViewOnlyType);
        Predicate predicate = predicate();
        if (predicate != null) {
            prescanRead.withFilter(predicate);
        }
        configurePrescanRead(prescanRead);
        Split prescanSplit = authResult != null ? new QueryAuthSplit(split, authResult) : split;
        LinkedHashSet<BlobViewStruct> viewStructs = new LinkedHashSet<>();
        RecordReader<InternalRow> prescanReader = prescanRead.createReader(prescanSplit);
        try {
            prescanReader.forEachRemaining(
                    row -> {
                        for (int i = 0; i < blobViewFields.length; i++) {
                            if (row.isNullAt(i)) {
                                continue;
                            }
                            Blob blob = row.getBlob(i);
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

        RecordReader<InternalRow> reader = createDataReader(split, authResult);
        Set<Integer> blobViewFieldSet = new HashSet<>();
        for (int field : blobViewFields) {
            blobViewFieldSet.add(field);
        }
        return reader.transform(row -> new BlobViewResolvingRow(row, blobViewFieldSet, resolver));
    }
}
