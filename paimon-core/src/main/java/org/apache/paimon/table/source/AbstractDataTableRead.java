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
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlobViewLookup;
import org.apache.paimon.utils.ListUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;

/** A {@link InnerTableRead} for data table. */
public abstract class AbstractDataTableRead implements InnerTableRead {

    private RowType readType;
    private boolean executeFilter = false;
    private Predicate predicate;
    private final TableSchema schema;
    @Nullable private final CatalogContext catalogContext;
    @Nullable private final Supplier<InnerTableRead> readFactory;

    public AbstractDataTableRead(TableSchema schema) {
        this(schema, null, null);
    }

    public AbstractDataTableRead(
            TableSchema schema,
            @Nullable CatalogContext catalogContext,
            @Nullable Supplier<InnerTableRead> readFactory) {
        this.schema = schema;
        this.catalogContext = catalogContext;
        this.readFactory = readFactory;
    }

    public abstract void applyReadType(RowType readType);

    public abstract RecordReader<InternalRow> reader(Split split) throws IOException;

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        return this;
    }

    @Override
    public final InnerTableRead withFilter(Predicate predicate) {
        this.predicate = predicate;
        return innerWithFilter(predicate);
    }

    protected abstract InnerTableRead innerWithFilter(Predicate predicate);

    @Override
    public TableRead executeFilter() {
        this.executeFilter = true;
        return this;
    }

    @Override
    public final InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(schema.logicalRowType().project(projection));
    }

    @Override
    public final InnerTableRead withReadType(RowType readType) {
        this.readType = readType;
        applyReadType(readType);
        return this;
    }

    protected void configurePrescanRead(InnerTableRead prescanRead) {}

    @Override
    public final RecordReader<InternalRow> createReader(Split split) throws IOException {
        TableQueryAuthResult authResult = null;
        if (split instanceof QueryAuthSplit) {
            QueryAuthSplit authSplit = (QueryAuthSplit) split;
            split = authSplit.split();
            authResult = authSplit.authResult();
        }

        if (catalogContext != null) {
            RowType rowType = this.readType == null ? schema.logicalRowType() : this.readType;
            int[] blobViewFields = blobViewFields(rowType);
            if (blobViewFields.length > 0) {
                if (readFactory == null) {
                    throw new IllegalStateException(
                            "Cannot read blob-view-field fields without a readFactory.");
                }
                return createBlobViewReader(split, authResult, blobViewFields);
            }
        }

        RecordReader<InternalRow> reader;
        if (authResult == null) {
            reader = reader(split);
        } else {
            reader = authedReader(split, authResult);
        }
        if (executeFilter) {
            reader = executeFilter(reader);
        }

        return reader;
    }

    private int[] blobViewFields(RowType rowType) {
        Set<String> blobViewFieldNames = CoreOptions.fromMap(schema.options()).blobViewField();
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
        RowType rowType = this.readType == null ? schema.logicalRowType() : this.readType;
        RowType blobViewOnlyType = rowType.project(blobViewFields);
        InnerTableRead prescanRead = readFactory.get();
        prescanRead.withReadType(blobViewOnlyType);
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

        RecordReader<InternalRow> reader =
                authResult == null ? reader(split) : authedReader(split, authResult);
        if (executeFilter) {
            reader = executeFilter(reader);
        }
        Set<Integer> blobViewFieldSet = new HashSet<>();
        for (int field : blobViewFields) {
            blobViewFieldSet.add(field);
        }
        return reader.transform(row -> new BlobViewResolvingRow(row, blobViewFieldSet, resolver));
    }

    private RecordReader<InternalRow> authedReader(Split split, TableQueryAuthResult authResult)
            throws IOException {
        RecordReader<InternalRow> reader;
        RowType tableType = schema.logicalRowType();
        RowType readType = this.readType == null ? tableType : this.readType;
        Predicate authPredicate = authResult.extractPredicate();
        ProjectedRow backRow = null;
        if (authPredicate != null) {
            Set<String> authFields = collectFieldNames(authPredicate);
            List<String> readFields = readType.getFieldNames();
            List<String> authAddNames = new ArrayList<>();
            Set<String> readFieldSet = new HashSet<>(readFields);
            for (String field : tableType.getFieldNames()) {
                if (authFields.contains(field) && !readFieldSet.contains(field)) {
                    authAddNames.add(field);
                }
            }
            if (!authAddNames.isEmpty()) {
                readType = tableType.project(ListUtils.union(readFields, authAddNames));
                withReadType(readType);
                backRow = ProjectedRow.from(readType.projectIndexes(readFields));
            }
        }
        reader = authResult.doAuth(reader(split), readType);
        if (backRow != null) {
            reader = reader.transform(backRow::replaceRow);
        }
        return reader;
    }

    private RecordReader<InternalRow> executeFilter(RecordReader<InternalRow> reader) {
        if (predicate == null) {
            return reader;
        }

        Predicate predicate = this.predicate;
        if (readType != null) {
            int[] projection = schema.logicalRowType().getFieldIndices(readType.getFieldNames());
            Optional<Predicate> optional =
                    predicate.visit(PredicateProjectionConverter.fromProjection(projection));
            if (!optional.isPresent()) {
                return reader;
            }
            predicate = optional.get();
        }

        Predicate finalFilter = predicate;
        return reader.filter(finalFilter::test);
    }
}
