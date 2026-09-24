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
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/** A {@link InnerTableRead} for data table. */
public abstract class AbstractDataTableRead implements InnerTableRead {

    private RowType readType;
    protected boolean executeFilter = false;
    private Predicate predicate;
    private final TableSchema schema;

    // reader-level filtering sees raw values, so it stays off for auth-enabled tables,
    // as read-level TopN already does (see ReadBuilderImpl)
    private final boolean queryAuthEnabled;

    // blob-view columns that only resolve through the dedicated blob-view read path
    private final Set<String> resolvedBlobViewFields;

    public AbstractDataTableRead(@Nullable TableSchema schema) {
        this.schema = schema;
        Set<String> blobViewFields = Collections.emptySet();
        boolean queryAuthEnabled = false;
        if (schema != null) {
            CoreOptions options = CoreOptions.fromMap(schema.options());
            if (options.blobViewResolveEnabled()) {
                blobViewFields = options.blobViewField();
            }
            queryAuthEnabled = options.queryAuthEnabled();
        }
        this.resolvedBlobViewFields = blobViewFields;
        this.queryAuthEnabled = queryAuthEnabled;
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
        if (queryAuthEnabled) {
            return this;
        }
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

    protected TableSchema schema() {
        return schema;
    }

    protected RowType currentReadType() {
        return readType == null ? schema.logicalRowType() : readType;
    }

    @Nullable
    protected Predicate predicate() {
        return predicate;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        QueryAuthContext queryAuthContext = unwrapQueryAuthSplit(split);
        return createDataReader(queryAuthContext.split(), queryAuthContext.authResult());
    }

    protected final QueryAuthContext unwrapQueryAuthSplit(Split split) {
        if (split instanceof QueryAuthSplit) {
            QueryAuthSplit authSplit = (QueryAuthSplit) split;
            return new QueryAuthContext(authSplit.split(), authSplit.authResult());
        }
        return new QueryAuthContext(split, null);
    }

    protected final RecordReader<InternalRow> createDataReader(
            Split split, @Nullable TableQueryAuthResult authResult) throws IOException {
        return createDataReader(split, authResult, executeFilter);
    }

    protected final RecordReader<InternalRow> createDataReader(
            Split split, @Nullable TableQueryAuthResult authResult, boolean filterOnRead)
            throws IOException {
        if (authResult == null && !(filterOnRead && predicate != null)) {
            // Restore an explicit projection after a previous split needed authorization columns.
            // Without a projection, preserve the underlying reader's default read type.
            if (readType != null) {
                applyReadType(readType);
            }
            return reader(split);
        }
        ReadTransform transform =
                ReadTransform.create(
                        schema.logicalRowType(),
                        currentReadType(),
                        predicate,
                        filterOnRead,
                        authResult,
                        resolvedBlobViewFields);
        if (readType != null || !transform.readType().equals(currentReadType())) {
            applyReadType(transform.readType());
        }
        return transform.apply(reader(split));
    }

    /** Split with auth context. */
    protected static class QueryAuthContext {

        private final Split split;
        @Nullable private final TableQueryAuthResult authResult;

        private QueryAuthContext(Split split, @Nullable TableQueryAuthResult authResult) {
            this.split = split;
            this.authResult = authResult;
        }

        protected Split split() {
            return split;
        }

        @Nullable
        protected TableQueryAuthResult authResult() {
            return authResult;
        }
    }
}
