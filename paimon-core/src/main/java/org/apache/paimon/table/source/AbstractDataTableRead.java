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
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** A {@link InnerTableRead} for data table. */
public abstract class AbstractDataTableRead implements InnerTableRead {

    private RowType readType;
    private boolean executeFilter = false;
    private Predicate predicate;
    private final TableSchema schema;

    // The read type the subclass reads with; differs from readType only when widened for
    // auth, and is fixed once a reader exists (split reads cache their format readers).
    @Nullable private RowType appliedReadType;
    private boolean readerCreated = false;

    // blob-view columns that only resolve through the dedicated blob-view read path
    private final Set<String> resolvedBlobViewFields;

    public AbstractDataTableRead(@Nullable TableSchema schema) {
        this.schema = schema;
        Set<String> blobViewFields = Collections.emptySet();
        if (schema != null) {
            CoreOptions options = CoreOptions.fromMap(schema.options());
            if (options.blobViewResolveEnabled()) {
                blobViewFields = options.blobViewField();
            }
        }
        this.resolvedBlobViewFields = blobViewFields;
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
        this.appliedReadType = readType;
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
        RecordReader<InternalRow> reader;
        if (authResult == null) {
            reader = backProject(readSplit(split));
        } else {
            reader = authedReader(split, authResult);
        }
        if (executeFilter) {
            reader = executeFilter(reader);
        }

        return reader;
    }

    private RecordReader<InternalRow> readSplit(Split split) throws IOException {
        RecordReader<InternalRow> reader = reader(split);
        // set only on success, so a transient failure does not fix the read schema
        readerCreated = true;
        return reader;
    }

    private RecordReader<InternalRow> authedReader(Split split, TableQueryAuthResult authResult)
            throws IOException {
        List<String> readFields = currentReadType().getFieldNames();
        Set<String> ruleFields = authResult.requiredAuthFields(readFields);
        RowType widened = widenedReadType(authResult, ruleFields);
        if (widened != null && !readerCreated && !widened.equals(appliedReadType)) {
            applyReadType(widened);
            appliedReadType = widened;
        }
        // the split read emits appliedReadType; rules are remapped against it by name
        RowType outputType = appliedReadType != null ? appliedReadType : currentReadType();
        if (widened != null && !widened.equals(outputType)) {
            // rules changed after the read schema was fixed: fail clearly if they no longer fit
            List<String> outputFields = outputType.getFieldNames();
            for (String field : widened.getFieldNames()) {
                if (!outputFields.contains(field)) {
                    throw new IllegalStateException(
                            String.format(
                                    "Query auth rules changed and now require column '%s', but the "
                                            + "read schema is already fixed to %s. Recreate the "
                                            + "reader to apply the new rules.",
                                    field, outputFields));
                }
            }
        }
        // masks apply only to columns readable from the query; skip the set when no
        // mask exists (filter-only auth)
        Set<String> activeFields;
        if (authResult.extractColumnMasking().isEmpty()) {
            activeFields = Collections.emptySet();
        } else {
            activeFields = new HashSet<>(readFields);
            activeFields.addAll(ruleFields);
        }
        RecordReader<InternalRow> reader =
                authResult.doAuth(readSplit(split), outputType, activeFields);
        return backProject(reader);
    }

    /**
     * Project auth-widened rows back to the query's read type — on every split, since the widened
     * read schema stays in effect even for splits without auth rules.
     */
    private RecordReader<InternalRow> backProject(RecordReader<InternalRow> reader) {
        if (appliedReadType == null || appliedReadType == readType) {
            return reader;
        }
        ProjectedRow backRow =
                ProjectedRow.from(
                        appliedReadType.projectIndexes(currentReadType().getFieldNames()));
        return reader.transform(backRow::replaceRow);
    }

    /**
     * The read type widened with the unprojected columns the auth rules read (mirrors the
     * row-filter augmentation of #8447), or null when the projection already covers them. The
     * projected fields are kept as-is to preserve nested pruning.
     */
    @Nullable
    private RowType widenedReadType(TableQueryAuthResult authResult, Set<String> ruleFields) {
        RowType tableType = schema.logicalRowType();
        RowType readType = currentReadType();
        Set<String> maskTargets = authResult.extractColumnMasking().keySet();
        for (String name : readType.getFieldNames()) {
            if (!ruleFields.contains(name) && !maskTargets.contains(name)) {
                continue;
            }
            if (!tableType.containsField(name)) {
                continue;
            }
            // rules must not touch a nested-pruned column (partial value)
            DataField tableField = tableType.getField(name);
            if (!readType.getField(name).type().equals(tableField.type())) {
                throw new IllegalStateException(
                        String.format(
                                "Query auth rules involve column '%s', which the query "
                                        + "projects with a pruned type %s instead of its "
                                        + "table type %s; cannot apply the rules to a "
                                        + "partial column.",
                                name, readType.getField(name).type(), tableField.type()));
            }
        }
        for (String name : ruleFields) {
            if (resolvedBlobViewFields.contains(name) && !readType.containsField(name)) {
                // auth-added columns bypass blob-view resolution
                throw new IllegalStateException(
                        String.format(
                                "Query auth rules read blob-view column '%s', which the query "
                                        + "does not project; such columns cannot be resolved. "
                                        + "Project the column or adjust the rule.",
                                name));
            }
        }
        return TableQueryAuthResult.appendMissingFields(tableType, readType, ruleFields);
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
