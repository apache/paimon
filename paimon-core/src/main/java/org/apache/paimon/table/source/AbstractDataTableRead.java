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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;

/** A {@link InnerTableRead} for data table. */
public abstract class AbstractDataTableRead implements InnerTableRead {

    private RowType readType;
    protected boolean executeFilter = false;
    private Predicate predicate;
    private final TableSchema schema;

    public AbstractDataTableRead(TableSchema schema) {
        this.schema = schema;
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
            reader = reader(split);
        } else {
            reader = authedReader(split, authResult);
        }
        if (executeFilter) {
            reader = executeFilter(reader);
        }

        return reader;
    }

    private RecordReader<InternalRow> authedReader(Split split, TableQueryAuthResult authResult)
            throws IOException {
        RecordReader<InternalRow> reader;
        RowType tableType = schema.logicalRowType();
        RowType readType = this.readType == null ? tableType : this.readType;
        Predicate authPredicate = authResult.extractPredicate();
        Map<String, Transform> columnMasking = authResult.extractColumnMasking();
        ProjectedRow backRow = null;
        List<String> readFields = readType.getFieldNames();
        Set<String> readFieldSet = new HashSet<>(readFields);
        Map<String, Transform> selectedColumnMasking = new HashMap<>();
        for (Map.Entry<String, Transform> mask : columnMasking.entrySet()) {
            if (readFieldSet.contains(mask.getKey())) {
                selectedColumnMasking.put(mask.getKey(), mask.getValue());
            }
        }
        Set<String> authFields = new HashSet<>();
        if (authPredicate != null) {
            authFields.addAll(collectFieldNames(authPredicate));
        }
        for (Map.Entry<String, Transform> mask : selectedColumnMasking.entrySet()) {
            authFields.add(mask.getKey());
            for (Object input : mask.getValue().inputs()) {
                if (input instanceof FieldRef) {
                    authFields.add(((FieldRef) input).name());
                }
            }
        }
        if (!authFields.isEmpty()) {
            List<DataField> expandedFields = new ArrayList<>(readType.getFields());
            for (DataField field : tableType.getFields()) {
                if (authFields.contains(field.name()) && !readFieldSet.contains(field.name())) {
                    expandedFields.add(field);
                }
            }
            if (expandedFields.size() > readType.getFieldCount()) {
                readType = readType.copy(expandedFields);
                applyReadType(readType);
                backRow = ProjectedRow.from(readType.projectIndexes(readFields));
            }
        }
        reader = authResult.doAuth(reader(split), readType, authPredicate, selectedColumnMasking);
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
