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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Optional;

/** A {@link InnerTableRead} for data table. */
public abstract class AbstractDataTableRead<T> implements InnerTableRead {

    private final DefaultValueAssigner defaultValueAssigner;

    private RowType readType;
    private boolean executeFilter = false;
    private Predicate predicate;
    private final TableSchema schema;

    public AbstractDataTableRead(TableSchema schema) {
        this.schema = schema;
        this.defaultValueAssigner = schema == null ? null : DefaultValueAssigner.create(schema);
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
        if (defaultValueAssigner != null) {
            predicate = defaultValueAssigner.handlePredicate(predicate);
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
        this.defaultValueAssigner.handleReadRowType(readType);
        applyReadType(readType);
        return this;
    }

    @Override
    public final RecordReader<InternalRow> createReader(Split split) throws IOException {
        RecordReader<InternalRow> reader = reader(split);
        if (defaultValueAssigner != null) {
            reader = defaultValueAssigner.assignFieldsDefaultValue(reader);
        }
        if (executeFilter) {
            reader = executeFilter(reader);
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
                    predicate.visit(new PredicateProjectionConverter(projection));
            if (!optional.isPresent()) {
                return reader;
            }
            predicate = optional.get();
        }

        Predicate finalFilter = predicate;
        return reader.filter(finalFilter::test);
    }
}
