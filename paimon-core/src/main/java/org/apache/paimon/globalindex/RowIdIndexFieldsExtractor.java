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

package org.apache.paimon.globalindex;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.ResolvedFieldPath;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The extractor to get partition, index field and row id from records. */
public class RowIdIndexFieldsExtractor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int rowIdPos;
    private final RowType readType;
    private final List<String> partitionKeys;
    private final List<String> indexFields;

    private transient Projection lazyPartitionProjection;
    private transient FieldGetter[] lazyIndexFieldGetters;

    public RowIdIndexFieldsExtractor(
            RowType readType, List<String> partitionKeys, String indexField) {
        this(readType, partitionKeys, Collections.singletonList(indexField));
    }

    public RowIdIndexFieldsExtractor(
            RowType readType, List<String> partitionKeys, List<String> indexFields) {
        this.readType = readType;
        this.partitionKeys = partitionKeys;
        this.indexFields = Collections.unmodifiableList(new ArrayList<>(indexFields));
        this.rowIdPos = readType.getFieldIndex(SpecialFields.ROW_ID.name());

        checkArgument(!indexFields.isEmpty(), "Index field paths must not be empty.");
        for (String indexField : indexFields) {
            Optional<ResolvedFieldPath> resolved = ResolvedFieldPath.resolve(readType, indexField);
            checkArgument(
                    resolved.isPresent(),
                    "Index field path '%s' does not exist in the read type.",
                    indexField);
        }
    }

    private Projection partitionProjection() {
        if (lazyPartitionProjection == null) {
            lazyPartitionProjection = CodeGenUtils.newProjection(readType, partitionKeys);
        }
        return lazyPartitionProjection;
    }

    private FieldGetter[] indexFieldGetters() {
        if (lazyIndexFieldGetters == null) {
            lazyIndexFieldGetters = new FieldGetter[indexFields.size()];
            for (int i = 0; i < indexFields.size(); i++) {
                ResolvedFieldPath path =
                        ResolvedFieldPath.resolve(readType, indexFields.get(i)).get();
                List<DataField> fields = path.fields();
                int[] indexes = path.indexes();
                FieldGetter[] pathGetters = new FieldGetter[indexes.length];
                for (int j = 0; j < indexes.length; j++) {
                    pathGetters[j] =
                            InternalRow.createFieldGetter(fields.get(j).type(), indexes[j]);
                }

                lazyIndexFieldGetters[i] =
                        row -> {
                            Object current = row;
                            for (FieldGetter pathGetter : pathGetters) {
                                if (current == null) {
                                    return null;
                                }
                                current = pathGetter.getFieldOrNull((InternalRow) current);
                            }
                            return current;
                        };
            }
        }
        return lazyIndexFieldGetters;
    }

    public BinaryRow extractPartition(InternalRow record) {
        return partitionProjection().apply(record).copy();
    }

    @Nullable
    public Object extractIndexField(InternalRow record) {
        return indexFieldGetters()[0].getFieldOrNull(record);
    }

    public InternalRow extractIndexFields(InternalRow record) {
        FieldGetter[] getters = indexFieldGetters();
        Object[] values = new Object[getters.length];
        for (int i = 0; i < getters.length; i++) {
            values[i] = getters[i].getFieldOrNull(record);
        }
        return GenericRow.of(values);
    }

    public Long extractRowId(InternalRow record) {
        return record.getLong(rowIdPos);
    }
}
