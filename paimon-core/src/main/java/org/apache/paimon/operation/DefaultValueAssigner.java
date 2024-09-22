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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The field Default value assigner. note that invoke of assigning should be after merge and schema
 * evolution.
 */
public class DefaultValueAssigner {

    private final RowType rowType;
    private final Map<String, String> defaultValues;

    private boolean needToAssign;

    private RowType readRowType;
    private DefaultValueRow defaultValueRow;

    private DefaultValueAssigner(Map<String, String> defaultValues, RowType rowType) {
        this.defaultValues = defaultValues;
        this.needToAssign = !defaultValues.isEmpty();
        this.rowType = rowType;
    }

    public DefaultValueAssigner handleReadRowType(RowType readRowType) {
        this.readRowType = readRowType;
        if (readRowType != null) {
            List<String> requiredFieldNames = readRowType.getFieldNames();
            needToAssign = defaultValues.keySet().stream().anyMatch(requiredFieldNames::contains);
        }
        return this;
    }

    public boolean needToAssign() {
        return needToAssign;
    }

    /** assign default value for column which value is null. */
    public RecordReader<InternalRow> assignFieldsDefaultValue(RecordReader<InternalRow> reader) {
        if (!needToAssign) {
            return reader;
        }

        if (defaultValueRow == null) {
            defaultValueRow = createDefaultValueRow();
        }

        return reader.transform(defaultValueRow::replaceRow);
    }

    @VisibleForTesting
    DefaultValueRow createDefaultValueRow() {
        List<DataField> fields;
        if (readRowType != null) {
            fields = readRowType.getFields();
        } else {
            fields = rowType.getFields();
        }

        GenericRow row = new GenericRow(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            DataField dataField = fields.get(i);
            String defaultValueStr = defaultValues.get(dataField.name());
            if (defaultValueStr == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            CastExecutor<Object, Object> resolve =
                    (CastExecutor<Object, Object>)
                            CastExecutors.resolve(VarCharType.STRING_TYPE, dataField.type());

            if (resolve == null) {
                throw new RuntimeException(
                        "Default value do not support the type of " + dataField.type());
            }
            Object defaultValue = resolve.cast(BinaryString.fromString(defaultValueStr));
            row.setField(i, defaultValue);
        }

        return DefaultValueRow.from(row);
    }

    public Predicate handlePredicate(Predicate filters) {
        Predicate result = filters;
        if (!defaultValues.isEmpty()) {
            if (filters != null) {
                // TODO improve predicate tree with replacing always true and always false
                PredicateReplaceVisitor deletePredicateWithFieldNameVisitor =
                        predicate -> {
                            if (defaultValues.containsKey(predicate.fieldName())) {
                                return Optional.empty();
                            }
                            return Optional.of(predicate);
                        };

                ArrayList<Predicate> filterWithoutDefaultValueField = new ArrayList<>();

                List<Predicate> predicates = PredicateBuilder.splitAnd(filters);
                for (Predicate predicate : predicates) {
                    predicate
                            .visit(deletePredicateWithFieldNameVisitor)
                            .ifPresent(filterWithoutDefaultValueField::add);
                }

                if (!filterWithoutDefaultValueField.isEmpty()) {
                    result = PredicateBuilder.and(filterWithoutDefaultValueField);
                } else {
                    result = null;
                }
            }
        }
        return result;
    }

    public static DefaultValueAssigner create(TableSchema schema) {
        CoreOptions coreOptions = new CoreOptions(schema.options());
        Map<String, String> defaultValues = coreOptions.getFieldDefaultValues();
        return new DefaultValueAssigner(defaultValues, schema.logicalRowType());
    }
}
