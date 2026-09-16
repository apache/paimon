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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.NestedProjectedRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Helpers for evaluating complete query filters before the output projection. */
public final class TableReadFilter {

    private TableReadFilter() {}

    /** Include every filter operand, preserving the order of the requested output fields. */
    public static RowType readType(RowType tableType, RowType readType, Predicate predicate) {
        Set<String> fields = PredicateVisitor.collectFieldNames(predicate);
        List<DataField> widened = new ArrayList<>(readType.getFields());
        for (DataField field : tableType.getFields()) {
            if (fields.contains(field.name())) {
                int index = readType.getFieldIndex(field.name());
                if (index < 0) {
                    widened.add(field);
                } else {
                    // Filters use the full field type, even when the output prunes nested fields.
                    widened.set(index, field);
                }
            }
        }
        RowType result = readType.copy(widened);
        checkArgument(
                result.getFieldNames().containsAll(fields),
                "Cannot execute filter on fields %s with read type %s.",
                fields,
                result);
        return result;
    }

    public static RecordReader<InternalRow> filter(
            RecordReader<InternalRow> reader, RowType readType, Predicate predicate) {
        Predicate remapped = TableQueryAuthResult.remapPredicate(predicate, readType);
        return reader.filter(remapped::test);
    }

    public static RecordReader<InternalRow> project(
            RecordReader<InternalRow> reader, RowType readType, RowType outputType) {
        NestedProjectedRow projection = NestedProjectedRow.create(readType, outputType);
        return projection == null ? reader : reader.transform(projection::replaceRow);
    }
}
