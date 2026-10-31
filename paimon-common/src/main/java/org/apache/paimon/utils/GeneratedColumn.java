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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A generation expression compiled once for a physical top-level column. */
public final class GeneratedColumn {

    private final int targetIndex;
    private final GeneratedExpression expression;

    private GeneratedColumn(int targetIndex, GeneratedExpression expression) {
        this.targetIndex = targetIndex;
        this.expression = expression;
    }

    public static GeneratedColumn create(RowType rowType, DataField field) {
        GeneratedExpression expression =
                GeneratedExpression.parse(
                        field.generationExpression(), rowType, Function.identity());
        checkArgument(
                field.type().equalsIgnoreNullable(expression.outputType()),
                "Generation expression type %s does not match column '%s' type %s.",
                expression.outputType(),
                field.name(),
                field.type());
        return new GeneratedColumn(rowType.getFieldIndex(field.name()), expression);
    }

    /** Resolves SQL identifiers with an engine's case-sensitivity rules. */
    public static String normalizeExpression(
            String expression, RowType rowType, Function<String, String> resolver) {
        return GeneratedExpression.parse(expression, rowType, resolver).normalized();
    }

    public int targetIndex() {
        return targetIndex;
    }

    public List<String> sourceNames() {
        return expression.references().stream().map(FieldRef::name).collect(Collectors.toList());
    }

    @Nullable
    public Object evaluate(InternalRow row) {
        return expression.evaluate(row);
    }
}
