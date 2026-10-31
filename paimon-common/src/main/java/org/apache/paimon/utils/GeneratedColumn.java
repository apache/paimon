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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A supported generation expression for a physical partition column. */
public final class GeneratedColumn {

    private static final Pattern TIMESTAMP_TO_DATE =
            Pattern.compile(
                    "\\s*CAST\\s*\\(\\s*(`(?:``|[^`])+`|[a-zA-Z_][a-zA-Z_0-9]*)\\s+AS\\s+DATE\\s*\\)\\s*",
                    Pattern.CASE_INSENSITIVE);

    private final int targetIndex;
    private final int sourceIndex;
    private final int precision;
    private final String sourceName;

    private GeneratedColumn(int targetIndex, int sourceIndex, int precision, String sourceName) {
        this.targetIndex = targetIndex;
        this.sourceIndex = sourceIndex;
        this.precision = precision;
        this.sourceName = sourceName;
    }

    public static GeneratedColumn create(RowType rowType, DataField field) {
        String sourceName = sourceName(field.generationExpression());
        checkArgument(
                rowType.containsField(sourceName),
                "Generation expression for column '%s' references unknown column '%s'.",
                field.name(),
                sourceName);
        DataField source = rowType.getField(sourceName);
        checkArgument(
                source.generationExpression() == null,
                "Generated column '%s' cannot reference generated column '%s'.",
                field.name(),
                sourceName);
        checkArgument(
                field.type().is(DataTypeRoot.DATE),
                "Generated column '%s' must have DATE type.",
                field.name());
        checkArgument(
                source.type().is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE),
                "Generation expression for column '%s' requires a TIMESTAMP WITHOUT TIME ZONE source.",
                field.name());
        return new GeneratedColumn(
                rowType.getFieldIndex(field.name()),
                rowType.getFieldIndex(sourceName),
                ((TimestampType) source.type()).getPrecision(),
                sourceName);
    }

    /** Parses the single referenced identifier without resolving it against a schema. */
    public static String sourceName(String expression) {
        Matcher matcher = TIMESTAMP_TO_DATE.matcher(expression);
        checkArgument(
                matcher.matches(),
                "Unsupported generation expression: %s. Only CAST(timestamp_column AS DATE) is supported.",
                expression);
        String reference = matcher.group(1);
        return reference.startsWith("`")
                ? reference.substring(1, reference.length() - 1).replace("``", "`")
                : reference;
    }

    public int targetIndex() {
        return targetIndex;
    }

    public String sourceName() {
        return sourceName;
    }

    @Nullable
    public Integer evaluate(InternalRow row) {
        return row.isNullAt(sourceIndex)
                ? null
                : (int)
                        Math.floorDiv(
                                row.getTimestamp(sourceIndex, precision).getMillisecond(),
                                DateTimeUtils.MILLIS_PER_DAY);
    }
}
