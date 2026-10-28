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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A Computed column's value is computed from input columns. Only expression with at most two inputs
 * (with referenced field at the first) is supported currently.
 */
public class ComputedColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final Expression expression;
    private final boolean caseSensitive;

    public ComputedColumn(String columnName, Expression expression, boolean caseSensitive) {
        this.columnName = columnName;
        this.expression = expression;
        this.caseSensitive = caseSensitive;
    }

    public String columnName() {
        return columnName;
    }

    public DataType columnType() {
        return expression.outputType();
    }

    @Nullable
    public String fieldReference() {
        return expression.fieldReference();
    }

    /** Compute column's value from given argument. Return null if input is null. */
    @Nullable
    public String eval(@Nullable String input) {
        if (fieldReference() != null && input == null) {
            return null;
        }
        return expression.eval(input);
    }

    /**
     * Evaluates this column against a source record. The referenced field is matched by exact name,
     * or ignoring case when the catalog is case-insensitive (record keys keep the case of the
     * source system).
     */
    @Nullable
    public String evalFromRecord(Map<String, String> rowData) {
        return evalFromRecord(rowData, Collections.emptySet());
    }

    /** Evaluate binary source fields as text, preserving the existing expression semantics. */
    @Nullable
    public String evalFromRecord(Map<String, String> rowData, Set<String> base64EncodedFields) {
        return eval(referencedValue(rowData, base64EncodedFields));
    }

    @Nullable
    private String referencedValue(Map<String, String> rowData, Set<String> base64EncodedFields) {
        String reference = fieldReference();
        if (reference == null) {
            return null;
        }
        String value = rowData.get(reference);
        if (caseSensitive || rowData.containsKey(reference)) {
            return decodeBase64IfNeeded(value, base64EncodedFields.contains(reference));
        }
        for (Map.Entry<String, String> entry : rowData.entrySet()) {
            if (reference.equalsIgnoreCase(entry.getKey())) {
                return decodeBase64IfNeeded(
                        entry.getValue(), base64EncodedFields.contains(entry.getKey()));
            }
        }
        return null;
    }

    /**
     * Decodes a marked Base64 binary value to text for expression evaluation.
     *
     * <p>Null and unmarked values are returned unchanged. Marked non-null values must contain valid
     * Base64. The default charset preserves the previous {@code new String(bytes)} interpretation
     * for computed columns; the record retains its Base64 value so the sink can recover the
     * original bytes.
     */
    @Nullable
    private String decodeBase64IfNeeded(@Nullable String value, boolean base64Encoded) {
        return value != null && base64Encoded
                ? new String(Base64.getDecoder().decode(value), Charset.defaultCharset())
                : value;
    }
}
