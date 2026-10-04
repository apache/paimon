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

package org.apache.paimon.flink;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

/**
 * Access to Flink's {@code NestedFieldReferenceExpression}, which only exists from Flink 1.19 on.
 *
 * <p>This module is compiled once and bundled into every {@code paimon-flink-*} distribution,
 * including those for Flink 1.16 to 1.18 where the class is absent. Every direct reference to it
 * therefore lives in {@link Holder}, a separate class file that is only ever loaded once {@link
 * #AVAILABLE} has confirmed the expression is on the classpath.
 */
public class NestedFieldReferences {

    private static final String CLASS_NAME =
            "org.apache.flink.table.expressions.NestedFieldReferenceExpression";

    private static final boolean AVAILABLE = isOnClasspath();

    private NestedFieldReferences() {}

    private static boolean isOnClasspath() {
        try {
            Class.forName(CLASS_NAME, false, NestedFieldReferences.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /** Whether {@code expression} references a field nested inside a row. */
    public static boolean isNestedFieldReference(Expression expression) {
        return AVAILABLE && Holder.isInstance(expression);
    }

    /**
     * The path to the referenced field, starting with the name of the top-level field it is nested
     * in. Callers must check {@link #isNestedFieldReference} first.
     */
    public static String[] fieldNames(Expression expression) {
        return Holder.fieldNames(expression);
    }

    /** The type of the referenced field. Callers must check {@link #isNestedFieldReference}. */
    public static DataType outputDataType(Expression expression) {
        return Holder.outputDataType(expression);
    }

    private static class Holder {

        static boolean isInstance(Expression expression) {
            return expression
                    instanceof org.apache.flink.table.expressions.NestedFieldReferenceExpression;
        }

        static String[] fieldNames(Expression expression) {
            return ((org.apache.flink.table.expressions.NestedFieldReferenceExpression) expression)
                    .getFieldNames();
        }

        static DataType outputDataType(Expression expression) {
            return ((org.apache.flink.table.expressions.NestedFieldReferenceExpression) expression)
                    .getOutputDataType();
        }
    }
}
