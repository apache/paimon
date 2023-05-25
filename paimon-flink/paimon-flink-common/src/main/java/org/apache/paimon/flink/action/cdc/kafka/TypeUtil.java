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

package org.apache.paimon.flink.action.cdc.kafka;

import java.util.Arrays;
import java.util.List;

/** Get precision or scale from string type. */
public class TypeUtil {

    private static final String DECIMAL = "decimal";

    private static final String SET = "set";
    private static final String ENUM = "enum";
    private static final String LEFT_BRACKETS = "(";
    private static final String RIGHT_BRACKETS = ")";
    private static final String COMMA = ",";

    private static final List<String> HAVE_SCALE_LIST =
            Arrays.asList("decimal", "numeric", "double", "real", "fixed");

    public static boolean isScaleType(String typeName) {
        return HAVE_SCALE_LIST.stream().anyMatch(type -> typeName.toLowerCase().startsWith(type));
    }

    public static boolean isEnumType(String typeName) {
        return typeName.toLowerCase().startsWith(ENUM);
    }

    public static boolean isSetType(String typeName) {
        return typeName.toLowerCase().startsWith(SET);
    }

    public static String getType(String typeName) {

        if (typeName.contains(LEFT_BRACKETS) && typeName.contains(RIGHT_BRACKETS)) {
            String type =
                    typeName.substring(0, typeName.indexOf(LEFT_BRACKETS)).trim()
                            + typeName.substring(typeName.indexOf(RIGHT_BRACKETS) + 1);
            return type;
        } else {
            return typeName;
        }
    }

    public static int getPrecision(String typeName) {

        if (typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && isScaleType(typeName)) {
            int precision =
                    Integer.parseInt(
                            typeName.substring(
                                            typeName.indexOf(LEFT_BRACKETS) + 1,
                                            typeName.indexOf(COMMA))
                                    .trim());
            return precision;
        } else if ((typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && !isScaleType(typeName)
                && !isEnumType(typeName)
                && !isSetType(typeName))) {
            int precision =
                    Integer.parseInt(
                            typeName.substring(
                                            typeName.indexOf(LEFT_BRACKETS) + 1,
                                            typeName.indexOf(RIGHT_BRACKETS))
                                    .trim());
            return precision;
        } else {
            return 0;
        }
    }

    public static int getScale(String typeName) {

        if (typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && isScaleType(typeName)) {
            int scale =
                    Integer.parseInt(
                            typeName.substring(
                                            typeName.indexOf(COMMA) + 1,
                                            typeName.indexOf(RIGHT_BRACKETS))
                                    .trim());
            return scale;
        } else {
            return 0;
        }
    }
}
