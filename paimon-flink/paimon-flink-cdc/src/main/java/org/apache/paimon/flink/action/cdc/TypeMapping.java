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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utility that holds data type mapping options. */
public class TypeMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<TypeMappingMode> typeMappingModes;

    public TypeMapping(Set<TypeMappingMode> typeMappingModes) {
        this.typeMappingModes = typeMappingModes;
    }

    public boolean containsMode(TypeMappingMode mode) {
        return typeMappingModes.contains(mode);
    }

    public static TypeMapping defaultMapping() {
        return new TypeMapping(Collections.emptySet());
    }

    public static TypeMapping parse(String[] rawOptions) {
        Set<TypeMappingMode> typeMappingModes =
                Arrays.stream(rawOptions)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .map(TypeMappingMode::mode)
                        .collect(Collectors.toSet());
        return new TypeMapping(typeMappingModes);
    }

    /**
     * Describe how to map MySQL data type to Paimon data type. Currently, three modes are
     * supported:
     *
     * <ul>
     *   <li>TINYINT1_NOT_BOOL: maps MySQL TINYINT(1) to TINYINT instead of BOOLEAN.
     *   <li>TO_NULLABLE: ignores all NOT NULL constraints (except for primary keys).
     *   <li>TO_STRING: maps all MySQL types to STRING.
     *   <li>CHAR_TO_STRING: maps MySQL CHAR(length)/VARCHAR(length) types to STRING.
     *   <li>LONGTEXT_TO_BYTES: maps MySQL LONGTEXT types to BYTES.
     *   <li>BIGINT_UNSIGNED_TO_BIGINT: maps MySQL BIGINT UNSIGNED types to Paimon BIGINT. Notice
     *       that there is potential overflow risk, and users should ensure the overflow won't
     *       occur.
     *   <li>DECIMAL_NO_CHANGE: Ignore DECIMAL type change.
     *   <li>NO_CHANGE: Ignore any type change
     * </ul>
     */
    public enum TypeMappingMode {
        TINYINT1_NOT_BOOL,
        TO_NULLABLE,
        TO_STRING,
        CHAR_TO_STRING,
        LONGTEXT_TO_BYTES,
        BIGINT_UNSIGNED_TO_BIGINT,
        DECIMAL_NO_CHANGE,
        NO_CHANGE;

        private static final Map<String, TypeMappingMode> TYPE_MAPPING_OPTIONS =
                Arrays.stream(TypeMappingMode.values())
                        .collect(
                                Collectors.toMap(
                                        TypeMappingMode::configString, Function.identity()));

        public static TypeMappingMode mode(String option) {
            TypeMappingMode typeMappingMode = TYPE_MAPPING_OPTIONS.get(option);
            if (typeMappingMode == null) {
                throw new UnsupportedOperationException(
                        "Unsupported type mapping option: " + option);
            }
            return typeMappingMode;
        }

        public String configString() {
            return name().toLowerCase().replace("_", "-");
        }
    }
}
