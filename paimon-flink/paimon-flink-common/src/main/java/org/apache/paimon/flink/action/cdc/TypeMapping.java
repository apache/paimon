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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;

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
        List<String> options =
                Arrays.stream(rawOptions)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .collect(Collectors.toList());

        Set<TypeMappingMode> typeMappingModes = new HashSet<>();

        for (String option : options) {
            switch (option.toLowerCase()) {
                case "tinyint1-not-bool":
                    typeMappingModes.add(TINYINT1_NOT_BOOL);
                    break;
                case "to-nullable":
                    typeMappingModes.add(TO_NULLABLE);
                    break;
                case "to-string":
                    typeMappingModes.add(TO_STRING);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type mapping option: " + option);
            }
        }

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
     * </ul>
     */
    public enum TypeMappingMode {
        TINYINT1_NOT_BOOL,
        TO_NULLABLE,
        TO_STRING
    }
}
