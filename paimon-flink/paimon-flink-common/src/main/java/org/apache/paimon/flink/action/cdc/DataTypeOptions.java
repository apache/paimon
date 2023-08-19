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
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.DataTypeOptions.DataTypeMapMode.ALL_TO_STRING;
import static org.apache.paimon.flink.action.cdc.DataTypeOptions.DataTypeMapMode.IDENTITY;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utility that holds data type conversion options. */
public class DataTypeOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataTypeMapMode dataTypeMapMode;
    private final boolean tinyint1NotBool;
    private final boolean ignoreNotNull;

    public DataTypeOptions(
            DataTypeMapMode dataTypeMapMode, boolean tinyint1NotBool, boolean ignoreNotNull) {
        this.dataTypeMapMode = dataTypeMapMode;
        this.tinyint1NotBool = tinyint1NotBool;
        this.ignoreNotNull = ignoreNotNull;
    }

    public DataTypeMapMode dataTypeMapMode() {
        return dataTypeMapMode;
    }

    public boolean tinyint1NotBool() {
        return tinyint1NotBool;
    }

    public boolean ignoreNotNull() {
        return ignoreNotNull;
    }

    public static DataTypeOptions defaultOptions() {
        return new DataTypeOptions(IDENTITY, false, false);
    }

    public static DataTypeOptions parse(String[] rawOptions) {
        List<String> options =
                Arrays.stream(rawOptions)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .collect(Collectors.toList());
        checkArgument(
                !(options.contains("identity") && options.contains("all-to-string")),
                "Cannot specify more than one data type map modes.");

        DataTypeMapMode dataTypeMapMode = IDENTITY;
        boolean tinyint1NotBool = false;
        boolean ignoreNotNull = false;

        for (String option : options) {
            switch (option.toLowerCase()) {
                case "identity":
                    dataTypeMapMode = IDENTITY;
                    break;
                case "all-to-string":
                    dataTypeMapMode = ALL_TO_STRING;
                    break;
                case "ignore-not-null":
                    ignoreNotNull = true;
                    break;
                case "tinyint1-not-bool":
                    tinyint1NotBool = true;
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported data type option: " + option);
            }
        }

        return new DataTypeOptions(dataTypeMapMode, tinyint1NotBool, ignoreNotNull);
    }

    /**
     * Describe how to map MySQL data type to Paimon data type. Currently, there are two modes:
     *
     * <ul>
     *   <li>IDENTITY: keep the original types.
     *   <li>ALL_TO_STRING: map all MySQL types to STRING.
     * </ul>
     */
    public enum DataTypeMapMode {
        IDENTITY,
        ALL_TO_STRING;
    }
}
