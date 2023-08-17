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

import javax.annotation.Nullable;

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

    public static DataTypeMapMode fromString(@Nullable String mode) {
        if (mode == null) {
            return IDENTITY;
        }

        switch (mode.toLowerCase()) {
            case "identity":
                return IDENTITY;
            case "all-to-string":
                return ALL_TO_STRING;
            default:
                throw new UnsupportedOperationException("Unsupported data type map mode: " + mode);
        }
    }
}
