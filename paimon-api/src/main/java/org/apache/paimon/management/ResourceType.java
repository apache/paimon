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

package org.apache.paimon.management;

import javax.annotation.Nullable;

import java.util.Locale;

/** Resource types supported by permission management. */
public enum ResourceType {
    CATALOG,
    CATALOG_ALL,
    DATABASE,
    DATABASE_ALL,
    TABLE,
    VIEW,
    FUNCTION,
    COLUMN,
    ROW_FILTER,
    COLUMN_MASKING;

    @Nullable
    public static ResourceType fromString(@Nullable String value) {
        return value == null ? null : valueOf(value.toUpperCase(Locale.ROOT));
    }
}
