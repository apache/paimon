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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * An enumeration of Data type families for clustering {@link DataTypeRoot}s into categories.
 *
 * <p>The enumeration is very close to the SQL standard in terms of naming and completeness.
 * However, it reflects just a subset of the evolving standard and contains some extensions
 * (indicated by {@code EXTENSION}).
 *
 * @since 0.4.0
 */
@Public
public enum DataTypeFamily {
    PREDEFINED,

    CONSTRUCTED,

    CHARACTER_STRING,

    BINARY_STRING,

    NUMERIC,

    INTEGER_NUMERIC,

    EXACT_NUMERIC,

    APPROXIMATE_NUMERIC,

    DATETIME,

    TIME,

    TIMESTAMP,

    COLLECTION,

    EXTENSION
}
