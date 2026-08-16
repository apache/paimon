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

package org.apache.paimon.index.pk;

import org.apache.paimon.options.Options;

/** Resolved definition of one source-backed primary-key index. */
public class PrimaryKeyIndexDefinition {

    /** Built-in primary-key index families. */
    public enum Family {
        VECTOR,
        BTREE,
        BITMAP,
        FULL_TEXT
    }

    private final String column;
    private final int fieldId;
    private final String indexType;
    private final Options options;
    private final Family family;

    public PrimaryKeyIndexDefinition(
            String column, int fieldId, String indexType, Options options, Family family) {
        this.column = column;
        this.fieldId = fieldId;
        this.indexType = indexType;
        this.options = options;
        this.family = family;
    }

    public String column() {
        return column;
    }

    public int fieldId() {
        return fieldId;
    }

    public String indexType() {
        return indexType;
    }

    public Options options() {
        return options;
    }

    public Family family() {
        return family;
    }
}
