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

package org.apache.paimon.data.shredding;

/** Constants for the shared-shredding MAP storage layout. */
public class MapSharedShreddingDefine {

    public static final String VERSION = "paimon.map.shared-shredding.version";
    public static final int CURRENT_VERSION = 1;
    public static final String FIELD_DICT = "paimon.map.shared-shredding.field-dict";
    public static final String FIELD_DICT_ORIGINAL_SIZE =
            "paimon.map.shared-shredding.field-dict-original-size";
    public static final String FIELD_COLUMNS = "paimon.map.shared-shredding.field-columns";
    public static final String OVERFLOW_SET = "paimon.map.shared-shredding.overflow-set";
    public static final String NUM_COLUMNS = "paimon.map.shared-shredding.num-columns";
    public static final String MAX_ROW_WIDTH = "paimon.map.shared-shredding.max-row-width";

    public static final String FIELD_MAPPING = "__field_mapping";
    public static final String OVERFLOW = "__overflow";

    public static final String DEFAULT_DICT_COMPRESSION = "zstd";

    public static String physicalColumnName(int index) {
        return "__col_" + index;
    }

    private MapSharedShreddingDefine() {}
}
