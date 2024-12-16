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

package org.apache.paimon.flink.utils;

import org.apache.flink.table.api.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class for having a unified string-based representation of Table API related classes such
 * as Schema, TypeInformation, etc.
 *
 * <p>Note to implementers: Please try to reuse key names as much as possible. Key-names should be
 * hierarchical and lower case. Use "-" instead of dots or camel case. E.g.,
 * connector.schema.start-from = from-earliest. Try not to use the higher level in a key-name. E.g.,
 * instead of connector.kafka.kafka-version use connector.kafka.version.
 *
 * <p>Properties with key normalization enabled contain only lower-case keys.
 */
public class FlinkDescriptorProperties {

    public static final String NAME = "name";

    public static final String DATA_TYPE = "data-type";

    public static final String EXPR = "expr";

    public static final String METADATA = "metadata";

    public static final String VIRTUAL = "virtual";

    public static final String WATERMARK = "watermark";

    public static final String WATERMARK_ROWTIME = "rowtime";

    public static final String WATERMARK_STRATEGY = "strategy";

    public static final String WATERMARK_STRATEGY_EXPR = WATERMARK_STRATEGY + '.' + EXPR;

    public static final String WATERMARK_STRATEGY_DATA_TYPE = WATERMARK_STRATEGY + '.' + DATA_TYPE;

    public static final String PRIMARY_KEY_NAME = "primary-key.name";

    public static final String PRIMARY_KEY_COLUMNS = "primary-key.columns";

    public static final String COMMENT = "comment";

    public static void removeSchemaKeys(String key, Schema schema, Map<String, String> options) {
        checkNotNull(key);
        checkNotNull(schema);

        List<String> subKeys = Arrays.asList(NAME, DATA_TYPE, EXPR, METADATA, VIRTUAL);
        for (int idx = 0; idx < schema.getColumns().size(); idx++) {
            for (String subKey : subKeys) {
                options.remove(key + '.' + idx + '.' + subKey);
            }
        }

        if (!schema.getWatermarkSpecs().isEmpty()) {
            subKeys =
                    Arrays.asList(
                            WATERMARK_ROWTIME,
                            WATERMARK_STRATEGY_EXPR,
                            WATERMARK_STRATEGY_DATA_TYPE);
            for (int idx = 0; idx < schema.getWatermarkSpecs().size(); idx++) {
                for (String subKey : subKeys) {
                    options.remove(key + '.' + WATERMARK + '.' + idx + '.' + subKey);
                }
            }
        }

        schema.getPrimaryKey()
                .ifPresent(
                        pk -> {
                            options.remove(key + '.' + PRIMARY_KEY_NAME);
                            options.remove(key + '.' + PRIMARY_KEY_COLUMNS);
                        });
    }
}
