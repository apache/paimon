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

package org.apache.paimon.flink.lineage;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Lineage utilities for building {@link SourceLineageVertex} and {@link LineageVertex} from a
 * Paimon table name and its physical warehouse path (namespace).
 */
public class LineageUtils {

    private static final String PAIMON_DATASET_PREFIX = "paimon://";

    private static final Set<String> PAIMON_OPTION_KEYS =
            CoreOptions.getOptions().stream().map(opt -> opt.key()).collect(Collectors.toSet());

    /**
     * Builds the config map for a dataset facet from a {@link Table}. Includes filtered Paimon
     * {@link CoreOptions}, partition keys, primary keys, and the table comment (if present).
     */
    private static Map<String, String> buildConfigMap(Table table) {
        Map<String, String> config = new HashMap<>();
        config.put("partition-keys", String.join(",", table.partitionKeys()));
        config.put("primary-keys", String.join(",", table.primaryKeys()));

        table.options().entrySet().stream()
                .filter(e -> PAIMON_OPTION_KEYS.contains(e.getKey()))
                .forEach(e -> config.put(e.getKey(), e.getValue()));

        return config;
    }

    /**
     * Returns the lineage namespace for a Paimon table. The namespace uses the {@code paimon://}
     * scheme followed by the table's physical warehouse path, e.g. {@code
     * "paimon://s3://my-bucket/warehouse/mydb.db/mytable"}.
     */
    public static String getNamespace(Table table) {
        return PAIMON_DATASET_PREFIX + CoreOptions.path(table.options());
    }

    /**
     * Creates a {@link SourceLineageVertex} for a Paimon source table.
     *
     * @param name fully qualified table name, e.g. {@code "paimon.mydb.mytable"}
     * @param isBounded whether the source is bounded (batch) or unbounded (streaming)
     * @param table the Paimon table (namespace is derived from its {@code path} option)
     */
    public static SourceLineageVertex sourceLineageVertex(
            String name, boolean isBounded, Table table) {
        LineageDataset dataset =
                new PaimonLineageDataset(name, getNamespace(table), buildConfigMap(table));
        Boundedness boundedness =
                isBounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
        return new PaimonSourceLineageVertex(boundedness, Collections.singletonList(dataset));
    }

    /**
     * Creates a {@link LineageVertex} for a Paimon sink table.
     *
     * @param name fully qualified table name, e.g. {@code "paimon.mydb.mytable"}
     * @param table the Paimon table (namespace is derived from its {@code path} option)
     */
    public static LineageVertex sinkLineageVertex(String name, Table table) {
        LineageDataset dataset =
                new PaimonLineageDataset(name, getNamespace(table), buildConfigMap(table));
        return new PaimonSinkLineageVertex(Collections.singletonList(dataset));
    }
}
