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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaField;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link LineageDataset} representing a Paimon table, identified by its fully qualified name and
 * catalog warehouse option as the namespace.
 */
public class PaimonLineageDataset implements LineageDataset {

    private final String name;
    private final String namespace;
    private final Map<String, String> tableOptions;
    @Nullable private final RowType rowType;

    public PaimonLineageDataset(String name, String namespace, Map<String, String> tableOptions) {
        this(name, namespace, tableOptions, null);
    }

    public PaimonLineageDataset(
            String name,
            String namespace,
            Map<String, String> tableOptions,
            @Nullable RowType rowType) {
        this.name = name;
        this.namespace = namespace;
        this.tableOptions = tableOptions;
        this.rowType = rowType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public Map<String, LineageDatasetFacet> facets() {
        Map<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(
                "config",
                new DatasetConfigFacet() {
                    @Override
                    public String name() {
                        return "config";
                    }

                    @Override
                    public Map<String, String> config() {
                        return tableOptions;
                    }
                });
        if (rowType != null) {
            facets.put(
                    "schema",
                    new DatasetSchemaFacet() {
                        @Override
                        public String name() {
                            return "schema";
                        }

                        @Override
                        public Map<String, DatasetSchemaField<String>> fields() {
                            Map<String, DatasetSchemaField<String>> result = new LinkedHashMap<>();
                            for (DataField field : rowType.getFields()) {
                                String fieldName = field.name();
                                String fieldType = field.type().asSQLString();
                                result.put(
                                        fieldName,
                                        new DatasetSchemaField<String>() {
                                            @Override
                                            public String name() {
                                                return fieldName;
                                            }

                                            @Override
                                            public String type() {
                                                return fieldType;
                                            }
                                        });
                            }
                            return result;
                        }
                    });
        }
        return facets;
    }
}
