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

package org.apache.paimon.flink.action;

import java.util.Optional;

/** Factory to create {@link CompactManifestAction}. */
public class CompactManifestActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact_manifest";

    private static final String OPTIONS = "options";
    private static final String DRY_RUN = "dry_run";
    private static final String MANIFEST_SORT_ENABLED = "manifest_sort.enabled";
    private static final String MANIFEST_SORT_PARTITION_FIELD = "manifest_sort.partition_field";
    private static final String MANIFEST_SORT_MAX_REWRITE_SIZE = "manifest_sort.max_rewrite_size";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        CompactManifestAction action =
                new CompactManifestAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params),
                        params.get(OPTIONS),
                        params.getBoolean(DRY_RUN, false),
                        params.getBoolean(MANIFEST_SORT_ENABLED, null),
                        params.get(MANIFEST_SORT_PARTITION_FIELD),
                        params.get(MANIFEST_SORT_MAX_REWRITE_SIZE));
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"compact_manifest\" compacts manifest files of the specified table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact_manifest \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <table_name> \\\n"
                        + "[--options <key1=value1,key2=value2>] \\\n"
                        + "[--dry_run <true|false>] \\\n"
                        + "[--manifest-sort.enabled <true|false>] \\\n"
                        + "[--manifest-sort.partition-field <partition_field>] \\\n"
                        + "[--manifest-sort.max-rewrite-size <memory_size>]");
        System.out.println();

        System.out.println("Example:");
        System.out.println(
                "  compact_manifest --warehouse s3://path/to/warehouse \\\n"
                        + "--database default --table T \\\n"
                        + "--manifest-sort.enabled true \\\n"
                        + "--manifest-sort.partition-field dt \\\n"
                        + "--manifest-sort.max-rewrite-size 1gb");
    }
}
