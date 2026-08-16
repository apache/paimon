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

import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.CatalogOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.ParameterUtils.parseKeyValueList;
import static org.apache.paimon.utils.ParameterUtils.parseKeyValueString;

/** Factory to create {@link Action}. */
public interface ActionFactory extends Factory {

    Logger LOG = LoggerFactory.getLogger(ActionFactory.class);

    String HELP = "help";
    String WAREHOUSE = "warehouse";
    String DATABASE = "database";
    String TABLE = "table";
    @Deprecated String PATH = "path";
    String CATALOG_CONF = "catalog_conf";
    String TABLE_CONF = "table_conf";
    String PARTITION = "partition";
    String EXPIRATIONTIME = "expiration_time";
    String TIMESTAMPFORMATTER = "timestamp_formatter";
    String EXPIRE_STRATEGY = "expire_strategy";
    String TIMESTAMP_PATTERN = "timestamp_pattern";
    // Supports `full` and `minor`.
    String COMPACT_STRATEGY = "compact_strategy";
    String MINOR = "minor";
    String FULL = "full";

    /**
     * Forces the action to run as a Flink job instead of local execution. This parameter only
     * affects {@link LocalAction} implementations. For non-LocalAction implementations, this
     * parameter has no effect.
     */
    String FORCE_START_FLINK_JOB = "force_start_flink_job";

    Optional<Action> create(MultipleParameterToolAdapter params);

    static Optional<Action> createAction(String[] args) {
        // to be compatible with old usage
        String action = args[0].toLowerCase().replaceAll("-", "_");
        String[] actionArgs = Arrays.copyOfRange(args, 1, args.length);
        ActionFactory actionFactory;
        try {
            actionFactory =
                    FactoryUtil.discoverFactory(
                            ActionFactory.class.getClassLoader(), ActionFactory.class, action);
        } catch (FactoryException e) {
            printDefaultHelp();
            throw new UnsupportedOperationException("Unknown action \"" + action + "\".");
        }

        LOG.info("{} job args: {}", actionFactory.identifier(), String.join(" ", actionArgs));

        MultipleParameterToolAdapter params = new MultipleParameterToolAdapter(actionArgs);
        if (params.has(HELP)) {
            actionFactory.printHelp();
            return Optional.empty();
        }

        if (params.has(PATH)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Parameter '%s' is deprecated. Please use '--%s %s=<warehouse>' to specify warehouse if needed, "
                                    + "and use '%s' to specify database and '%s' to specify table.",
                            PATH, CATALOG_CONF, CatalogOptions.WAREHOUSE.key(), DATABASE, TABLE));
        }

        Optional<Action> optionalAction = actionFactory.create(params);

        if (params.has(FORCE_START_FLINK_JOB)) {
            optionalAction =
                    optionalAction.map(
                            a -> {
                                if (!(a instanceof ActionBase)) {
                                    throw new UnsupportedOperationException(
                                            String.format(
                                                    "Action %s does not support %s.",
                                                    action, FORCE_START_FLINK_JOB));
                                }
                                // Refer to Flink's AbstractParameterTool.NO_VALUE_KEY
                                if ("__NO_VALUE_KEY".equals(params.get(FORCE_START_FLINK_JOB))) {
                                    throw new IllegalArgumentException(
                                            "Please specify the value for parameter "
                                                    + FORCE_START_FLINK_JOB);
                                }
                                return ((ActionBase) a)
                                        .forceStartFlinkJob(
                                                Boolean.parseBoolean(
                                                        params.get(FORCE_START_FLINK_JOB)));
                            });
        }

        return optionalAction;
    }

    void printHelp();

    static void printDefaultHelp() {
        System.out.println("Usage: <action> [OPTIONS]");
        System.out.println();

        System.out.println("Available actions:");
        List<String> identifiers =
                FactoryUtil.discoverIdentifiers(
                        ActionFactory.class.getClassLoader(), ActionFactory.class);
        identifiers.forEach(action -> System.out.println("  " + action));
        System.out.println("For detailed options of each action, run <action> --help");
    }

    default List<Map<String, String>> getPartitions(MultipleParameterToolAdapter params) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : params.getMultiParameter(PARTITION)) {
            Map<String, String> kvs = parseCommaSeparatedKeyValues(partition);
            partitions.add(kvs);
        }

        return partitions;
    }

    default Map<String, String> optionalConfigMap(MultipleParameterToolAdapter params, String key) {
        if (!params.has(key)) {
            return Collections.emptyMap();
        }

        Map<String, String> config = new HashMap<>();
        for (String kvString : params.getMultiParameter(key)) {
            parseKeyValueString(config, kvString);
        }
        return config;
    }

    default Map<String, List<String>> optionalConfigMapList(
            MultipleParameterToolAdapter params, String key) {
        if (!params.has(key)) {
            return Collections.emptyMap();
        }

        Map<String, List<String>> config = new HashMap<>();
        for (String kvString : params.getMultiParameter(key)) {
            parseKeyValueList(config, kvString);
        }
        return config;
    }

    default Map<String, String> catalogConfigMap(MultipleParameterToolAdapter params) {
        Map<String, String> catalogConfig = new HashMap<>(optionalConfigMap(params, CATALOG_CONF));
        String warehouse = params.get(WAREHOUSE);
        if (warehouse != null && !catalogConfig.containsKey(WAREHOUSE)) {
            catalogConfig.put(WAREHOUSE, warehouse);
        }
        return catalogConfig;
    }
}
