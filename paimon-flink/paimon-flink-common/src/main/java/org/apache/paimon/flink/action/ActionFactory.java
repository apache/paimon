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

import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
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
import static org.apache.paimon.utils.ParameterUtils.parseKeyValueString;

/** Factory to create {@link Action}. */
public interface ActionFactory extends Factory {

    Logger LOG = LoggerFactory.getLogger(ActionFactory.class);

    String HELP = "help";
    String WAREHOUSE = "warehouse";
    String DATABASE = "database";
    String TABLE = "table";
    String PATH = "path";
    String CATALOG_CONF = "catalog_conf";
    String TABLE_CONF = "table_conf";
    String PARTITION = "partition";
    String EXPIRATIONTIME = "expiration_time";
    String TIMESTAMPFORMATTER = "timestamp_formatter";

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

        MultipleParameterToolAdapter params =
                new MultipleParameterToolAdapter(MultipleParameterTool.fromArgs(actionArgs));
        if (params.has(HELP)) {
            actionFactory.printHelp();
            return Optional.empty();
        }

        return actionFactory.create(params);
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

    default Tuple3<String, String, String> getTablePath(MultipleParameterToolAdapter params) {
        String warehouse = params.get(WAREHOUSE);
        String database = params.get(DATABASE);
        String table = params.get(TABLE);
        String path = params.get(PATH);

        Tuple3<String, String, String> tablePath = null;
        int count = 0;
        if (warehouse != null || database != null || table != null) {
            if (warehouse == null || database == null || table == null) {
                throw new IllegalArgumentException(
                        "Warehouse, database and table must be specified all at once to specify a table.");
            }
            tablePath = Tuple3.of(warehouse, database, table);
            count++;
        }
        if (path != null) {
            tablePath =
                    Tuple3.of(
                            CatalogUtils.warehouse(path),
                            CatalogUtils.database(path),
                            CatalogUtils.table(path));
            count++;
        }

        if (count != 1) {
            throw new IllegalArgumentException(
                    "Please specify either \"warehouse, database and table\" or \"path\".");
        }

        return tablePath;
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

    default void checkRequiredArgument(MultipleParameterToolAdapter params, String key) {
        Preconditions.checkArgument(
                params.has(key), "Argument '%s' is required. Run '<action> --help' for help.", key);
    }

    default String getRequiredValue(MultipleParameterToolAdapter params, String key) {
        checkRequiredArgument(params, key);
        return params.get(key);
    }
}
