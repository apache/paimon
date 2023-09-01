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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Base test class for {@link org.apache.paimon.flink.action.Action}s related to MongoDB. */
public abstract class MongoDBActionITCaseBase extends CdcActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBActionITCaseBase.class);
    protected static MongoClient client;
    public static final MongoDBContainer MONGODB_CONTAINER =
            new MongoDBContainer("mongo:6.0.6")
                    .withSharding()
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        // MONGODB_CONTAINER.setPortBindings(Collections.singletonList("27017:27017"));
        Startables.deepStart(Stream.of(MONGODB_CONTAINER)).join();
        LOG.info("Containers are started.");
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(MONGODB_CONTAINER.getConnectionString()))
                        .build();
        client = MongoClients.create(settings);
    }

    protected Map<String, String> getBasicMongoDBConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("hosts", MONGODB_CONTAINER.getHostAndPort());
        return config;
    }

    protected String createRecordsToMongoDB(String fileName, String content) {
        return MONGODB_CONTAINER.executeCommandFileInSeparateDatabase(fileName, content);
    }

    protected static String writeRecordsToMongoDB(String fileName, String dbName, String content) {
        return MONGODB_CONTAINER.executeCommandFileInSeparateDatabase(fileName, dbName, content);
    }

    protected MongoDBSyncTableActionBuilder syncTableActionBuilder(
            Map<String, String> mongodbConfig) {
        return new MongoDBSyncTableActionBuilder(mongodbConfig);
    }

    protected MongoDBSyncDatabaseActionBuilder syncDatabaseActionBuilder(
            Map<String, String> mongodbConfig) {
        return new MongoDBSyncDatabaseActionBuilder(mongodbConfig);
    }

    /** Builder to build {@link MongoDBSyncTableAction} from action arguments. */
    protected class MongoDBSyncTableActionBuilder
            extends SyncTableActionBuilder<MongoDBSyncTableAction> {

        public MongoDBSyncTableActionBuilder(Map<String, String> mongodbConfig) {
            super(mongodbConfig);
        }

        public MongoDBSyncTableActionBuilder withPrimaryKeys(String... primaryKeys) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncTableActionBuilder withComputedColumnArgs(String... computedColumnArgs) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncTableActionBuilder withComputedColumnArgs(
                List<String> computedColumnArgs) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncTableActionBuilder withTypeMappingModes(String... typeMappingModes) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncTableAction build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    tableName));

            args.addAll(mapToArgs("--mongodb-conf", sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(listToArgs("--partition-keys", partitionKeys));

            MultipleParameterTool params =
                    MultipleParameterTool.fromArgs(args.toArray(args.toArray(new String[0])));
            return (MongoDBSyncTableAction)
                    new MongoDBSyncTableActionFactory()
                            .create(params)
                            .orElseThrow(RuntimeException::new);
        }
    }

    /** Builder to build {@link MongoDBSyncDatabaseAction} from action arguments. */
    protected class MongoDBSyncDatabaseActionBuilder
            extends SyncDatabaseActionBuilder<MongoDBSyncDatabaseAction> {

        public MongoDBSyncDatabaseActionBuilder(Map<String, String> mongodbConfig) {
            super(mongodbConfig);
        }

        public MongoDBSyncDatabaseActionBuilder ignoreIncompatible(boolean ignoreIncompatible) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncDatabaseActionBuilder mergeShards(boolean mergeShards) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncDatabaseActionBuilder withMode(String mode) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncDatabaseActionBuilder withTypeMappingModes(String... typeMappingModes) {
            throw new UnsupportedOperationException();
        }

        public MongoDBSyncDatabaseAction build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList("--warehouse", warehouse, "--database", database));

            args.addAll(mapToArgs("--mongodb-conf", sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(nullableToArgs("--table-prefix", tablePrefix));
            args.addAll(nullableToArgs("--table-suffix", tableSuffix));
            args.addAll(nullableToArgs("--including-tables", includingTables));
            args.addAll(nullableToArgs("--excluding-tables", excludingTables));

            MultipleParameterTool params =
                    MultipleParameterTool.fromArgs(args.toArray(args.toArray(new String[0])));
            return (MongoDBSyncDatabaseAction)
                    new MongoDBSyncDatabaseActionFactory()
                            .create(params)
                            .orElseThrow(RuntimeException::new);
        }
    }
}
