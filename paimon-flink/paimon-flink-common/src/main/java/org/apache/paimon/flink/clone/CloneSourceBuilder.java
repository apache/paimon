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

package org.apache.paimon.flink.clone;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Pick the tables to be cloned based on the user input parameters. The record type of the build
 * DataStream is {@link Tuple2}. The left element is the identifier of source table and the right
 * element is the identifier of target table.
 */
public class CloneSourceBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CloneSourceBuilder.class);

    private final StreamExecutionEnvironment env;
    private final Map<String, String> sourceCatalogConfig;
    private final String database;
    private final String tableName;
    private final String targetDatabase;
    private final String targetTableName;

    public CloneSourceBuilder(
            StreamExecutionEnvironment env,
            Map<String, String> sourceCatalogConfig,
            String database,
            String tableName,
            String targetDatabase,
            String targetTableName) {
        this.env = env;
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.database = database;
        this.tableName = tableName;
        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
    }

    public DataStream<Tuple2<String, String>> build() throws Exception {
        try (Catalog sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig))) {
            return build(sourceCatalog);
        }
    }

    private DataStream<Tuple2<String, String>> build(Catalog sourceCatalog) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();

        if (StringUtils.isNullOrWhitespaceOnly(database)) {
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(tableName),
                    "tableName must be blank when database is null.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must be blank when clone all tables in a catalog.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");
            for (String db : sourceCatalog.listDatabases()) {
                for (String table : sourceCatalog.listTables(db)) {
                    String s = db + "." + table;
                    result.add(new Tuple2<>(s, s));
                }
            }
        } else if (StringUtils.isNullOrWhitespaceOnly(tableName)) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone all tables in a database.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");
            for (String table : sourceCatalog.listTables(database)) {
                result.add(new Tuple2<>(database + "." + table, targetDatabase + "." + table));
            }
        } else {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone a table.");
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must not be blank when clone a table.");
            result.add(
                    new Tuple2<>(
                            database + "." + tableName, targetDatabase + "." + targetTableName));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("The clone identifiers of source table and target table are: {}", result);
        }
        return env.fromCollection(result).forceNonParallel().forward();
    }
}
