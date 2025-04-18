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

package org.apache.paimon.flink.clone.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.CloneHiveAction;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.migrate.HiveCloneUtils;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Utils for building {@link CloneHiveAction}. */
public class CloneHiveUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CloneHiveUtils.class);

    public static DataStream<Tuple2<Identifier, Identifier>> buildSource(
            String sourceDatabase,
            String sourceTableName,
            String targetDatabase,
            String targetTableName,
            Catalog sourceCatalog,
            StreamExecutionEnvironment env)
            throws Exception {
        List<Tuple2<Identifier, Identifier>> result = new ArrayList<>();
        HiveCatalog hiveCatalog = getRootHiveCatalog(sourceCatalog);
        if (StringUtils.isNullOrWhitespaceOnly(sourceDatabase)) {
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(sourceTableName),
                    "sourceTableName must be blank when database is null.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must be blank when clone all tables in a catalog.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");

            for (Identifier identifier : HiveCloneUtils.listTables(hiveCatalog)) {
                result.add(new Tuple2<>(identifier, identifier));
            }
        } else if (StringUtils.isNullOrWhitespaceOnly(sourceTableName)) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone all tables in a database.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");

            for (Identifier identifier : HiveCloneUtils.listTables(hiveCatalog, sourceDatabase)) {
                result.add(
                        new Tuple2<>(
                                identifier,
                                Identifier.create(targetDatabase, identifier.getObjectName())));
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
                            Identifier.create(sourceDatabase, sourceTableName),
                            Identifier.create(targetDatabase, targetTableName)));
        }

        checkState(!result.isEmpty(), "Didn't find any table in source catalog.");

        if (LOG.isDebugEnabled()) {
            LOG.debug("The clone identifiers of source table and target table are: {}", result);
        }
        return env.fromCollection(result).forceNonParallel();
    }

    public static HiveCatalog getRootHiveCatalog(Catalog catalog) {
        Catalog rootCatalog = DelegateCatalog.rootCatalog(catalog);
        checkArgument(
                rootCatalog instanceof HiveCatalog,
                "Only support HiveCatalog now but found %s.",
                rootCatalog.getClass().getName());
        return (HiveCatalog) rootCatalog;
    }

    // ---------------------------------- Classes ----------------------------------

    /** Shuffle tables. */
    public static class TableChannelComputer
            implements ChannelComputer<Tuple2<Identifier, Identifier>> {

        private static final long serialVersionUID = 1L;

        private transient int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(Tuple2<Identifier, Identifier> record) {
            return Math.floorMod(
                    Objects.hash(record.f1.getDatabaseName(), record.f1.getTableName()),
                    numChannels);
        }

        @Override
        public String toString() {
            return "shuffle by identifier hash";
        }
    }

    /** Shuffle tables. */
    public static class DataFileChannelComputer implements ChannelComputer<DataFileInfo> {

        private static final long serialVersionUID = 1L;

        private transient int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(DataFileInfo record) {
            return Math.floorMod(Objects.hash(record.identifier()), numChannels);
        }

        @Override
        public String toString() {
            return "shuffle by identifier hash";
        }
    }
}
