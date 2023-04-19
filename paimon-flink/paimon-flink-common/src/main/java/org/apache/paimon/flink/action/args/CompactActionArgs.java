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

package org.apache.paimon.flink.action.args;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.CompactAction;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CompactActionArgs extends ActionArgs {

    @Parameter(
            names = {"--warehouse"},
            description = "warehouse is the path to Paimon warehouse.")
    private String warehouse;

    @Parameter(
            names = {"--database"},
            description = "database is the database name in Paimon catalog.")
    private String database;

    @Parameter(
            names = {"--table"},
            description = "table is the Paimon table name.")
    private String table;

    @DynamicParameter(
            names = {"--partition"},
            description = "partition key value pair, for example key1=value1 key2=value2")
    private Map<String, String> partition;

    @Override
    public Optional<Action> buildAction() {
        List<Map<String, String>> partitions = new ArrayList<>();
        partitions.add(partition);
        return Optional.of(
                new CompactAction(warehouse, database, table).withPartitions(partitions));
    }

    @Override
    public StringBuilder usage() {
        return CompactAction.printHelp();
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, String> getPartition() {
        return partition;
    }

    public void setPartition(Map<String, String> partition) {
        this.partition = partition;
    }
}
