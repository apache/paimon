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

import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.regex.Pattern;

/** Clear consumers action for Flink. */
public class ClearConsumerAction extends TableActionBase implements LocalAction {

    private String includingConsumers;
    private String excludingConsumers;

    protected ClearConsumerAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public ClearConsumerAction withIncludingConsumers(@Nullable String includingConsumers) {
        this.includingConsumers = includingConsumers;
        return this;
    }

    public ClearConsumerAction withExcludingConsumers(@Nullable String excludingConsumers) {
        this.excludingConsumers = excludingConsumers;
        return this;
    }

    @Override
    public void executeLocally() {
        FileStoreTable dataTable = (FileStoreTable) table;
        ConsumerManager consumerManager =
                new ConsumerManager(
                        dataTable.fileIO(),
                        dataTable.location(),
                        dataTable.snapshotManager().branch());

        Pattern includingPattern =
                StringUtils.isNullOrWhitespaceOnly(includingConsumers)
                        ? Pattern.compile(".*")
                        : Pattern.compile(includingConsumers);
        Pattern excludingPattern =
                StringUtils.isNullOrWhitespaceOnly(excludingConsumers)
                        ? null
                        : Pattern.compile(excludingConsumers);
        consumerManager.clearConsumers(includingPattern, excludingPattern);
    }
}
