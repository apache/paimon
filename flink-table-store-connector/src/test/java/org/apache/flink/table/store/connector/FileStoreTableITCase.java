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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.ROOT_PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;

/** ITCase for file store table api. */
public abstract class FileStoreTableITCase extends AbstractTestBase {

    protected TableEnvironment bEnv;
    protected TableEnvironment sEnv;
    protected String path;

    @Before
    public void before() throws IOException {
        bEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        sEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        sEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        path = TEMPORARY_FOLDER.newFolder().toURI().toString();
        prepareEnv(bEnv, path);
        prepareEnv(sEnv, path);
    }

    private void prepareEnv(TableEnvironment env, String path) {
        Configuration config = env.getConfig().getConfiguration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        config.setString(TABLE_STORE_PREFIX + ROOT_PATH.key(), path);
        ddl().forEach(env::executeSql);
    }

    protected abstract List<String> ddl();

    protected CloseableIterator<Row> streamSqlIter(String query, Object... args) {
        return sEnv.executeSql(String.format(query, args)).collect();
    }

    protected List<Row> batchSql(String query, Object... args) {
        TableResult tableResult = bEnv.executeSql(String.format(query, args));

        try (CloseableIterator<Row> iter = tableResult.collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect the table result.", e);
        }
    }
}
