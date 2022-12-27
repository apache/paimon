/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.table.SupportsWrite;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 * Spark {@link WriteBuilder}.
 *
 * <p>TODO: Support overwrite.
 */
public class SparkWriteBuilder implements WriteBuilder {

    private final SupportsWrite table;
    private final String queryId;
    private final Lock.Factory lockFactory;
    private final Configuration conf;

    public SparkWriteBuilder(
            SupportsWrite table, String queryId, Lock.Factory lockFactory, Configuration conf) {
        this.table = table;
        this.queryId = queryId;
        this.lockFactory = lockFactory;
        this.conf = conf;
    }

    @Override
    public Write build() {
        return new SparkWrite(table, queryId, lockFactory, conf);
    }
}
