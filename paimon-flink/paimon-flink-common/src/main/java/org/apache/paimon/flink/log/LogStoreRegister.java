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

package org.apache.paimon.flink.log;

import org.apache.paimon.catalog.Identifier;

import java.util.Map;

/**
 * {@link LogStoreRegister} will register and unregister topic for a Paimon table, you can implement
 * it for customized log system management.
 */
public interface LogStoreRegister {
    /**
     * Register topic in log system for the table.
     *
     * @param table the created table identifier
     * @param options the table options
     * @return options for the topic in log system which will be added to table
     */
    Map<String, String> registerTopic(Identifier table, Map<String, String> options);

    /**
     * Unregister topic in log system for the table.
     *
     * @param table the table identifier
     * @param options the dropped table
     */
    void unRegisterTopic(Identifier table, Map<String, String> options);
}
