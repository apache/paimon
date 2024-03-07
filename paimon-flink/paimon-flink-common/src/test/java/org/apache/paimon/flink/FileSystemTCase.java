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

package org.apache.paimon.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ITCase for paimon file system. */
public class FileSystemTCase {
    @Test
    public void testLoadJaxbApi() {
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        String ddl =
                "CREATE CATALOG paimon WITH (\n"
                        + "'type' = 'paimon',\n"
                        + "'fs.oss.endpoint' = 'oss.aliyuncs.com',\n"
                        + "'fs.oss.accessKeyId' = 'myAccessKeyID',\n"
                        + "'fs.oss.accessKeySecret' = 'myAccessKeySecret',\n"
                        + "'warehouse' = 'oss://object/path/'\n"
                        + ");";

        ValidationException validationException =
                assertThrows(ValidationException.class, () -> tEnv.executeSql(ddl));
        assertTrue(validationException.getCause().getMessage().contains("InvalidAccessKeyId"));
    }
}
