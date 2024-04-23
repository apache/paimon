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

package org.apache.paimon.jdbc;

class DistributedLockDialectFactory {
    static JdbcDistributedLockDialect create(String protocol) {
        JdbcProtocol type = JdbcProtocol.valueOf(protocol.toUpperCase());
        switch (type) {
            case SQLITE:
                return new SqlLiteDistributedLockDialect();
            case MYSQL:
                return new MysqlDistributedLockDialect();
            default:
                throw new UnsupportedOperationException(
                        String.format("Distributed locks based on %s are not supported", protocol));
        }
    }

    /** Supported jdbc protocol. */
    enum JdbcProtocol {
        SQLITE,
        MARIADB,
        MYSQL
    }
}
