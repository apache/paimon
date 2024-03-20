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

/** Distributed lock implementation based on mysql table. */
public class MysqlDistributedLockDialect extends AbstractDistributedLockDialect {

    @Override
    public String getCreateTableSql() {
        return "CREATE TABLE "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + "("
                + JdbcUtils.LOCK_ID
                + " VARCHAR(%s) NOT NULL,"
                + JdbcUtils.ACQUIRED_AT
                + " TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                + JdbcUtils.EXPIRE_TIME
                + " BIGINT DEFAULT 0 NOT NULL,"
                + "PRIMARY KEY ("
                + JdbcUtils.LOCK_ID
                + ")"
                + ")";
    }

    @Override
    public String getLockAcquireSql() {
        return "INSERT INTO "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " ("
                + JdbcUtils.LOCK_ID
                + ","
                + JdbcUtils.EXPIRE_TIME
                + ") VALUES (?,?)";
    }

    @Override
    public String getReleaseLockSql() {
        return "DELETE FROM "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " WHERE "
                + JdbcUtils.LOCK_ID
                + " = ?";
    }

    @Override
    public String getTryReleaseTimedOutLock() {
        return "DELETE FROM "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " WHERE TIMESTAMPDIFF(SECOND, "
                + JdbcUtils.ACQUIRED_AT
                + ", NOW()) >"
                + JdbcUtils.EXPIRE_TIME
                + " and "
                + JdbcUtils.LOCK_ID
                + " = ?";
    }
}
