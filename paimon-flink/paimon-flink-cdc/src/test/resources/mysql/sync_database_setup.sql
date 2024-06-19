-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant the test user 'paimonuser' all privileges:
--
GRANT ALL PRIVILEGES ON *.* TO 'paimonuser'@'%';

-- ################################################################################
--  MySqlSyncDatabaseActionITCase
-- ################################################################################

CREATE DATABASE paimon_sync_database;
USE paimon_sync_database;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);

-- no primary key, should be ignored
CREATE TABLE t3 (
    v1 INT
);

-- to make sure we use JDBC Driver correctly
CREATE DATABASE paimon_sync_database1;
USE paimon_sync_database1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);

-- no primary key, should be ignored
CREATE TABLE t3 (
    v1 INT
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testIgnoreIncompatibleTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_ignore_incompatible;
USE paimon_sync_database_ignore_incompatible;

CREATE TABLE incompatible (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE compatible (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testTableAffix
-- ################################################################################

CREATE DATABASE paimon_sync_database_affix;
USE paimon_sync_database_affix;

CREATE TABLE t1 (
    k1 INT,
    v0 VARCHAR(10),
    PRIMARY KEY (k1)
);

CREATE TABLE t2 (
    k2 INT,
    v0 VARCHAR(10),
    PRIMARY KEY (k2)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testIncludingTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_including;
USE paimon_sync_database_including;

CREATE TABLE paimon_1 (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE paimon_2 (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE flink (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE ignored (
    k INT,
    PRIMARY KEY (k)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testExcludingTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_excluding;
USE paimon_sync_database_excluding;

CREATE TABLE paimon_1 (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE paimon_2 (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE flink (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE sync (
    k INT,
    PRIMARY KEY (k)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testIncludingAndExcludingTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_in_excluding;
USE paimon_sync_database_in_excluding;

CREATE TABLE paimon_1 (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE paimon_2 (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE flink (
    k INT,
    PRIMARY KEY (k)
);

CREATE TABLE test (
    k INT,
    PRIMARY KEY (k)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testIgnoreCaseDivided
-- ################################################################################

CREATE DATABASE paimon_ignore_CASE_divided;
USE paimon_ignore_CASE_divided;

CREATE TABLE T (
    k INT,
    UPPERCASE_V0 VARCHAR(20),
    PRIMARY KEY (k)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testIgnoreCaseCombined
-- ################################################################################

CREATE DATABASE paimon_ignore_CASE_combined;
USE paimon_ignore_CASE_combined;

CREATE TABLE T (
    k INT,
    UPPERCASE_V0 VARCHAR(20),
    PRIMARY KEY (k)
);


-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testNewlyAddedTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_newly_added_tables;
USE paimon_sync_database_newly_added_tables;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);

CREATE DATABASE paimon_sync_database_newly_added_tables_1;
USE paimon_sync_database_newly_added_tables_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);


CREATE DATABASE paimon_sync_database_newly_added_tables_2;
USE paimon_sync_database_newly_added_tables_2;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);


CREATE DATABASE paimon_sync_database_newly_added_tables_3;
USE paimon_sync_database_newly_added_tables_3;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);


CREATE DATABASE paimon_sync_database_newly_added_tables_4;
USE paimon_sync_database_newly_added_tables_4;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);

CREATE DATABASE paimon_sync_database_add_ignored_table;
USE paimon_sync_database_add_ignored_table;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE a (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE DATABASE many_table_sync_test;
USE many_table_sync_test;

CREATE TABLE a (
    k INT,
    v VARCHAR(10),
    PRIMARY KEY (k)
);

-- ################################################################################
--  testSyncMultipleShards
-- ################################################################################

CREATE DATABASE database_shard_1;
USE database_shard_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k BIGINT,
    v1 DOUBLE,
    PRIMARY KEY (k)
);

CREATE TABLE t3 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE DATABASE database_shard_2;
USE database_shard_2;

-- test schema merging
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(20),
    v2 BIGINT,
    PRIMARY KEY (k)
);

-- test schema evolution
CREATE TABLE t2 (
    k BIGINT,
    v1 DOUBLE,
    PRIMARY KEY (k)
);

-- test some shard doesn't have primary key
CREATE TABLE t3 (
    k INT,
    v1 VARCHAR(10)
);

-- ################################################################################
--  testSyncMultipleShardsWithoutMerging
-- ################################################################################

CREATE DATABASE without_merging_shard_1;
USE without_merging_shard_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE DATABASE without_merging_shard_2;
USE without_merging_shard_2;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(20),
    v2 BIGINT,
    PRIMARY KEY (k)
);

-- test some shard doesn't have primary key
CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10)
);

-- ################################################################################
--  testMonitoredAndExcludedTablesWithMering
-- ################################################################################

CREATE DATABASE monitored_and_excluded_shard_1;
USE monitored_and_excluded_shard_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t3 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);


CREATE DATABASE monitored_and_excluded_shard_2;
USE monitored_and_excluded_shard_2;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k INT,
    v2 DOUBLE,
    PRIMARY KEY (k)
);

CREATE TABLE t3 (
    k INT,
    v2 VARCHAR(10)
);

-- ################################################################################
--  MySqlSyncDatabaseActionITCase#testNewlyAddedTablesOptionsChange
-- ################################################################################

CREATE DATABASE newly_added_tables_option_schange;
USE newly_added_tables_option_schange;

CREATE TABLE t1 (
   k INT,
   v1 VARCHAR(10),
   PRIMARY KEY (k)
);

-- ################################################################################
--  testMetadataColumns
-- ################################################################################

CREATE DATABASE metadata;
USE metadata;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
