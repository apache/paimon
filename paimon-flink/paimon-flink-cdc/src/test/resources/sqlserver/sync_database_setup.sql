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

-- ################################################################################
--  SqlServerSyncDatabaseActionITCase
-- ################################################################################

CREATE DATABASE paimon_sync_database;
USE paimon_sync_database;
EXEC sys.sp_cdc_enable_db;

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
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3', @role_name = NULL, @supports_net_changes = 0;


-- ################################################################################
--  SqlServerSyncDatabaseActionITCase#testIgnoreIncompatibleTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_ignore_incompatible;
USE paimon_sync_database_ignore_incompatible;
EXEC sys.sp_cdc_enable_db;

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

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'incompatible', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'compatible', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  SqlServerSyncDatabaseActionITCase#testTableAffix
-- ################################################################################

CREATE DATABASE paimon_sync_database_affix;
USE paimon_sync_database_affix;
EXEC sys.sp_cdc_enable_db;

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

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  SqlServerSyncDatabaseActionITCase#testIncludingTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_including;
USE paimon_sync_database_including;
EXEC sys.sp_cdc_enable_db;

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

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'paimon_1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'paimon_2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flink', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'ignored', @role_name = NULL, @supports_net_changes = 0;


-- ################################################################################
--  SqlServerSyncDatabaseActionITCase#testExcludingTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_excluding;
USE paimon_sync_database_excluding;
EXEC sys.sp_cdc_enable_db;

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
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'paimon_1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'paimon_2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flink', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sync', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  SqlServerSyncDatabaseActionITCase#testIncludingAndExcludingTables
-- ################################################################################

CREATE DATABASE paimon_sync_database_in_excluding;
USE paimon_sync_database_in_excluding;
EXEC sys.sp_cdc_enable_db;

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
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'paimon_1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'paimon_2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'flink', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'test', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  SqlServerSyncDatabaseActionITCase#testIgnoreCase
-- ################################################################################

CREATE DATABASE paimon_ignore_CASE;
USE paimon_ignore_CASE;
EXEC sys.sp_cdc_enable_db;

CREATE TABLE T (
    k INT,
    UPPERCASE_V0 VARCHAR(20),
    PRIMARY KEY (k)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'T', @role_name = NULL, @supports_net_changes = 0;


-- ################################################################################
--  testSyncMultipleShards
-- ################################################################################

CREATE DATABASE database_shard;
USE database_shard;
EXEC sys.sp_cdc_enable_db;

create schema schema_1;
CREATE TABLE schema_1.t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE schema_1.t2 (
    k BIGINT,
    v1 FLOAT,
    PRIMARY KEY (k)
);

CREATE TABLE schema_1.t3 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

EXEC sys.sp_cdc_enable_table @source_schema = 'schema_1', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_1', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_1', @source_name = 't3', @role_name = NULL, @supports_net_changes = 0;

create schema schema_2;
-- test schema merging
CREATE TABLE schema_2.t1 (
    k INT,
    v1 VARCHAR(20),
    v2 BIGINT,
    PRIMARY KEY (k)
);

-- test schema evolution
CREATE TABLE schema_2.t2 (
    k BIGINT,
    v1 FLOAT,
    PRIMARY KEY (k)
);

-- test some shard doesn't have primary key
CREATE TABLE schema_2.t3 (
    k INT,
    v1 VARCHAR(10)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_2', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_2', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_2', @source_name = 't3', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  testSyncMultipleShardsWithoutMerging
-- ################################################################################

CREATE DATABASE without_merging_shard;
USE without_merging_shard;
EXEC sys.sp_cdc_enable_db;

CREATE SCHEMA schema_1;
CREATE TABLE schema_1.t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

CREATE TABLE schema_1.t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

EXEC sys.sp_cdc_enable_table @source_schema = 'schema_1', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_1', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;

CREATE SCHEMA schema_2;
CREATE TABLE schema_2.t1 (
    k INT,
    v1 VARCHAR(20),
    v2 BIGINT,
    PRIMARY KEY (k)
);

-- test some shard doesn't have primary key
CREATE TABLE schema_2.t2 (
    k INT,
    v1 VARCHAR(10)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_2', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'schema_2', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  testMetadataColumns
-- ################################################################################

CREATE DATABASE metadata;
USE metadata;
EXEC sys.sp_cdc_enable_db;

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
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
