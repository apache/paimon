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
--  PostgreSqlSyncDatabaseActionITCase
-- ################################################################################

DROP SCHEMA IF EXISTS test_schema CASCADE;
CREATE SCHEMA test_schema;
SET search_path TO test_schema;

CREATE TABLE test_table_01 (
    k1 INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k1)
);

CREATE TABLE test_table_02 (
    k2 INT,
    v2 VARCHAR(10),
    PRIMARY KEY (k2)
);

CREATE TABLE test_table_03 (
    k3 INT,
    v3 VARCHAR(10),
    PRIMARY KEY (k3)
);

CREATE TABLE test_table_04 (
    k4 INT,
    v4 VARCHAR(10),
    PRIMARY KEY (k4)
);

CREATE TABLE test_table_05 (
    k5 INT,
    v5 VARCHAR(10),
    PRIMARY KEY (k5)
);

CREATE TABLE test_table_06 (
    k6 INT,
    v6 VARCHAR(10),
    PRIMARY KEY (k6)
);

INSERT INTO test_table_01 VALUES (1, 'a1'),(3, 'a3');
INSERT INTO test_table_02 VALUES (2, 'a2'),(4, 'a4');

-- ################################################################################
--  PostgreSqlSyncDatabaseActionITCase#testIncludingAndExcludingTables
-- ################################################################################

DROP SCHEMA IF EXISTS paimon_sync_database_in_excluding CASCADE;
CREATE SCHEMA paimon_sync_database_in_excluding;
SET search_path TO paimon_sync_database_in_excluding;

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
--  PostgreSqlSyncDatabaseActionITCase#testTableAffix
-- ################################################################################

DROP SCHEMA IF EXISTS paimon_sync_database_affix CASCADE;
CREATE SCHEMA paimon_sync_database_affix;
SET search_path TO paimon_sync_database_affix;

CREATE TABLE t1 (
    k1 INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k1)
);

CREATE TABLE t2 (
    k2 INT,
    v2 VARCHAR(10),
    PRIMARY KEY (k2)
);

-- ################################################################################
--  PostgreSqlSyncDatabaseActionITCase#testIgnoreIncompatibleTables
-- ################################################################################

DROP SCHEMA IF EXISTS paimon_sync_database_ignore_incompatible CASCADE;
CREATE SCHEMA paimon_sync_database_ignore_incompatible;
SET search_path TO paimon_sync_database_ignore_incompatible;

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