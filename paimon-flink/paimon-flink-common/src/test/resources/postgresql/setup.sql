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

-- ################################################################################
--  PostgreSqlSyncDatabaseActionITCase
-- ################################################################################

-- ################################################################################
--  PostgreSqlSyncDatabaseActionITCase#testIncludingTables
-- ################################################################################
DROP SCHEMA IF EXISTS paimon_sync_database_including CASCADE;
CREATE SCHEMA paimon_sync_database_including;
SET search_path TO paimon_sync_database_including;

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
--  PostgreSqlSyncDatabaseActionITCase#testExcludingTables
-- ################################################################################
DROP SCHEMA IF EXISTS paimon_sync_database_excluding CASCADE;
CREATE SCHEMA paimon_sync_database_excluding;
SET search_path TO paimon_sync_database_excluding;

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
    v0 VARCHAR(10),
    PRIMARY KEY (k1)
);

CREATE TABLE t2 (
    k2 INT,
    v0 VARCHAR(10),
    PRIMARY KEY (k2)
);




