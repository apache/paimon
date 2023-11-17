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
--
-- ################################################################################
--  SqlServerSyncDatabaseTableListITCase
-- ################################################################################

-- captured databases
CREATE DATABASE shard_database;

-- create tables

USE shard_database;
EXEC sys.sp_cdc_enable_db;

CREATE SCHEMA shard_1;

CREATE TABLE shard_1.t1 (k INT, name VARCHAR(100)); -- ignored because of pk absence
CREATE TABLE shard_1.t11 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_1.t2 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_1.t3 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_1.ta (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
CREATE TABLE shard_1.taa (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_1.s1 (k INT, name VARCHAR(100)); -- ignored because of pk absence
CREATE TABLE shard_1.s2 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_1.sa (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
CREATE TABLE shard_1.m (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 't11', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 't3', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 'ta', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 'taa', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 's1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 's2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 'sa', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 'm', @role_name = NULL, @supports_net_changes = 0;


CREATE SCHEMA shard_2;
CREATE TABLE shard_2.t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_2.t2 (k INT, name VARCHAR(100)); -- ignored because of pk absence
CREATE TABLE shard_2.t22 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_2.t3 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE shard_2.tb (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured

EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 't22', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 't3', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 'tb', @role_name = NULL, @supports_net_changes = 0;

CREATE SCHEMA x_shard_1;

CREATE TABLE x_shard_1.t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
EXEC sys.sp_cdc_enable_table @source_schema = 'x_shard_1', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;


CREATE SCHEMA ignored;

CREATE TABLE ignored.t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
EXEC sys.sp_cdc_enable_table @source_schema = 'ignored', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
