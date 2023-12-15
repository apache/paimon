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
--  MySqlSyncDatabaseTableListITCase
-- ################################################################################

-- captured databases
CREATE DATABASE shard_1;
CREATE DATABASE shard_2;
CREATE DATABASE x_shard_1;

-- ignored databases
CREATE DATABASE ignored;

-- create tables

USE shard_1;

CREATE TABLE t1 (k INT, name VARCHAR(100)); -- ignored because of pk absence
CREATE TABLE t11 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE t2 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE t3 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
CREATE TABLE taa (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE s1 (k INT, name VARCHAR(100)); -- ignored because of pk absence
CREATE TABLE s2 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE sa (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
CREATE TABLE m (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored

INSERT INTO t2 VALUES (1, 'A');

USE shard_2;

CREATE TABLE t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE t2 (k INT, name VARCHAR(100)); -- ignored because of pk absence
CREATE TABLE t22 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE t3 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
CREATE TABLE tb (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured

USE x_shard_1;

CREATE TABLE t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured

USE ignored;

CREATE TABLE t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
