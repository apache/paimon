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
INSERT INTO t1 VALUES (1, 'shard_1.t1');

CREATE TABLE t11 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t11 VALUES (1, 'shard_1.t11');

CREATE TABLE t2 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t2 VALUES (1, 'shard_1.t2');

CREATE TABLE t3 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t3 VALUES (1, 'shard_1.t3');

CREATE TABLE ta (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
INSERT INTO ta VALUES (1, 'shard_1.ta');

CREATE TABLE taa (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO taa VALUES (1, 'shard_1.taa');

CREATE TABLE s1 (k INT, name VARCHAR(100)); -- ignored because of pk absence
INSERT INTO s1 VALUES (1, 'shard_1.s1');

CREATE TABLE s2 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO s2 VALUES (1, 'shard_1.s2');

CREATE TABLE sa (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
INSERT INTO sa VALUES (1, 'shard_1.sa');

CREATE TABLE m (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
INSERT INTO m VALUES (1, 'shard_1.m');

USE shard_2;

CREATE TABLE t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t1 VALUES (2, 'shard_2.t1');

CREATE TABLE t2 (k INT, name VARCHAR(100)); -- ignored because of pk absence
INSERT INTO t2 VALUES (2, 'shard_2.t2');

CREATE TABLE t22 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t22 VALUES (2, 'shard_2.t22');

CREATE TABLE t3 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t3 VALUES (2, 'shard_2.t3');

CREATE TABLE tb (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO tb VALUES (2, 'shard_2.tb');

USE x_shard_1;

CREATE TABLE t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- captured
INSERT INTO t1 VALUES (3, 'x_shard_1.t1');

USE ignored;

CREATE TABLE t1 (k INT, name VARCHAR(100), PRIMARY KEY (k)); -- ignored
INSERT INTO t1 VALUES (4, 'ignored.t1');
