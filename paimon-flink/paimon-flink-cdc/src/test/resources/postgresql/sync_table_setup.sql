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
--  PostgreSqlSyncTableActionITCase
-- ################################################################################

DROP SCHEMA IF EXISTS sync_table_schema CASCADE;
CREATE SCHEMA sync_table_schema;
SET search_path TO sync_table_schema;

CREATE TABLE t1 (
    id INT,
    uid INT,
    name VARCHAR(100),
    pt VARCHAR(100),
    PRIMARY KEY (id)
);

CREATE TABLE t2 (
    id INT,
    uid INT,
    address VARCHAR(100),
    pt VARCHAR(100),
    PRIMARY KEY (id)
);

CREATE TABLE incompatible_field_1 (
    _id INT,
    v1 TIMESTAMP,
    PRIMARY KEY (_id)
);

CREATE TABLE incompatible_field_2 (
    _id INT,
    v1 INT,
    PRIMARY KEY (_id)
);

CREATE TABLE incompatible_pk_1 (
    a INT,
    b BIGINT,
    c VARCHAR(20),
    PRIMARY KEY (a, b)
);

CREATE TABLE incompatible_pk_2 (
    a INT,
    b BIGINT,
    c VARCHAR(20),
    PRIMARY KEY (a)
);

CREATE TABLE test_computed_column (
    pk INT,
    _date DATE,
    _datetime TIMESTAMP,
    _timestamp TIMESTAMPTZ,
    PRIMARY KEY (pk)
);

CREATE TABLE test_options_change (
    pk INT,
    _date DATE,
    _datetime TIMESTAMP,
    _timestamp TIMESTAMPTZ,
    PRIMARY KEY (pk)
);