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
--  PostgresSyncTableActionITCase
-- ################################################################################

CREATE DATABASE paimon_sync_table;

\c paimon_sync_table;

CREATE TABLE schema_evolution_1 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);

ALTER TABLE schema_evolution_1
    REPLICA IDENTITY FULL;

CREATE TABLE schema_evolution_2 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);

ALTER TABLE schema_evolution_2
    REPLICA IDENTITY FULL;

CREATE TABLE schema_evolution_multiple (
    _id INT,
    v1 VARCHAR(10),
    v2 INT,
    v3 VARCHAR(10),
    PRIMARY KEY (_id)
);

ALTER TABLE schema_evolution_multiple
    REPLICA IDENTITY FULL;

CREATE TABLE all_types_table (
    _id INT,
    pt DECIMAL(2, 1),
    -- BIT
    _bit1 BIT,
    _bit BIT(11),
    _bit_varying BIT VARYING,
    _bit_varying1 BIT VARYING(64),
    -- BOOLEAN
    _boolean BOOLEAN,
    _bool BOOL,
    -- SMALLINT
    _smallint SMALLINT,
    -- INT
     _int INT,
    -- BIGINT
    _bigint BIGINT,
    _small_serial SMALLSERIAL,
    _serial SERIAL,
    _big_serial BIGSERIAL,
    -- FLOAT
    _float FLOAT,
    -- REAL
    _real REAL,
    -- DOUBLE PRECISION
    _double_precision DOUBLE PRECISION,
    -- NUMERIC
    _numeric NUMERIC(8, 3),
    -- DECIMAL
    _decimal DECIMAL(8),
    _big_decimal DECIMAL(38,10),
    -- DATE
    _date DATE,
    -- TIMESTAMP
    _timestamp TIMESTAMP(6) DEFAULT NULL,
    _timestamp0 TIMESTAMP,
    -- TIME
    _time TIME(6) DEFAULT NULL,
    _time0 TIME,
    -- string
    _char CHAR(10),
    _varchar VARCHAR(20),
    _text TEXT,
    -- BYTEA
    _bin BYTEA,
    -- json
    _json JSON,
    _array VARCHAR[],
    PRIMARY KEY (_id)
);

ALTER TABLE all_types_table
    REPLICA IDENTITY FULL;

INSERT INTO all_types_table (
    _id, pt,
    _bit1, _bit, _bit_varying, _bit_varying1,
    _boolean, _bool,
    _smallint,
    _int,
    _bigint, _small_serial, _serial, _big_serial,
    _float,
    _real,
    _double_precision,
    _numeric,
    _decimal, _big_decimal,
    _date,
    _timestamp, _timestamp0,
    _time, _time0,
    _char, _varchar, _text,
    _bin,
    _json,
    _array
) VALUES (
    1, 1.1,
    '1', B'11111000111', B'101', B'010',
    true, true,
    1000,
    1000000,
    10000000000, 1, 2, 3,
    1.5,
    1.000001,
    1.000111,
    12345.11,
    11111, 2222222222222222300000001111.1234567890,
    '2023-03-23'::DATE,
    '2023-03-23 14:30:05'::TIMESTAMP, '2023-03-23'::TIMESTAMP,
    '10:13:23'::TIME, '10:13:23'::TIME,
    'Paimon', 'Apache Paimon', 'Apache Paimon PostgreSQL Test Data',
    'bytes',
    '{"a": "b"}'::JSON,
    ARRAY['item1', 'item2']::VARCHAR[]
    ), (
    2, 2.2,
    NULL, NULL, NULL, NULL,
    NULL, NULL,
    NULL,
    NULL,
    NULL, 4, 5, 6,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL, NULL,
    NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL,
    NULL,
    NULL,
    NULL
    );

CREATE TABLE incompatible_field_1 (
    _id INT,
    v1 TIMESTAMP,
    PRIMARY KEY (_id)
);

ALTER TABLE incompatible_field_1
    REPLICA IDENTITY FULL;

CREATE TABLE incompatible_field_2 (
    _id INT,
    v1 INT,
    PRIMARY KEY (_id)
);

ALTER TABLE incompatible_field_2
    REPLICA IDENTITY FULL;

CREATE TABLE incompatible_pk_1 (
    a INT,
    b BIGINT,
    c VARCHAR(20),
    PRIMARY KEY (a, b)
);

ALTER TABLE incompatible_pk_1
    REPLICA IDENTITY FULL;

CREATE TABLE incompatible_pk_2 (
    a INT,
    b BIGINT,
    c VARCHAR(20),
    PRIMARY KEY (a)
);

ALTER TABLE incompatible_pk_2
    REPLICA IDENTITY FULL;

CREATE TABLE test_computed_column (
    pk INT,
    _date DATE,
    _timestamp TIMESTAMP,
    PRIMARY KEY (pk)
);

ALTER TABLE test_computed_column
    REPLICA IDENTITY FULL;

CREATE TABLE test_options_change (
   pk INT,
   _date DATE,
   _timestamp TIMESTAMP,
   PRIMARY KEY (pk)
);

ALTER TABLE test_options_change
    REPLICA IDENTITY FULL;

CREATE TABLE test_exist_column_alter (
    pk INT,
    a BIGINT,
    b VARCHAR(30),
    c INT,
    PRIMARY KEY (pk)
);


-- ################################################################################
--  testMetadataColumns
-- ################################################################################

CREATE TABLE test_metadata_columns (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

ALTER TABLE test_metadata_columns
    REPLICA IDENTITY FULL;

-- ################################################################################
--  testSyncShard
-- ################################################################################

DROP SCHEMA IF EXISTS shard_1 CASCADE;
CREATE SCHEMA shard_1;
SET search_path TO shard_1;

CREATE TABLE t1 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS shard_2 CASCADE;
CREATE SCHEMA shard_2;
SET search_path TO shard_2;

CREATE TABLE t1 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

ALTER TABLE t2
    REPLICA IDENTITY FULL;
