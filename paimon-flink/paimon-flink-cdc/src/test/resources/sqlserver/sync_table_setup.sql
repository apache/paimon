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
--  SqlServerSyncTableActionITCase
-- ################################################################################

CREATE DATABASE paimon_sync_table;
USE paimon_sync_table;
EXEC sys.sp_cdc_enable_db;

CREATE TABLE schema_evolution_1 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'schema_evolution_1', @role_name = NULL, @supports_net_changes = 0;


CREATE TABLE schema_evolution_2 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'schema_evolution_2', @role_name = NULL, @supports_net_changes = 0;


CREATE TABLE all_types_table (
    _id INT,
    pt DECIMAL(2, 1),
    -- BIT
    _bit1 BIT,
    -- TINYINT
    _tinyint TINYINT,
    -- SMALLINT
    _smallint SMALLINT,
    -- INT
     _int INT,
    -- BIGINT
    _bigint BIGINT,
    -- FLOAT
    _float FLOAT,
    -- REAL
    _real REAL,
    -- NUMERIC
    _numeric NUMERIC(8),
    _numeric8_3 NUMERIC(8, 3),
    -- DECIMAL
    _decimal DECIMAL(8),
    _big_decimal DECIMAL(38,10),
    -- SMALLMONEY
    _smallmoney SMALLMONEY,
    -- MONEY
    _money MONEY,
    _big_money MONEY,
    -- DATE
    _date DATE,
    -- TIME
    _time3 TIME(3),
    _time6 TIME(6),
    _time7 TIME(7),
    -- DATETIME
    _datetime DATETIME,
    -- SMALLDATETIME
    _smalldatetime SMALLDATETIME,
    -- DATETIME2
    _datetime2 DATETIME2,
    _datetime2_3 DATETIME2(3),
    _datetime2_6 DATETIME2(6),
    _datetime2_7 DATETIME2(7),
    -- string
    _char CHAR(10),
    _varchar VARCHAR(20),
    _text TEXT,
    _nchar NCHAR(10),
    _nvarchar NVARCHAR(20),
    _ntext NTEXT,
    _xml XML,
    _datetimeoffset  DATETIMEOFFSET(7)
    PRIMARY KEY (_id)
);


INSERT INTO all_types_table VALUES (
    1,
    1.1,
    -- BIT
    255,
    -- TINYINT
    1,
    -- SMALLINT
    1000,
    -- INT
    1000000,
    -- BIGINT
    10000000000,
    -- FLOAT
    3.14159,
    -- REAL
    3.14,
    -- NUMERIC
    12345678, 12345.678,
    -- DECIMAL
    12345678, 12345.6789123456,
    -- SMALLMONEY
    12345.6700,
    -- MONEY
    12345678.0000, 12345.6789,
    -- DATE
    '2023-03-23',
     -- TIME
    '10:30:15.123','10:30:15.123456','10:30:15.1234567',
    -- DATETIME
    '2023-09-30 10:30:15.123',
    -- SMALLDATETIME
    '2023-03-23 14:30:05',
    -- DATETIME2
    '2023-09-30 10:30:15.1234567', '2023-09-30 10:30:15.123','2023-09-30 10:30:15.123456', '2023-09-30 10:30:15.1234567',
    -- string
    'Paimon','Apache Paimon','Apache Paimon SQLServer TEXT Test Data','Paimon','Apache Paimon','Apache Paimon NTEXT Long Test Data',
    -- XML
    NULL,
    -- DATETIMEOFFSET
    '2023-02-01T10:00:00+05:00'
), (
    2,2.2,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'all_types_table', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE incompatible_field_1 (
    _id INT,
    v1 SMALLDATETIME,
    PRIMARY KEY (_id)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'incompatible_field_1', @role_name = NULL, @supports_net_changes = 0;


CREATE TABLE incompatible_field_2 (
    _id INT,
    v1 INT,
    PRIMARY KEY (_id)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'incompatible_field_2', @role_name = NULL, @supports_net_changes = 0;


CREATE TABLE incompatible_pk_1 (
    a INT,
    b BIGINT,
    c VARCHAR(20),
    PRIMARY KEY (a, b)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'incompatible_pk_1', @role_name = NULL, @supports_net_changes = 0;


CREATE TABLE incompatible_pk_2 (
    a INT,
    b BIGINT,
    c VARCHAR(20),
    PRIMARY KEY (a)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'incompatible_pk_2', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE test_computed_column (
    pk INT,
    _date DATE,
    _datetime DATETIME,
    PRIMARY KEY (pk)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'test_computed_column', @role_name = NULL, @supports_net_changes = 0;

CREATE TABLE test_options_change (
   pk INT,
   _date DATE,
   _datetime DATETIME,
   PRIMARY KEY (pk)
);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'test_options_change', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  testSyncShardsMultipleSchemas
-- ################################################################################

CREATE DATABASE shard_schema;
USE shard_schema;
EXEC sys.sp_cdc_enable_db;

CREATE SCHEMA shard_1;
CREATE TABLE shard_1.t1 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

CREATE TABLE shard_1.t2 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

CREATE SCHEMA shard_2;
CREATE TABLE shard_2.t1 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

CREATE TABLE shard_2.t2 (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_1', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 't1', @role_name = NULL, @supports_net_changes = 0;
EXEC sys.sp_cdc_enable_table @source_schema = 'shard_2', @source_name = 't2', @role_name = NULL, @supports_net_changes = 0;

-- ################################################################################
--  testMetadataColumns
-- ################################################################################

CREATE DATABASE table_metadata;
USE table_metadata;
EXEC sys.sp_cdc_enable_db;

CREATE TABLE test_metadata_columns (
    pk INT,
    _date VARCHAR(10),
    PRIMARY KEY (pk)
);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'test_metadata_columns', @role_name = NULL, @supports_net_changes = 0;