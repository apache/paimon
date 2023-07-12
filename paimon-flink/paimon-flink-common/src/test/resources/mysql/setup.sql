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
--  MySqlSyncTableActionITCase
-- ################################################################################

CREATE DATABASE paimon_sync_table;
USE paimon_sync_table;

CREATE TABLE schema_evolution_1 (
    pt INT comment  'primary',
    _id INT comment  '_id',
    v1 VARCHAR(10) comment  'v1',
    PRIMARY KEY (_id)
);

CREATE TABLE schema_evolution_2 (
    pt INT comment 'primary',
    _id INT comment  '_id',
    v1 VARCHAR(10) comment  'v1',
    PRIMARY KEY (_id)
);

CREATE TABLE schema_evolution_multiple (
    _id INT comment 'primary',
    v1 VARCHAR(10) comment 'v1',
    v2 INT comment 'v2',
    v3 VARCHAR(10) comment 'v3',
    PRIMARY KEY (_id)
);

CREATE TABLE all_types_table (
    _id INT,
    pt DECIMAL(2, 1),
    -- TINYINT
    _tinyint1 TINYINT(1),
    _boolean BOOLEAN,
    _bool BOOL,
    _tinyint TINYINT,
    _tinyint_unsigned TINYINT(2) UNSIGNED,
    _tinyint_unsigned_zerofill TINYINT(2) UNSIGNED ZEROFILL,
    -- SMALLINT
    _smallint SMALLINT,
    _smallint_unsigned SMALLINT UNSIGNED,
    _smallint_unsigned_zerofill SMALLINT(4) UNSIGNED ZEROFILL,
    -- MEDIUMINT
    _mediumint MEDIUMINT,
    _mediumint_unsigned MEDIUMINT UNSIGNED,
    _mediumint_unsigned_zerofill MEDIUMINT(8) UNSIGNED ZEROFILL,
    -- INT
     _int INT,
     _int_unsigned INT UNSIGNED,
     _int_unsigned_zerofill INT(8) UNSIGNED ZEROFILL,
    -- BIGINT
    _bigint BIGINT,
    _bigint_unsigned BIGINT UNSIGNED,
    _bigint_unsigned_zerofill BIGINT(16) UNSIGNED ZEROFILL,
    _serial SERIAL,
    -- FLOAT
    _float FLOAT,
    _float_unsigned FLOAT UNSIGNED,
    _float_unsigned_zerofill FLOAT(4) UNSIGNED ZEROFILL,
    -- REAL
    _real REAL,
    _real_unsigned REAL UNSIGNED,
    _real_unsigned_zerofill REAL(10, 7) UNSIGNED ZEROFILL,
    -- DOUBLE
    _double DOUBLE,
    _double_unsigned DOUBLE UNSIGNED,
    _double_unsigned_zerofill DOUBLE(10, 7) UNSIGNED ZEROFILL,
    -- DOUBLE PRECISION
    _double_precision DOUBLE PRECISION,
    _double_precision_unsigned DOUBLE PRECISION UNSIGNED,
    _double_precision_unsigned_zerofill DOUBLE PRECISION(10, 7) UNSIGNED ZEROFILL,
    -- NUMERIC
    _numeric NUMERIC(8, 3),
    _numeric_unsigned NUMERIC(8, 3) UNSIGNED,
    _numeric_unsigned_zerofill NUMERIC(8, 3) UNSIGNED ZEROFILL,
    -- FIXED
    _fixed FIXED(40, 3),
    _fixed_unsigned FIXED(40, 3) UNSIGNED,
    _fixed_unsigned_zerofill FIXED(40, 3) UNSIGNED ZEROFILL,
    -- DECIMAL
    _decimal DECIMAL(8),
    _decimal_unsigned DECIMAL(8) UNSIGNED,
    _decimal_unsigned_zerofill DECIMAL(8) UNSIGNED ZEROFILL,
    -- DATE
    _date DATE,
    -- DATETIME
    _datetime DATETIME,
    _datetime3 DATETIME(3),
    _datetime6 DATETIME(6),
    -- DATETIME precision test
    _datetime_p DATETIME,
    _datetime_p2 DATETIME(2),
    -- TIMESTAMP
    _timestamp TIMESTAMP(6) DEFAULT NULL,
    _timestamp0 TIMESTAMP,
    -- string
    _char CHAR(10),
    _varchar VARCHAR(20),
    _tinytext TINYTEXT,
    _text TEXT,
    _mediumtext MEDIUMTEXT,
    _longtext LONGTEXT,
    -- BINARY
    _bin BINARY(10),
    _varbin VARBINARY(20),
    _tinyblob TINYBLOB,
    _blob BLOB,
    _mediumblob MEDIUMBLOB,
    _longblob LONGBLOB,
    -- json
    _json JSON,
    -- enum
    _enum ENUM ('value1','value2','value3'),
    -- YEAR
    _year YEAR,
    _time TIME,
    _point POINT,
    _geometry GEOMETRY,
    _linestring LINESTRING,
    _polygon  POLYGON,
    _multipoint  MULTIPOINT,
    _multiline  MULTILINESTRING,
    _multipolygon  MULTIPOLYGON,
    _geometrycollection GEOMETRYCOLLECTION,
    _set SET('a', 'b', 'c', 'd'),
    PRIMARY KEY (_id)
);


INSERT INTO all_types_table VALUES (
    1, 1.1,
    -- TINYINT
    true, true, false, 1, 2, 3,
    -- SMALLINT
    1000, 2000, 3000,
    -- MEDIUMINT
    100000, 200000, 300000,
    -- INT
    1000000, 2000000, 3000000,
    -- BIGINT
    10000000000, 20000000000, 30000000000, 40000000000,
    -- FLOAT
    1.5, 2.5, 3.5,
    -- REAL
    1.000001, 2.000002, 3.000003,
    -- DOUBLE
    1.000011, 2.000022, 3.000033,
    -- DOUBLE PRECISION
    1.000111, 2.000222, 3.000333,
    -- NUMERIC
    12345.11, 12345.22, 12345.33,
    -- FIXED
    123456789876543212345678987654321.11, 123456789876543212345678987654321.22, 123456789876543212345678987654321.33,
    -- DECIMAL
    11111, 22222, 33333,
    -- DATE
    '2023-03-23',
    -- DATETIME
    '2023-03-23 14:30:05', '2023-03-23 14:30:05.123', '2023-03-23 14:30:05.123456',
    -- DATETIME precision test
    '2023-03-24 14:30', '2023-03-24 14:30:05.12',
    -- TIMESTAMP
    '2023-03-23 15:00:10.123456', '2023-03-23 00:10',
    -- string
    'Paimon', 'Apache Paimon','Apache Paimon MySQL TINYTEXT Test Data', 'Apache Paimon MySQL Test Data','Apache Paimon MySQL MEDIUMTEXT Test Data','Apache Paimon MySQL Long Test Data',
    -- BINARY
    'bytes', 'more bytes', 'TINYBLOB type test data', 'BLOB type test data' , 'MEDIUMBLOB type test data' , 'LONGBLOB  bytes test data',
    -- json
    '{"a":"b"}',
    -- enum
    'value1',
     -- YEAR
     2023,
     -- TIME,
     '10:13:23',
    ST_GeomFromText('POINT(1 1)'),
    ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
    ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)'),
    ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
    ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
    ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),
    ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'),
    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'),
    'a,b'
), (
    2, 2.2,
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, 50000000000, -- SERIAL is never NULL
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL, NULL,NULL, NULL,
    NULL, NULL, NULL, NULL,NULL, NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE incompatible_field_1 (
    _id INT,
    v1 DATETIME,
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
    _datetime DATETIME,
    _timestamp TIMESTAMP,
    PRIMARY KEY (pk)
);

CREATE TABLE test_tinyint1_convert (
    pk INT,
    _datetime DATETIME,
    _tinyint1 TINYINT(1),
    PRIMARY KEY (pk)
);

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

-- test tinyint(1) convert
CREATE DATABASE paimon_sync_database_tinyint;
USE paimon_sync_database_tinyint;

CREATE TABLE t4 (
    pk INT,
    _datetime DATETIME,
    _tinyint1 TINYINT(1),
    PRIMARY KEY (pk)
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
--  MySqlSyncDatabaseActionITCase#testIgnoreCase
-- ################################################################################

CREATE DATABASE paimon_ignore_CASE;
USE paimon_ignore_CASE;

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

CREATE DATABASE paimon_sync_table_tinyint;
USE paimon_sync_table_tinyint;

CREATE TABLE schema_evolution_3 (
    pt INT comment  'primary',
    _id INT comment  '_id',
    v1 VARCHAR(10) comment  'v1',
    PRIMARY KEY (_id)
);

CREATE DATABASE paimon_sync_database_tinyint;
USE paimon_sync_database_tinyint;

CREATE TABLE schema_evolution_4 (
    _id INT comment  '_id',
    v1 VARCHAR(10) comment  'v1',
    PRIMARY KEY (_id)
);

CREATE TABLE schema_evolution_5 (
    _id INT comment  '_id',
    v1 VARCHAR(10) comment  'v1',
    v2 TINYINT(1) comment 'tinyint(1)',
    PRIMARY KEY (_id)
);