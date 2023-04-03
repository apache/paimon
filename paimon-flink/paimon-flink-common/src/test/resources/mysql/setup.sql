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
-- However, in this database we'll grant 2 users different privileges:
--
-- 1) 'paimonuser' - all privileges required by the snapshot reader AND binlog reader (used for testing)
-- 2) 'mysqluser' - all privileges
--
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES  ON *.* TO 'paimonuser'@'%';
CREATE USER 'mysqluser' IDENTIFIED BY 'mysqlpw';
GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%';

USE paimon_test;

CREATE TABLE schema_evolution_1 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);

CREATE TABLE schema_evolution_2 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);

CREATE TABLE all_types_table (
    _id INT,
    _boolean TINYINT(1),
    _tinyint TINYINT,
    _tinyint_unsigned TINYINT(2) UNSIGNED,
    _tinyint_unsigned_zerofill TINYINT(2) UNSIGNED ZEROFILL,
    _smallint SMALLINT,
    _smallint_unsigned SMALLINT UNSIGNED,
    _smallint_unsigned_zerofill SMALLINT(4) UNSIGNED ZEROFILL,
    _mediumint MEDIUMINT,
    _mediumint_unsigned MEDIUMINT UNSIGNED,
    _mediumint_unsigned_zerofill MEDIUMINT(8) UNSIGNED ZEROFILL,
    _int INT,
    _int_unsigned INT UNSIGNED,
    _int_unsigned_zerofill INT(8) UNSIGNED ZEROFILL,
    _bigint BIGINT,
    _bigint_unsigned BIGINT UNSIGNED,
    _bigint_unsigned_zerofill BIGINT(16) UNSIGNED ZEROFILL,
    _serial SERIAL,
    _float FLOAT,
    _float_unsigned FLOAT UNSIGNED,
    _float_unsigned_zerofill FLOAT(4) UNSIGNED ZEROFILL,
    _real REAL,
    _real_unsigned REAL UNSIGNED,
    _real_unsigned_zerofill REAL(10, 7) UNSIGNED ZEROFILL,
    _double DOUBLE,
    _double_unsigned DOUBLE UNSIGNED,
    _double_unsigned_zerofill DOUBLE(10, 7) UNSIGNED ZEROFILL,
    _double_precision DOUBLE PRECISION,
    _double_precision_unsigned DOUBLE PRECISION UNSIGNED,
    _double_precision_unsigned_zerofill DOUBLE PRECISION(10, 7) UNSIGNED ZEROFILL,
    _numeric NUMERIC(8, 3),
    _numeric_unsigned NUMERIC(8, 3) UNSIGNED,
    _numeric_unsigned_zerofill NUMERIC(8, 3) UNSIGNED ZEROFILL,
    _fixed FIXED(40, 3),
    _fixed_unsigned FIXED(40, 3) UNSIGNED,
    _fixed_unsigned_zerofill FIXED(40, 3) UNSIGNED ZEROFILL,
    _decimal DECIMAL(8),
    _decimal_unsigned DECIMAL(8) UNSIGNED,
    _decimal_unsigned_zerofill DECIMAL(8) UNSIGNED ZEROFILL,
    _date DATE,
    _datetime DATETIME,
    _timestamp TIMESTAMP(6) DEFAULT NULL,
    _char CHAR(10),
    _varchar VARCHAR(20),
    _text TEXT,
    _bin BINARY(10),
    _varbin VARBINARY(20),
    _blob BLOB,
    PRIMARY KEY (_id)
);

INSERT INTO all_types_table VALUES (
    1,
    true, 1, 2, 3,
    1000, 2000, 3000,
    100000, 200000, 300000,
    1000000, 2000000, 3000000,
    10000000000, 20000000000, 30000000000, 40000000000,
    1.5, 2.5, 3.5,
    1.000001, 2.000002, 3.000003,
    1.000011, 2.000022, 3.000033,
    1.000111, 2.000222, 3.000333,
    12345.11, 12345.22, 12345.33,
    123456789876543212345678987654321.11, 123456789876543212345678987654321.22, 123456789876543212345678987654321.33,
    11111, 22222, 33333,
    '2023-03-23', '2023-03-23 14:30:05', '2023-03-23 15:00:10.123456',
    'Paimon', 'Apache Paimon', 'Apache Paimon MySQL Test Data',
    'bytes', 'more bytes', 'very long bytes test data'
), (
    2,
    NULL, NULL, NULL, NULL,
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
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL
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
