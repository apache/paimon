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
--  OracleSyncTableActionITCase
-- ################################################################################

CREATE TABLE DEBEZIUM.composite1 (
    id NUMBER(9, 0) NOT NULL,
    name VARCHAR(255) NOT NULL,
    WEIGHT FLOAT,
    PRIMARY KEY(id)
);
CREATE TABLE DEBEZIUM.composite2 (
    id NUMBER(9, 0) NOT NULL,
    name VARCHAR(255) NOT NULL,
    WEIGHT FLOAT,
    PRIMARY KEY(id)
);

ALTER TABLE DEBEZIUM.composite1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE DEBEZIUM.composite2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- ----------------------------------------------------------------------------------------------------------------
-- TABLE:  FULL_TYPES
-- ----------------------------------------------------------------------------------------------------------------

-- Set session timezone to fixed Asia/Shanghai for checking TIMESTAMP_LTZ type
ALTER SESSION SET TIME_ZONE='Asia/Shanghai';

create table DEBEZIUM.FULL_TYPES (
    ID                           NUMBER(9) not null,
    VAL_VARCHAR                  VARCHAR2(1000),
    VAL_VARCHAR2                 VARCHAR2(1000),
    VAL_NVARCHAR2                NVARCHAR2(1000),
    VAL_CHAR                     CHAR(3),
    VAL_NCHAR                    NCHAR(3),
    VAL_BF                       BINARY_FLOAT,
    VAL_BD                       BINARY_DOUBLE,
    VAL_F                        FLOAT,
    VAL_F_10                     FLOAT(10),
    VAL_NUM                      NUMBER(10, 6),
    VAL_DP                       FLOAT,
    VAL_R                        FLOAT(63),
    VAL_DECIMAL                  NUMBER(10, 6),
    VAL_NUMERIC                  NUMBER(10, 6),
    VAL_NUM_VS                   NUMBER,
    VAL_INT                      NUMBER,
    VAL_INTEGER                  NUMBER,
    VAL_SMALLINT                 NUMBER,
    VAL_NUMBER_38_NO_SCALE       NUMBER(38),
    VAL_NUMBER_38_SCALE_0        NUMBER(38),
    VAL_NUMBER_1                 NUMBER(1),
    VAL_NUMBER_2                 NUMBER(2),
    VAL_NUMBER_4                 NUMBER(4),
    VAL_NUMBER_9                 NUMBER(9),
    VAL_NUMBER_18                NUMBER(18),
    VAL_DATE                     DATE,
    VAL_TS                       TIMESTAMP(6),
    VAL_TS_PRECISION2            TIMESTAMP(2),
    VAL_TS_PRECISION4            TIMESTAMP(4),
    VAL_TS_PRECISION9            TIMESTAMP(6),
    VAL_TSLTZ                    TIMESTAMP(6) WITH LOCAL TIME ZONE,
    T15VARCHAR                   SYS.XMLTYPE,
    primary key (ID)
);

ALTER TABLE DEBEZIUM.FULL_TYPES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEBEZIUM.FULL_TYPES VALUES (
    1, 'vc2',
    'vc2', 'nvc2', 'c', 'nc',
    1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,
    1, 22, 333, 4444, 5555, 1, 9, 999, 99999999, 99999999999999999,
    TO_DATE('2022-10-30', 'yyyy-mm-dd'),
    TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
    TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
    TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
    TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),
    TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
    sys.xmlType.createXML('<name><a id="1" value="some values">test xmlType</a></name>')
);