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

-- Create and populate products and category tables using a single insert with many rows
-- CREATE TABLE DEBEZIUM.PRODUCTS (
--                                    id NUMBER(9, 0) NOT NULL,
--                                    NAME VARCHAR(255) NOT NULL,
--                                    DESCRIPTION VARCHAR(512),
--                                    WEIGHT FLOAT,
--                                    PRIMARY KEY(id)
-- );
-- CREATE TABLE DEBEZIUM.COMPOSITE (
--                                    id NUMBER(9, 0) NOT NULL,
--                                    NAME VARCHAR(255) NOT NULL,
--                                    DESCRIPTION VARCHAR(512),
--                                    WEIGHT FLOAT,
--                                    PRIMARY KEY(id)
-- );

-- sqlplus dbzuser/dbz@localhost:1521/ORCLCDB

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

-- CREATE TABLE DEBEZIUM.cat (
--                                    id NUMBER(9, 0) NOT NULL,
--                                    CATEGORY_NAME VARCHAR(255),
--                                    PRIMARY KEY(id)
-- );


-- ALTER TABLE DEBEZIUM.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
-- ALTER TABLE DEBEZIUM.cat ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE DEBEZIUM.composite1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE DEBEZIUM.composite2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
-- ALTER TABLE DEBEZIUM.COMPOSITE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- INSERT INTO DEBEZIUM.composite (id,name,weight)
-- VALUES (101,'scooter',3.14);
-- INSERT INTO DEBEZIUM.composite (id,name,weight)
-- VALUES (102,'test',6.28);
-- INSERT INTO DEBEZIUM.COMPOSITE (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (101,'scooter','Small 2-wheel scooter',3.14);

-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (101,'scooter','Small 2-wheel scooter',3.14);
-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (102,'car battery','12V car battery',8.1);
-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (103,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8);
-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (104,'hammer','12oz carpenters hammer',0.75);
-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (105,'hammer','14oz carpenters hammer',0.875);
-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (106,'hammer','16oz carpenters hammer',1.0);
-- INSERT INTO DEBEZIUM.PRODUCTS (id,NAME,DESCRIPTION,WEIGHT)
-- VALUES (107,'rocks','box of assorted rocks',5.3);

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
--                                      VAL_NUMBER_2_NEGATIVE_SCALE  NUMBER(1, -1),
--                                      VAL_NUMBER_4_NEGATIVE_SCALE  NUMBER(2, -2),
--                                      VAL_NUMBER_9_NEGATIVE_SCALE  NUMBER(8, -1),
--                                      VAL_NUMBER_18_NEGATIVE_SCALE NUMBER(16, -2),
--                                      VAL_NUMBER_36_NEGATIVE_SCALE NUMBER(36, -2),
                                     VAL_DATE                     DATE,
                                     VAL_TS                       TIMESTAMP(6),
                                     VAL_TS_PRECISION2            TIMESTAMP(2),
                                     VAL_TS_PRECISION4            TIMESTAMP(4),
                                     VAL_TS_PRECISION9            TIMESTAMP(6),
--                                      VAL_TSTZ                     TIMESTAMP(6) WITH TIME ZONE,
                                     VAL_TSLTZ                    TIMESTAMP(6) WITH LOCAL TIME ZONE,
--                                      VAL_INT_YTM                  INTERVAL YEAR(2) TO MONTH,
--                                      VAL_INT_DTS                  INTERVAL DAY(3) TO SECOND(2),
--                                      VAL_CLOB_INLINE              CLOB,
--                                      VAL_NCLOB_INLINE             NCLOB,
--                                      VAL_BLOB_INLINE              BLOB,
                                     T15VARCHAR                   SYS.XMLTYPE,
                                     primary key (ID)
);

ALTER TABLE DEBEZIUM.FULL_TYPES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEBEZIUM.FULL_TYPES VALUES (
                                           1, 'vc2',
                                        'vc2', 'nvc2', 'c', 'nc',
                                           1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,
                                           1, 22, 333, 4444, 5555, 1, 9, 999, 99999999, 99999999999999999,
--                                             94, 949, 99999994, 99999999999999949, 9999999999999999999999999999999999949,
                                           TO_DATE('2022-10-30', 'yyyy-mm-dd'),
                                           TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                                           TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                                           TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                                           TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),
--                                              TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789 -11:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM'),
                                           TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
--                                            INTERVAL '-3-6' YEAR TO MONTH,
--                                            INTERVAL '-1 2:3:4.56' DAY TO SECOND,
--                                            TO_CLOB ('col_clob'),
--                                            TO_NCLOB ('col_nclob'),
--                                            utl_raw.cast_to_raw ('col_blob'),
                                           sys.xmlType.createXML('<name><a id="1" value="some values">test xmlType</a></name>')
                                       );