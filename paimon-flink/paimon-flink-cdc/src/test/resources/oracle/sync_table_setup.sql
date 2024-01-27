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

CREATE TABLE DEBEZIUM.composite (
                                    id NUMBER(9, 0) NOT NULL,
                                    name VARCHAR(255) NOT NULL,
                                    PRIMARY KEY(id)
);

-- CREATE TABLE DEBEZIUM.cat (
--                                    id NUMBER(9, 0) NOT NULL,
--                                    CATEGORY_NAME VARCHAR(255),
--                                    PRIMARY KEY(id)
-- );


-- ALTER TABLE DEBEZIUM.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
-- ALTER TABLE DEBEZIUM.cat ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE DEBEZIUM.composite ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
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