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

-- Mimics the update of uv and pv of items in an E-commercial website.
-- Primary keys ranges from 0 to 10^8; Each record is about 150 bytes.

CREATE TABLE item_uv_pv_1d_source (
    `item_id` BIGINT,
    `item_name` STRING,
    `item_click_uv_1d` BIGINT,
    `item_click_pv_1d` BIGINT,
    `item_like_uv_1d` BIGINT,
    `item_like_pv_1d` BIGINT,
    `item_cart_uv_1d` BIGINT,
    `item_cart_pv_1d` BIGINT,
    `item_share_uv_1d` BIGINT,
    `item_share_pv_1d` BIGINT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '999999999',
    'fields.item_id.min' = '0',
    'fields.item_id.max' = '99999999',
    'fields.item_click_uv_1d.min' = '0',
    'fields.item_click_uv_1d.max' = '999999999',
    'fields.item_click_pv_1d.min' = '0',
    'fields.item_click_pv_1d.max' = '999999999',
    'fields.item_like_uv_1d.min' = '0',
    'fields.item_like_uv_1d.max' = '999999999',
    'fields.item_like_pv_1d.min' = '0',
    'fields.item_like_pv_1d.max' = '999999999',
    'fields.item_cart_uv_1d.min' = '0',
    'fields.item_cart_uv_1d.max' = '999999999',
    'fields.item_cart_pv_1d.min' = '0',
    'fields.item_cart_pv_1d.max' = '999999999',
    'fields.item_share_uv_1d.min' = '0',
    'fields.item_share_uv_1d.max' = '999999999',
    'fields.item_share_pv_1d.min' = '0',
    'fields.item_share_pv_1d.max' = '999999999'
);

CREATE VIEW item_uv_pv_1d AS
SELECT
    `item_id`,
    SUBSTR(`item_name`, 0, MOD(`item_id`, 32) + 64) AS `item_name`,
    `item_click_uv_1d`,
    `item_click_pv_1d`,
    `item_like_uv_1d`,
    `item_like_pv_1d`,
    `item_cart_uv_1d`,
    `item_cart_pv_1d`,
    `item_share_uv_1d`,
    `item_share_pv_1d`,
    NOW() AS `ts`
FROM item_uv_pv_1d_source;

-- __SINK_DDL_BEGIN__

CREATE TABLE IF NOT EXISTS ${SINK_NAME} (
    `item_id` BIGINT,
    `item_name` STRING,
    `item_click_uv_1d` BIGINT,
    `item_click_pv_1d` BIGINT,
    `item_like_uv_1d` BIGINT,
    `item_like_pv_1d` BIGINT,
    `item_cart_uv_1d` BIGINT,
    `item_cart_pv_1d` BIGINT,
    `item_share_uv_1d` BIGINT,
    `item_share_pv_1d` BIGINT,
    `ts` TIMESTAMP(3),
    PRIMARY KEY (`item_id`) NOT ENFORCED
) WITH (
    ${DDL_TEMPLATE}
);

-- __SINK_DDL_END__

INSERT INTO ${SINK_NAME} SELECT * FROM item_uv_pv_1d;
